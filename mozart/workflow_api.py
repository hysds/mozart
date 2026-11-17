"""Shared implementation of the mozart ``/workflow`` REST namespace (PLAN-workflow-cwl-profile.md section 10).

``api_v01/workflow.py`` and ``api_v02/workflow.py`` are thin wrappers around
:func:`make_workflow_ns`, so the two API versions cannot drift.

Workflows are authored in CWL (schema 2.0, see ``hysds_commons.cwl``):

* ``POST /workflow/cwl/validate``  dry-run compile, never stores anything
* ``POST /workflow/cwl``           compile against the registry and register the spec
* ``GET  /workflow/cwl`` and ``/workflow/cwl/<id:version>``  list / fetch registered workflows
* ``GET  /workflow/cwl/operation/<job_type>``  Operation snippet for authoring
* ``POST /workflow/runs``          start a run (the engine runs it)
* ``GET  /workflow/runs[/<id>]``   run status (Redis while active, OpenSearch afterwards)
* ``POST /workflow/runs/<id>/cancel``

While a run is active Redis is the source of truth (section 6.10). mozart reads OpenSearch only
to register a spec and, once, at run start to freeze the job definitions; it then hands the run to
the engine through ``hysds.workflow_state`` and does not submit any job itself.

All expected-404/409 paths use the raw opensearch client with ``ignore=``: the
``OpenSearchUtility`` wrapper retries every error, 404/409 included, for ~34s.
"""
import importlib.util
import json
import uuid
from datetime import datetime, timezone

from flask import Response, request
from flask_restx import Namespace, Resource

from hysds_commons.workflow_refs2 import resolve_run_inputs
from hysds_commons.workflow_run2 import (
    TERMINAL_RUN_STATUSES,
    freeze_job_definitions,
    new_run_doc,
    validate_run_doc,
)
from hysds_commons.workflow_spec2 import spec_doc_id, step_queue

from mozart import app, mozart_es

DEFAULT_USER = "anonymous"
SUMMARY_EXCLUDES = ["steps", "job_definitions", "inputs", "outputs"]
MAX_LIST_SIZE = 1000

_redis_client = None


class MissingJobType(Exception):
    """A job type used by the workflow has no job_spec / hysds_io / container in the registry."""


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _fail(message, code, **extra):
    return {"success": False, "message": message, **extra}, code


def _es():
    return getattr(mozart_es, "es", mozart_es)


def _specs_index():
    return app.config["WORKFLOW_SPECS_INDEX"]


def _status_alias():
    """The ``workflow_status`` read alias over the daily ``workflow_status-YYYY.MM.DD`` indices."""
    return app.config["WORKFLOW_STATUS_INDEX"]


def _is_error(resp, code):
    return isinstance(resp, dict) and "error" in resp and resp.get("status") == code


def _found(resp):
    """The ``_source`` of an ``es.get(..., ignore=404)`` response, or None when absent."""
    if resp is None or _is_error(resp, 404) or not resp.get("found", True):
        return None
    return resp.get("_source")


# ---------------------------------------------------------------- lazy S4 / CWL / redis access

CWL_EXTRA_MESSAGE = (
    "CWL workflow authoring (validate, register, operation snippets) needs the optional 'workflows' "
    "extra of mozart (hysds_commons[cwl]: cwltool and cwl-utils), which is not installed. "
    "Install it with: pip install 'mozart[workflows]' (or 'hysds_commons[cwl]'). "
    "Already-registered workflows can still be listed and run."
)


def cwl_available():
    """True when the ``cwl`` extra (cwltool + cwl-utils) is importable; does not import it (cwltool is slow)."""
    for name in ("cwltool", "cwl_utils"):
        try:
            if importlib.util.find_spec(name) is None:
                return False
        except (ImportError, ValueError):
            return False
    return True


def _cwl():
    """``hysds_commons.cwl`` (needs the ``cwl`` extra); ImportError carries the install hint."""
    import hysds_commons.cwl as cwl  # noqa: PLC0415

    return cwl


def _state():
    """``hysds.workflow_state`` (the engine's Redis state API), imported lazily so mozart starts without it."""
    import hysds.workflow_state as state  # noqa: PLC0415

    return state


def _redis():
    """The status Redis, built the way the workflow consumer builds it (``REDIS_JOB_STATUS_URL``)."""
    global _redis_client
    if _redis_client is None:
        from redis import StrictRedis  # noqa: PLC0415

        from hysds.celery import app as hysds_app  # noqa: PLC0415

        _redis_client = StrictRedis.from_url(hysds_app.conf.REDIS_JOB_STATUS_URL, ssl_cert_reqs="none")
    return _redis_client


# ---------------------------------------------------------------- registry reads (OpenSearch)

def _hysds_io_id(job_type):
    return job_type.replace("job", "hysds-io", 1)


def _fetch_job_spec(job_type):
    return _found(_es().get(index=app.config["JOB_SPECS_INDEX"], id=job_type, ignore=404))


def _fetch_hysds_io(job_type):
    io_id = _hysds_io_id(job_type)
    for index in (app.config["HYSDS_IOS_INDEX"], "hysds_ios-grq"):
        src = _found(_es().get(index=index, id=io_id, ignore=404))
        if src is not None:
            return src
    return None


def job_type_lookup(job_type):
    """Registry lookup for ``compile_cwl``: ``{"params": [...], "outputs": [...]|None}`` or None when unregistered."""
    job_spec = _fetch_job_spec(job_type)
    hysds_io = _fetch_hysds_io(job_type) if job_spec is not None else None
    if job_spec is None or hysds_io is None:
        return None
    outputs = hysds_io.get("outputs")
    if outputs is None:
        outputs = job_spec.get("outputs")
    return {"params": hysds_io.get("params") or [], "outputs": outputs}


def fetch_job_definition(job_type):
    """Freeze source (B26): job_spec, hysds_io, container and first recommended queue of one job type."""
    job_spec = _fetch_job_spec(job_type)
    if job_spec is None:
        raise MissingJobType(f"job type '{job_type}': job_spec is not registered")
    hysds_io = _fetch_hysds_io(job_type)
    if hysds_io is None:
        raise MissingJobType(f"job type '{job_type}': hysds-io '{_hysds_io_id(job_type)}' is not registered")
    container_id = job_spec.get("container")
    container = _found(_es().get(index=app.config["CONTAINERS_INDEX"], id=container_id, ignore=404)) \
        if container_id else None
    if container is None:
        raise MissingJobType(f"job type '{job_type}': container '{container_id}' is not registered")
    queues = job_spec.get("recommended-queues") or []
    return {"job_spec": job_spec, "hysds_io": hysds_io, "container": container,
            "queue": queues[0] if queues else None}


# ---------------------------------------------------------------- request parsing

def _json_body():
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else None


def _cwl_source():
    """CWL text from the request: a raw YAML/JSON body, or a JSON object ``{"cwl": "<text>"}``.
    Returns ``(text, user)``; text is None when the body is empty or not decodable."""
    user = request.args.get("user") or DEFAULT_USER
    if request.is_json:
        body = request.get_json(silent=True)
        if isinstance(body, dict) and isinstance(body.get("cwl"), (str, dict)):
            cwl = body["cwl"]
            user = body.get("user") or user
            return (json.dumps(cwl) if isinstance(cwl, dict) else cwl), user
        if isinstance(body, dict):  # the CWL document itself, as JSON
            return json.dumps(body), user
        return None, user
    raw = request.get_data()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, user
    return (text if text.strip() else None), user


def _no_cwl_extra(e):
    """501: this mozart was installed without the ``workflows`` extra (the endpoint exists but is not
    implemented here); the message names the extra and how to install it."""
    app.logger.warning("workflow CWL endpoint called without the cwl extra: %s", e)
    return _fail(CWL_EXTRA_MESSAGE, 501, error=str(e))


# ---------------------------------------------------------------- CWL endpoints

def validate_cwl():
    text, _ = _cwl_source()
    if text is None:
        return _fail("request body must be CWL text (YAML or JSON), or a JSON object with a 'cwl' field", 400)
    try:
        res = _cwl().compile_cwl(text, job_type_lookup)
    except ImportError as e:
        return _no_cwl_extra(e)
    except Exception as e:
        app.logger.error("workflow cwl validate failed: %s", e)
        return _fail(f"Failed to validate CWL. {type(e)}:{e}", 500)
    return {"success": True, "message": "", **res.as_dict(), "sha256": res.sha256}, 200


def register_cwl():
    text, user = _cwl_source()
    if text is None:
        return _fail("request body must be CWL text (YAML or JSON), or a JSON object with a 'cwl' field", 400)
    try:
        res = _cwl().compile_cwl(text, job_type_lookup)
    except ImportError as e:
        return _no_cwl_extra(e)
    except Exception as e:
        app.logger.error("workflow cwl compile failed: %s", e)
        return _fail(f"Failed to compile CWL. {type(e)}:{e}", 500)
    if not res.ok:
        return {"success": False, "message": "invalid workflow", "errors": res.errors,
                "warnings": res.warnings}, 400
    spec = res.spec
    spec_id = spec_doc_id(spec)
    now = _now()
    doc = {
        "id": spec["id"], "version": str(spec["version"]), "schema_version": spec.get("schema_version"),
        "label": spec.get("label") or spec["id"], "username": user,
        "creation_time": now, "modification_time": now,
        "sha256": res.sha256, "cwl": text, "cwl_normalized": res.cwl, "spec": spec,
    }
    try:
        r = _es().index(index=_specs_index(), id=spec_id, body=doc, op_type="create", ignore=409)
        if _is_error(r, 409):
            existing = _found(_es().get(index=_specs_index(), id=spec_id, ignore=404)) or {}
            if existing.get("sha256") == res.sha256:
                return {"success": True, "message": "already registered", "spec_id": spec_id,
                        "sha256": res.sha256, "warnings": res.warnings, "result": spec_id}, 200
            return _fail(f"workflow {spec_id} is already registered with different content "
                         f"(sha256 {existing.get('sha256')}); bump the version", 409,
                         spec_id=spec_id, sha256=existing.get("sha256"))
    except Exception as e:
        app.logger.error("workflow spec create failed: %s", e)
        return _fail(f"Failed to store workflow spec. {type(e)}:{e}", 500)
    return {"success": True, "message": "", "spec_id": spec_id, "sha256": res.sha256,
            "warnings": res.warnings, "result": spec_id}, 201


def get_operation(job_type):
    try:
        job_spec = _fetch_job_spec(job_type)
        hysds_io = _fetch_hysds_io(job_type) if job_spec is not None else None
    except Exception as e:
        return _fail(f"Failed to look up job type. {type(e)}:{e}", 500)
    if job_spec is None or hysds_io is None:
        return _fail(f"job type not registered (job_spec and hysds-io are both required): {job_type}", 404)
    try:
        cwl = _cwl()
        snippet = cwl.operation_snippet(job_type, hysds_io, job_spec)
        operation = cwl.generate_operation(job_type, hysds_io, job_spec)
    except ImportError as e:
        return _no_cwl_extra(e)
    if request.args.get("format") == "json":
        return {"success": True, "message": "", "job_type": job_type, "operation": operation,
                "snippet": snippet}, 200
    return Response(snippet, mimetype="text/yaml")


def _load_spec(spec_id):
    """``(compiled_spec, sha256)`` of a registered workflow, or ``(None, None)``."""
    src = _found(_es().get(index=_specs_index(), id=spec_id, ignore=404))
    if src is None:
        return None, None
    return src.get("spec"), src.get("sha256")


def get_spec(spec_id):
    try:
        src = _found(_es().get(index=_specs_index(), id=spec_id, ignore=404))
    except Exception as e:
        return _fail(f"Failed to get workflow spec. {type(e)}:{e}", 500)
    if src is None:
        return _fail(f"workflow spec not found: {spec_id}", 404)
    return {"success": True, "message": "", "result": src}, 200


def list_specs():
    try:
        r = _es().search(
            index=_specs_index(),
            body={"query": {"match_all": {}},
                  "sort": [{"creation_time": {"order": "desc", "unmapped_type": "date"}}],
                  "_source": ["id", "version", "label", "username", "creation_time", "sha256"]},
            size=MAX_LIST_SIZE)
    except Exception as e:
        return _fail(f"Failed to list workflow specs. {type(e)}:{e}", 500)
    result = [{**h["_source"], "spec_id": h["_id"]} for h in r["hits"]["hits"]]
    return {"success": True, "message": "", "count": len(result), "result": result}, 200


# ---------------------------------------------------------------- runs

def create_run():
    body = _json_body()
    if body is None:
        return _fail("request body must be a JSON object", 400)
    spec_id = body.get("spec_id")
    if not isinstance(spec_id, str) or not spec_id:
        return _fail("spec_id is required", 400)
    tags = body.get("tags") or []
    if not isinstance(tags, list) or not all(isinstance(t, str) and t for t in tags):
        return _fail("tags must be a list of non-empty strings", 400)
    user = body.get("user") or DEFAULT_USER

    try:
        spec, sha256 = _load_spec(spec_id)
    except Exception as e:
        return _fail(f"Failed to load workflow spec. {type(e)}:{e}", 500)
    if spec is None:
        return _fail(f"workflow spec not found: {spec_id}", 404)

    inputs, errors = resolve_run_inputs(spec, body.get("inputs"))
    if errors:
        return {"success": False, "message": "invalid run inputs", "errors": errors}, 400

    # The last OpenSearch read for this run (B26): freeze the job definitions.
    try:
        defs = freeze_job_definitions(spec, fetch_job_definition)
    except MissingJobType as e:
        return {"success": False, "message": str(e), "errors": [str(e)]}, 400
    except Exception as e:
        app.logger.error("workflow job definition freeze failed: %s", e)
        return _fail(f"Failed to look up job definitions. {type(e)}:{e}", 500)
    no_queue = [sid for sid, st in spec["steps"].items()
                if "job_type" in st and not step_queue(spec, sid, defs[st["job_type"]].get("queue"))]
    if no_queue:
        return {"success": False, "message": "no queue for step(s): " + ", ".join(sorted(no_queue)),
                "errors": [f"step '{sid}': set its queue or give job type '{spec['steps'][sid]['job_type']}' "
                           f"a recommended queue" for sid in sorted(no_queue)]}, 400

    run_id = "run-" + uuid.uuid4().hex
    run = new_run_doc(spec, run_id, inputs, defs, spec_sha256=sha256)
    run["username"] = user
    run["tags"] = tags
    run["index"] = "workflow_status-" + run["created_at"][:10].replace("-", ".")
    problems = validate_run_doc(run)
    if problems:
        app.logger.error("run doc invalid: %s", problems)
        return _fail("internal error: run document is invalid", 500, errors=problems)

    try:
        state = _state()
    except ImportError as e:
        return _fail(f"workflow engine state is not available on this mozart ({e})", 503)
    try:
        state.init_run(_redis(), spec, run)
        state.publish_run_event(run_id, "run_started")
    except Exception as e:
        msg = f"Failed to start workflow run. {type(e)}:{e}"
        app.logger.error(msg)
        return _fail(msg, 500, run_id=run_id)
    return {"success": True, "message": "", "run_id": run_id, "result": run_id}, 202


def _read_run(run_id):
    """``(doc, source)``: the run from Redis while active, else from OpenSearch; ``(None, None)`` if unknown.
    A missing S4 module or a Redis error degrades to the OpenSearch record."""
    try:
        doc = _state().get_run(_redis(), run_id)
        if doc is not None:
            return doc, "redis"
    except ImportError:
        pass
    except Exception as e:  # Redis down: the (possibly lagging) OpenSearch record still answers
        app.logger.warning("redis read of run %s failed: %s", run_id, e)
    r = _es().search(index=_status_alias(), body={
        "query": {"term": {"run_id": run_id}}, "size": 1,
        "sort": [{"version": {"order": "desc", "unmapped_type": "long"}}]}, ignore=404)
    hits = ((r or {}).get("hits") or {}).get("hits") or []
    return (hits[0]["_source"], "opensearch") if hits else (None, None)


def get_run(run_id):
    try:
        doc, source = _read_run(run_id)
    except Exception as e:
        return _fail(f"Failed to get workflow run. {type(e)}:{e}", 500)
    if doc is None:
        return _fail(f"workflow run not found: {run_id}", 404)
    return {"success": True, "message": "", "source": source, "result": doc}, 200


def list_runs():
    a = request.args
    filters = [{"term": {f: a[f]}} for f in ("status", "spec_id", "username", "spec_version") if a.get(f)]
    if a.get("tag"):
        filters.append({"term": {"tags": a["tag"]}})
    try:
        size = max(1, min(int(a.get("size", 100)), MAX_LIST_SIZE))
        start = max(0, int(a.get("from", 0)))
    except ValueError:
        return _fail("size and from must be integers", 400)
    try:
        r = _es().search(index=_status_alias(), ignore=404, body={
            "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
            "sort": [{"created_at": {"order": "desc", "unmapped_type": "date"}}],
            "_source": {"excludes": SUMMARY_EXCLUDES}, "size": size, "from": start})
    except Exception as e:
        return _fail(f"Failed to list workflow runs. {type(e)}:{e}", 500)
    hits = ((r or {}).get("hits") or {})
    total = hits.get("total", {})
    total = total.get("value") if isinstance(total, dict) else total
    result = [h["_source"] for h in hits.get("hits", [])]
    return {"success": True, "message": "", "total": total if total is not None else len(result),
            "count": len(result), "result": result}, 200


def steps_query(run_id):
    return {
        "size": 0,
        "query": {"term": {"workflow.run_id": run_id}},
        "aggs": {"steps": {"terms": {"field": "workflow.step_id", "size": 1000},
                           "aggs": {"statuses": {"terms": {"field": "status", "size": 50}}}}},
    }


def get_run_steps(run_id):
    try:
        doc, _ = _read_run(run_id)
    except Exception as e:
        return _fail(f"Failed to get workflow run. {type(e)}:{e}", 500)
    if doc is None:
        return _fail(f"workflow run not found: {run_id}", 404)
    try:
        r = _es().search(index=app.config["JOB_STATUS_INDEX"], body=steps_query(run_id))
    except Exception as e:
        return _fail(f"Failed to query job status. {type(e)}:{e}", 500)
    steps = {}
    for b in r.get("aggregations", {}).get("steps", {}).get("buckets", []):
        steps[b["key"]] = {s["key"]: s["doc_count"] for s in b.get("statuses", {}).get("buckets", [])}
    return {"success": True, "message": "", "run_id": run_id, "result": steps}, 200


def cancel_run(run_id):
    """404 unknown run; 409 already completed/failed; 200 already cancelled; else 202 after publishing
    ``cancel_requested`` (the engine cancels the run and revokes its jobs; mozart edits nothing)."""
    body = _json_body() or {}
    user = body.get("user") or request.args.get("user") or DEFAULT_USER
    try:
        doc, _ = _read_run(run_id)
    except Exception as e:
        return _fail(f"Failed to get workflow run. {type(e)}:{e}", 500)
    if doc is None:
        return _fail(f"workflow run not found: {run_id}", 404)
    status = doc.get("status")
    if status == "cancelled":
        return {"success": True, "message": "already cancelled", "run_id": run_id, "result": run_id}, 200
    if status in TERMINAL_RUN_STATUSES:
        return _fail(f"workflow run {run_id} is already {status}", 409)
    try:
        _state().publish_run_event(run_id, "cancel_requested", {"username": user})
    except ImportError as e:
        return _fail(f"workflow engine state is not available on this mozart ({e})", 503)
    except Exception as e:
        msg = f"Failed to request cancel of run {run_id}. {type(e)}:{e}"
        app.logger.error(msg)
        return _fail(msg, 500, run_id=run_id)
    return {"success": True, "message": "", "run_id": run_id, "result": run_id}, 202


def make_workflow_ns():
    """Build the ``workflow`` Namespace (called once per API version)."""
    ns = Namespace("workflow", description="CWL workflows and runs")

    @ns.route("/cwl/validate", endpoint="workflow-cwl-validate")
    class CwlValidate(Resource):
        @ns.doc(responses={200: "Compile result (ok, errors, warnings, compiled, graph)", 400: "No CWL"})
        def post(self):
            """Dry-run compile of CWL text; stores nothing"""
            return validate_cwl()

    @ns.route("/cwl", endpoint="workflow-cwl")
    class Cwl(Resource):
        @ns.doc(responses={200: "Already registered", 201: "Registered", 400: "Invalid CWL",
                           409: "Same id:version with different content"})
        def post(self):
            """Compile and register a CWL workflow (immutable, id:version)"""
            return register_cwl()

        def get(self):
            """List registered workflows"""
            return list_specs()

    @ns.route("/cwl/operation/<string:job_type>", endpoint="workflow-cwl-operation")
    class CwlOperation(Resource):
        @ns.doc(responses={200: "Operation YAML (?format=json for JSON)", 404: "Job type not registered"})
        def get(self, job_type):
            """The Operation snippet for a registered job type"""
            return get_operation(job_type)

    @ns.route("/cwl/<string:spec_id>", endpoint="workflow-cwl-spec")
    class Spec(Resource):
        def get(self, spec_id):
            """Get one registered workflow (compiled spec, CWL, sha256) by id:version"""
            return get_spec(spec_id)

    @ns.route("/runs", endpoint="workflow-runs")
    class Runs(Resource):
        @ns.doc(responses={202: "Accepted", 400: "Bad request", 404: "No such spec"})
        def post(self):
            """Start a workflow run"""
            return create_run()

        def get(self):
            """List runs (filters: status, spec_id, spec_version, username, tag; size, from)"""
            return list_runs()

    @ns.route("/runs/<string:run_id>", endpoint="workflow-run")
    class Run(Resource):
        def get(self, run_id):
            """Get the run document (Redis while active, else OpenSearch)"""
            return get_run(run_id)

    @ns.route("/runs/<string:run_id>/steps", endpoint="workflow-run-steps")
    class RunSteps(Resource):
        def get(self, run_id):
            """Per-step job_status counts for a run (tag aggregation; may lag)"""
            return get_run_steps(run_id)

    @ns.route("/runs/<string:run_id>/cancel", endpoint="workflow-run-cancel")
    class RunCancel(Resource):
        @ns.doc(responses={200: "Already cancelled", 202: "Cancel requested", 404: "No such run",
                           409: "Run already completed/failed"})
        def post(self, run_id):
            """Request cancellation (the engine cancels the run and revokes its jobs)"""
            return cancel_run(run_id)

    return ns
