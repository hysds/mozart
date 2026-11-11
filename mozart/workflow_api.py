"""Shared implementation of the mozart ``/workflow`` REST namespace (contract section 6).

``api_v01/workflow.py`` and ``api_v02/workflow.py`` are thin wrappers around
:func:`make_workflow_ns`, so the two API versions cannot drift.

All expected-404/409 paths use the raw opensearch client with ``ignore=`` (or the
``hysds_commons.workflow_status`` helpers): the ``OpenSearchUtility`` wrapper retries
every error, 404/409 included, for ~34s.
"""
import uuid
from datetime import datetime, timezone

from flask import request
from flask_restx import Namespace, Resource

from hysds_commons.workflow_cancel import (
    TERMINAL_RUN_STATUSES,
    cancel_mutate,
    resolve_revoke_job_type,
    sweep_run,
)
from hysds_commons.workflow_refs import WorkflowRefError, build_context, resolve_inputs
from hysds_commons.workflow_spec import (
    DEFAULT_WORKFLOW_CONNECTORS,
    WorkflowSpecError,
    check_spec,
    resolve_connectors,
    spec_doc_id,
    validate_run_inputs,
)
from hysds_commons.workflow_status import (
    WorkflowStatusNotFound,
    create_status,
    get_status,
    update_status,
)
# mozart is the submission authority, so it submits the entry job in-process
from hysds_commons.workflow_submit import submit_workflow_job_inprocess as submit_workflow_job, workflow_tag

from mozart import app, mozart_es

DEFAULT_USER = "anonymous"


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _fail(message, code):
    return {"success": False, "message": message}, code


def _es():
    return getattr(mozart_es, "es", mozart_es)


def _specs_index():
    return app.config["WORKFLOW_SPECS_INDEX"]


def _status_index():
    return app.config["WORKFLOW_STATUS_INDEX"]


def _body():
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else None


def _is_error(resp, code):
    return isinstance(resp, dict) and "error" in resp and resp.get("status") == code


def _missing_connector_specs(spec):
    """[(node_id, kind, connector)] whose connector job_spec is absent from the job_specs index."""
    missing, seen = [], {}
    for nid, node in spec["nodes"].items():
        conn = node.get("connector")
        if not conn:
            continue
        if conn not in seen:
            r = _es().get(index=app.config["JOB_SPECS_INDEX"], id=conn, ignore=404)
            seen[conn] = not (_is_error(r, 404) or not r.get("found", True))
        if not seen[conn]:
            missing.append((nid, node["kind"], conn))
    return missing


def create_spec():
    spec = _body()
    if spec is None:
        return _fail("request body must be a JSON object", 400)
    try:
        check_spec(spec)
    except WorkflowSpecError as e:
        return {
            "success": False,
            "message": "invalid workflow spec",
            "errors": list(e.errors),
        }, 400
    spec, errors = resolve_connectors(spec, app.config.get("WORKFLOW_CONNECTORS", DEFAULT_WORKFLOW_CONNECTORS))
    if errors:
        return {"success": False, "message": "invalid workflow spec", "errors": errors}, 400
    try:
        missing = _missing_connector_specs(spec)
    except Exception as e:
        app.logger.error("workflow connector job_spec lookup failed: %s", e)
        return _fail(f"Failed to check connector job_specs. {type(e)}:{e}", 500)
    if missing:
        return {
            "success": False,
            "message": "connector job_spec(s) not registered",
            "errors": [f"node '{nid}' ({kind}): connector job_spec '{c}' is not registered in the "
                       f"job_specs index" for nid, kind, c in missing],
        }, 400
    spec_id = spec_doc_id(spec)
    now = _now()
    doc = dict(spec)
    doc.setdefault("username", DEFAULT_USER)
    doc["creation_time"] = now
    doc["modification_time"] = now
    try:
        r = _es().index(
            index=_specs_index(), id=spec_id, body=doc, op_type="create", ignore=409
        )
    except Exception as e:
        app.logger.error("workflow spec create failed: %s", e)
        return _fail(f"Failed to store workflow spec. {type(e)}:{e}", 500)
    if _is_error(r, 409):
        return _fail(f"workflow spec {spec_id} already exists", 409)
    return {"success": True, "message": "", "spec_id": spec_id, "result": spec_id}, 201


def list_specs():
    try:
        r = _es().search(
            index=_specs_index(),
            body={
                "query": {"match_all": {}},
                "sort": [{"creation_time": {"order": "desc", "unmapped_type": "date"}}],
                "_source": ["id", "version", "label", "username", "creation_time"],
            },
            size=1000,
        )
    except Exception as e:
        app.logger.error("workflow spec list failed: %s", e)
        return _fail(f"Failed to list workflow specs. {type(e)}:{e}", 500)
    result = []
    for hit in r["hits"]["hits"]:
        item = dict(hit["_source"])
        item["spec_id"] = hit["_id"]
        result.append(item)
    return {"success": True, "message": "", "count": len(result), "result": result}, 200


def _load_spec(spec_id):
    r = _es().get(index=_specs_index(), id=spec_id, ignore=404)
    if _is_error(r, 404) or not r.get("found", True):
        return None
    return r["_source"]


def get_spec(spec_id):
    try:
        spec = _load_spec(spec_id)
    except Exception as e:
        return _fail(f"Failed to get workflow spec. {type(e)}:{e}", 500)
    if spec is None:
        return _fail(f"workflow spec not found: {spec_id}", 404)
    return {"success": True, "message": "", "result": spec}, 200


def _mark_failed(run_id, entry, error):
    def mutate(doc):
        doc["status"] = "failed"
        doc["error"] = error
        doc["updated_at"] = _now()
        if entry in doc.get("nodes", {}):
            doc["nodes"][entry] = {**doc["nodes"][entry], "state": "failed"}
        return doc

    try:
        update_status(_es(), _status_index(), run_id, mutate)
    except Exception as e:  # best effort: the original error is what we report
        app.logger.error("could not mark run %s failed: %s", run_id, e)


def create_run():
    body = _body()
    if body is None:
        return _fail("request body must be a JSON object", 400)
    spec_id = body.get("spec_id")
    if not isinstance(spec_id, str) or not spec_id:
        return _fail("spec_id is required", 400)
    try:
        spec = _load_spec(spec_id)
    except Exception as e:
        return _fail(f"Failed to load workflow spec. {type(e)}:{e}", 500)
    if spec is None:
        return _fail(f"workflow spec not found: {spec_id}", 404)

    inputs, errors = validate_run_inputs(spec, body.get("inputs"))
    if errors:
        return {"success": False, "message": "invalid run inputs", "errors": errors}, 400

    user = body.get("user") or DEFAULT_USER
    entry = spec["entry"]
    node = spec["nodes"][entry]
    queue = node.get("queue") or (spec.get("defaults") or {}).get("queue")
    if not queue:
        return _fail(
            f"no queue for entry node '{entry}': set node.queue or defaults.queue", 400
        )

    run_id = "run-" + uuid.uuid4().hex
    now = _now()
    status_doc = {
        "run_id": run_id,
        "spec_id": spec_id,
        "status": "running",
        "user": user,
        "inputs": inputs,
        "submitted_at": now,
        "updated_at": now,
        "error": None,
        "outputs": None,
        "nodes": {nid: {"state": "pending"} for nid in spec["nodes"]},
    }
    try:
        create_status(_es(), _status_index(), run_id, status_doc)
    except Exception as e:
        app.logger.error("workflow run create failed: %s", e)
        return _fail(f"Failed to create workflow run. {type(e)}:{e}", 500)

    try:
        params = resolve_inputs(node, build_context(status_doc))
    except WorkflowRefError as e:
        msg = f"entry '{entry}' input resolution failed: {e}"
        _mark_failed(run_id, entry, msg)
        return {"success": False, "message": msg, "run_id": run_id}, 400

    try:
        payload_id = submit_workflow_job(
            job_type=node["run"],
            queue=queue,
            params=params,
            workflow=workflow_tag(run_id, entry, spec_id),
            username=user,
        )
    except Exception as e:
        msg = f"Failed to submit entry '{entry}'. {type(e)}:{e}"
        app.logger.error(msg)
        _mark_failed(run_id, entry, msg)
        return {"success": False, "message": msg, "run_id": run_id}, 500

    def mutate(doc):
        # The entry may already have finished and been advanced by a connector
        # (fast job, slow write here). Only a still-pending entry is flipped,
        # otherwise we would clobber 'completed' back to 'running'.
        node_state = doc["nodes"].get(entry) or {}
        if node_state.get("state") != "pending":
            return None
        doc["nodes"][entry] = {"state": "running", "payload_id": payload_id}
        doc["updated_at"] = _now()
        return doc

    try:
        update_status(_es(), _status_index(), run_id, mutate)
    except Exception as e:
        msg = f"Entry submitted ({payload_id}) but run status update failed. {type(e)}:{e}"
        app.logger.error(msg)
        return {"success": False, "message": msg, "run_id": run_id}, 500
    return {"success": True, "message": "", "run_id": run_id, "result": run_id}, 202


def get_run(run_id):
    try:
        doc, _, _ = get_status(_es(), _status_index(), run_id)
    except WorkflowStatusNotFound:
        return _fail(f"workflow run not found: {run_id}", 404)
    except Exception as e:
        return _fail(f"Failed to get workflow run. {type(e)}:{e}", 500)
    return {"success": True, "message": "", "result": doc}, 200


def steps_query(run_id):
    return {
        "size": 0,
        "query": {"term": {"workflow.run_id": run_id}},
        "aggs": {
            "steps": {
                "terms": {"field": "workflow.step_id", "size": 1000},
                "aggs": {"statuses": {"terms": {"field": "status", "size": 50}}},
            }
        },
    }


def get_run_steps(run_id):
    try:
        get_status(_es(), _status_index(), run_id)
    except WorkflowStatusNotFound:
        return _fail(f"workflow run not found: {run_id}", 404)
    except Exception as e:
        return _fail(f"Failed to get workflow run. {type(e)}:{e}", 500)
    try:
        r = _es().search(
            index=app.config["JOB_STATUS_INDEX"], body=steps_query(run_id)
        )
    except Exception as e:
        return _fail(f"Failed to query job status. {type(e)}:{e}", 500)
    steps = {}
    for b in r.get("aggregations", {}).get("steps", {}).get("buckets", []):
        steps[b["key"]] = {
            s["key"]: s["doc_count"] for s in b.get("statuses", {}).get("buckets", [])
        }
    return {"success": True, "message": "", "run_id": run_id, "result": steps}, 200


def cancel_run(run_id):
    """Contract 12.5. 404 unknown run; 409 terminal and not cancelled; 200 already
    cancelled (sweep only); else 202 after cancelling the run and revoking its jobs."""
    body = _body() or {}
    user = body.get("user") or request.args.get("user") or DEFAULT_USER
    try:
        doc, _, _ = get_status(_es(), _status_index(), run_id)
    except WorkflowStatusNotFound:
        return _fail(f"workflow run not found: {run_id}", 404)
    except Exception as e:
        return _fail(f"Failed to get workflow run. {type(e)}:{e}", 500)

    status = doc.get("status")
    already = status == "cancelled"
    if status in TERMINAL_RUN_STATUSES and not already:
        return _fail(f"workflow run {run_id} is already {status}", 409)
    if not already:
        try:
            doc = update_status(_es(), _status_index(), run_id, cancel_mutate(user))
        except Exception as e:
            app.logger.error("workflow run cancel failed: %s", e)
            return _fail(f"Failed to cancel workflow run. {type(e)}:{e}", 500)
        if doc.get("status") != "cancelled":  # lost a race with completion/failure
            return _fail(f"workflow run {run_id} is already {doc.get('status')}", 409)

    try:
        job_type = resolve_revoke_job_type(
            _es(), app.config["JOB_SPECS_INDEX"], app.config.get("WORKFLOW_REVOKE_JOB_TYPE"))
        revoked = sweep_run(_es(), app.config["JOB_STATUS_INDEX"], run_id,
                            submit_workflow_job, job_type, username=user)
    except Exception as e:
        msg = f"Run {run_id} is cancelled but revoking its jobs failed (retry the cancel). {type(e)}:{e}"
        app.logger.error(msg)
        return {"success": False, "message": msg, "run_id": run_id}, 500
    return {"success": True, "message": "", "run_id": run_id, "revoked": revoked,
            "result": run_id}, (200 if already else 202)


def make_workflow_ns():
    """Build the ``workflow`` Namespace (called once per API version)."""
    ns = Namespace("workflow", description="Workflow specs and runs")

    @ns.route("/specs", endpoint="workflow-specs")
    class Specs(Resource):
        @ns.doc(responses={201: "Created", 400: "Invalid spec", 409: "Exists"})
        def post(self):
            """Validate and store a workflow spec (immutable, id:version)"""
            return create_spec()

        def get(self):
            """List workflow specs"""
            return list_specs()

    @ns.route("/specs/<string:spec_id>", endpoint="workflow-spec")
    class Spec(Resource):
        def get(self, spec_id):
            """Get one workflow spec by id:version"""
            return get_spec(spec_id)

    @ns.route("/runs", endpoint="workflow-runs")
    class Runs(Resource):
        @ns.doc(responses={202: "Accepted", 400: "Bad request", 404: "No such spec"})
        def post(self):
            """Start a workflow run"""
            return create_run()

    @ns.route("/runs/<string:run_id>", endpoint="workflow-run")
    class Run(Resource):
        def get(self, run_id):
            """Get the workflow run status document"""
            return get_run(run_id)

    @ns.route("/runs/<string:run_id>/steps", endpoint="workflow-run-steps")
    class RunSteps(Resource):
        def get(self, run_id):
            """Per-step job_status counts for a run (tag aggregation; may lag)"""
            return get_run_steps(run_id)

    @ns.route("/runs/<string:run_id>/cancel", endpoint="workflow-run-cancel")
    class RunCancel(Resource):
        @ns.doc(responses={200: "Already cancelled (sweep only)", 202: "Cancelled",
                           404: "No such run", 409: "Run already completed/failed"})
        def post(self, run_id):
            """Cancel a run and revoke its non-terminal jobs"""
            return cancel_run(run_id)

    return ns
