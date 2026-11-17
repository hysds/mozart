"""``/workflow`` REST namespace (CWL, schema 2.0), API v0.1 and v0.2.

Flask test client with a fake OpenSearch; Redis and ``hysds.workflow_state`` (S4) are mocked.
The CWL compiler is the real ``hysds_commons.cwl``.
"""
import copy
import importlib.util
import os
import sys
import types
import unittest.mock as umock

import pytest
from flask import Flask
from flask_restx import Api

pytest.importorskip("cwltool")

REPO = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
TINY = open(os.path.join(REPO, "test", "fixtures", "tiny.cwl")).read()
RADIOMETER = os.path.expanduser("~/hysds-docs/examples/radiometer-orbit-l2smp-v2.cwl")


class FakeES:
    """Minimal in-memory opensearch client: index/get/search with ignore= semantics."""

    def __init__(self):
        self.docs = {}  # (index, id) -> source
        self.searches = []
        self.search_hits = []
        self.raise_on_get = None

    def index(self, index, id, body, op_type=None, ignore=None):
        if op_type == "create" and (index, id) in self.docs:
            return {"error": {"type": "version_conflict_engine_exception"}, "status": 409}
        self.docs[(index, id)] = copy.deepcopy(body)
        return {"result": "created", "_id": id}

    def get(self, index, id, ignore=None):
        if self.raise_on_get:
            raise self.raise_on_get
        if (index, id) not in self.docs:
            return {"error": {}, "status": 404, "found": False}
        return {"found": True, "_source": copy.deepcopy(self.docs[(index, id)])}

    def search(self, index=None, body=None, **kw):
        self.searches.append((index, body, kw))
        if index == "specs":
            return {"hits": {"hits": [{"_id": k[1], "_source": v} for k, v in self.docs.items()
                                      if k[0] == "specs"]}}
        if index == "status_alias":
            return {"hits": {"total": {"value": len(self.search_hits)}, "hits": self.search_hits}}
        return {"aggregations": {"steps": {"buckets": []}}, "hits": {"hits": []}}


def register_job_type(es, jt="job-a:v1", params=("x",), container="img:1", queue="q-a", outputs=None):
    es.docs[("job_specs", jt)] = {"id": jt, "container": container, "recommended-queues": [queue] if queue else [],
                                  "params": [{"name": p} for p in params]}
    io = {"id": jt.replace("job", "hysds-io", 1), "params": [{"name": p, "from": "submitter"} for p in params]}
    if outputs is not None:
        io["outputs"] = outputs
    es.docs[("hysds_ios", jt.replace("job", "hysds-io", 1))] = io
    es.docs.setdefault(("containers", container), {"id": container, "url": "x"})


@pytest.fixture(params=["01", "02"])
def env(request, monkeypatch):
    flask_app = Flask("t")
    flask_app.config.update(WORKFLOW_SPECS_INDEX="specs", WORKFLOW_STATUS_INDEX="status_alias",
                            JOB_STATUS_INDEX="job_status-current", JOB_SPECS_INDEX="job_specs",
                            HYSDS_IOS_INDEX="hysds_ios", CONTAINERS_INDEX="containers")
    es = FakeES()
    monkeypatch.setitem(sys.modules, "mozart", umock.MagicMock(app=flask_app, mozart_es=umock.MagicMock(es=es)))

    def load(name, path):
        s = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(s)
        monkeypatch.setitem(sys.modules, name, m)
        s.loader.exec_module(m)
        return m

    common = load("mozart.workflow_api", os.path.join(REPO, "mozart", "workflow_api.py"))
    thin = load(f"_wf_{request.param}", os.path.join(REPO, "mozart", "services", f"api_v{request.param}",
                                                       "workflow.py"))
    api = Api(flask_app)
    api.add_namespace(thin.workflow_ns, path="/workflow")

    # S4 contract, mocked
    state = types.ModuleType("hysds.workflow_state")
    state.init_run = umock.MagicMock()
    state.publish_run_event = umock.MagicMock()
    state.get_run = umock.MagicMock(return_value=None)
    state.list_active_runs = umock.MagicMock(return_value=[])
    monkeypatch.setitem(sys.modules, "hysds.workflow_state", state)
    monkeypatch.setattr(common, "_redis", lambda: "redis-client")
    register_job_type(es)
    c = flask_app.test_client()
    return types.SimpleNamespace(c=c, es=es, state=state, common=common)


def post_cwl(env, path, text=TINY, **kw):
    return env.c.post(path, data=text, content_type="text/yaml", **kw)


# ---- validate

def test_validate_ok_stores_nothing(env):
    r = post_cwl(env, "/workflow/cwl/validate")
    j = r.get_json()
    assert r.status_code == 200 and j["ok"] is True and j["errors"] == []
    assert j["compiled"]["steps"]["a"]["job_type"] == "job-a:v1" and "graph" in j and j["sha256"]
    assert ("specs", "tiny:1.0.0") not in env.es.docs


def test_validate_json_wrapper(env):
    r = env.c.post("/workflow/cwl/validate", json={"cwl": TINY})
    assert r.get_json()["ok"] is True


def test_validate_registry_errors(env):
    del env.es.docs[("job_specs", "job-a:v1")]
    j = post_cwl(env, "/workflow/cwl/validate").get_json()
    assert j["ok"] is False and "compiled" not in j
    assert {e["code"] for e in j["errors"]} == {"unknown_job_type"}
    assert j["errors"][0]["path"].startswith("steps.a")


def test_validate_unknown_param_uses_hysds_io(env):
    register_job_type(env.es, params=("y",))
    codes = {e["code"] for e in post_cwl(env, "/workflow/cwl/validate").get_json()["errors"]}
    assert "unknown_param" in codes


def test_validate_syntax_error(env):
    j = post_cwl(env, "/workflow/cwl/validate", text="a: [unclosed").get_json()
    assert j["ok"] is False and j["errors"]


@pytest.mark.skipif(not os.path.exists(RADIOMETER), reason="radiometer example not present")
def test_validate_unknown_job_types_radiometer(env):
    j = post_cwl(env, "/workflow/cwl/validate", text=open(RADIOMETER).read()).get_json()
    assert j["ok"] is False and "unknown_job_type" in {e["code"] for e in j["errors"]}


def test_validate_warnings_passed_through(env):
    res = umock.MagicMock(ok=True, errors=[], warnings=[{"path": "p", "code": "w", "message": "m"}],
                          sha256="abc")
    res.as_dict.return_value = {"ok": True, "warnings": res.warnings, "errors": [], "compiled": {}, "graph": {}}
    with umock.patch("hysds_commons.cwl.compile_cwl", return_value=res):
        j = post_cwl(env, "/workflow/cwl/validate").get_json()
    assert j["ok"] is True and j["warnings"][0]["code"] == "w"


def test_validate_empty_body(env):
    assert post_cwl(env, "/workflow/cwl/validate", text="  ").status_code == 400


# ---- compile call

@pytest.mark.parametrize("path", ["/workflow/cwl/validate", "/workflow/cwl"])
def test_endpoints_call_the_compiler_with_the_registry_lookup_only(env, path):
    res = umock.MagicMock(ok=False, errors=[{"path": "p", "code": "e", "message": "m"}], warnings=[], sha256="abc")
    res.as_dict.return_value = {"ok": False, "warnings": [], "errors": res.errors, "compiled": {}, "graph": {}}
    with umock.patch("hysds_commons.cwl.compile_cwl", return_value=res) as comp:
        post_cwl(env, path)
    assert len(comp.call_args.args) == 2 and not comp.call_args.kwargs


# ---- register

def test_register_new(env):
    r = post_cwl(env, "/workflow/cwl?user=alice")
    j = r.get_json()
    assert r.status_code == 201 and j["spec_id"] == "tiny:1.0.0"
    doc = env.es.docs[("specs", "tiny:1.0.0")]
    assert doc["cwl"] == TINY and doc["sha256"] == j["sha256"] and doc["username"] == "alice"
    assert doc["spec"]["steps"]["a"]["job_type"] == "job-a:v1" and doc["cwl_normalized"]["class"] == "Workflow"


def test_register_idempotent(env):
    assert post_cwl(env, "/workflow/cwl").status_code == 201
    r = post_cwl(env, "/workflow/cwl")
    assert r.status_code == 200 and r.get_json()["success"] is True


def test_register_conflict_different_sha(env):
    post_cwl(env, "/workflow/cwl")
    r = post_cwl(env, "/workflow/cwl", text=TINY + "\n# edited\n")
    assert r.status_code == 409 and "different content" in r.get_json()["message"]


def test_register_invalid_not_stored(env):
    del env.es.docs[("job_specs", "job-a:v1")]
    r = post_cwl(env, "/workflow/cwl")
    assert r.status_code == 400 and r.get_json()["errors"] and ("specs", "tiny:1.0.0") not in env.es.docs


def test_get_and_list_registered(env):
    post_cwl(env, "/workflow/cwl")
    assert env.c.get("/workflow/cwl/tiny:1.0.0").get_json()["result"]["spec"]["id"] == "tiny"
    assert env.c.get("/workflow/cwl/nope:1").status_code == 404
    assert env.c.get("/workflow/cwl").get_json()["count"] == 1


# ---- operation snippet

def test_operation_yaml(env):
    r = env.c.get("/workflow/cwl/operation/job-a:v1")
    assert r.status_code == 200 and r.mimetype == "text/yaml"
    text = r.get_data(as_text=True)
    assert "class: Operation" in text and "job_type: job-a:v1" in text and "x:" in text


def test_operation_json_and_404(env):
    j = env.c.get("/workflow/cwl/operation/job-a:v1?format=json").get_json()
    assert j["operation"]["class"] == "Operation" and "x" in j["operation"]["inputs"]
    assert env.c.get("/workflow/cwl/operation/job-zzz:v1").status_code == 404


# ---- run submit

def registered(env):
    assert post_cwl(env, "/workflow/cwl").status_code == 201


def test_submit_happy_path(env):
    registered(env)
    r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "hello"},
                                           "user": "bob", "tags": ["t1"]})
    j = r.get_json()
    assert r.status_code == 202 and j["run_id"].startswith("run-")
    (redis, spec, run), _ = env.state.init_run.call_args
    assert redis == "redis-client" and spec["id"] == "tiny"
    assert run["run_id"] == j["run_id"] and run["inputs"] == {"x": "hello"} and run["status"] == "running"
    assert run["username"] == "bob" and run["tags"] == ["t1"] and run["spec_sha256"]
    assert run["index"].startswith("workflow_status-")
    d = run["job_definitions"]["job-a:v1"]
    assert d["queue"] == "q-a" and d["container"]["id"] == "img:1" and d["job_spec"]["id"] == "job-a:v1"
    assert d["hysds_io"]["id"] == "hysds-io-a:v1"
    env.state.publish_run_event.assert_called_once_with(j["run_id"], "run_started")


def test_submit_init_precedes_publish(env):
    registered(env)
    order = []
    env.state.init_run.side_effect = lambda *a: order.append("init")
    env.state.publish_run_event.side_effect = lambda *a, **k: order.append("publish")
    env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "h"}})
    assert order == ["init", "publish"]


def test_submit_bad_inputs(env):
    registered(env)
    for inputs in ({}, {"x": 5}, {"x": "a", "zzz": 1}):
        r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": inputs})
        assert r.status_code == 400 and r.get_json()["errors"], inputs
    env.state.init_run.assert_not_called()


def test_submit_unknown_spec_and_missing_id(env):
    assert env.c.post("/workflow/runs", json={"spec_id": "nope:1"}).status_code == 404
    assert env.c.post("/workflow/runs", json={}).status_code == 400


def test_submit_missing_job_type_at_freeze(env):
    registered(env)
    del env.es.docs[("job_specs", "job-a:v1")]
    r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "h"}})
    assert r.status_code == 400 and "job-a:v1" in r.get_json()["message"]
    env.state.init_run.assert_not_called()


def test_submit_missing_container_at_freeze(env):
    registered(env)
    del env.es.docs[("containers", "img:1")]
    r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "h"}})
    assert r.status_code == 400 and "container" in r.get_json()["message"]


def test_submit_no_queue(env):
    registered(env)
    register_job_type(env.es, queue=None)
    r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "h"}})
    assert r.status_code == 400 and "no queue" in r.get_json()["message"]


def test_submit_engine_state_unavailable(env, monkeypatch):
    registered(env)
    monkeypatch.setitem(sys.modules, "hysds.workflow_state", None)  # import raises ImportError
    r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "h"}})
    assert r.status_code == 503


def test_submit_init_failure(env):
    registered(env)
    env.state.init_run.side_effect = RuntimeError("redis down")
    r = env.c.post("/workflow/runs", json={"spec_id": "tiny:1.0.0", "inputs": {"x": "h"}})
    assert r.status_code == 500
    env.state.publish_run_event.assert_not_called()


# ---- status

def test_status_from_redis(env):
    env.state.get_run.return_value = {"run_id": "run-1", "status": "running", "version": 3}
    j = env.c.get("/workflow/runs/run-1").get_json()
    assert j["source"] == "redis" and j["result"]["version"] == 3
    env.state.get_run.assert_called_once_with("redis-client", "run-1")
    assert env.es.searches == []


def test_status_from_opensearch_when_not_active(env):
    env.es.search_hits = [{"_source": {"run_id": "run-1", "status": "completed", "version": 9}}]
    j = env.c.get("/workflow/runs/run-1").get_json()
    assert j["source"] == "opensearch" and j["result"]["status"] == "completed"
    index, body, _ = env.es.searches[0]
    assert index == "status_alias" and body["query"] == {"term": {"run_id": "run-1"}}


def test_status_opensearch_when_redis_errors(env):
    env.state.get_run.side_effect = ConnectionError("down")
    env.es.search_hits = [{"_source": {"run_id": "run-1", "status": "failed"}}]
    assert env.c.get("/workflow/runs/run-1").get_json()["source"] == "opensearch"


def test_status_not_found(env):
    assert env.c.get("/workflow/runs/run-x").status_code == 404


def test_list_runs_through_alias(env):
    env.es.search_hits = [{"_source": {"run_id": "r1"}}, {"_source": {"run_id": "r2"}}]
    j = env.c.get("/workflow/runs?status=running&spec_id=tiny:1.0.0&size=5").get_json()
    assert j["count"] == 2 and j["total"] == 2
    index, body, _ = env.es.searches[0]
    assert index == "status_alias" and body["size"] == 5
    assert {"term": {"status": "running"}} in body["query"]["bool"]["filter"]
    assert "steps" in body["_source"]["excludes"]


# ---- cancel

def test_cancel_publishes_event(env):
    env.state.get_run.return_value = {"run_id": "run-1", "status": "running"}
    r = env.c.post("/workflow/runs/run-1/cancel", json={"user": "carol"})
    assert r.status_code == 202
    env.state.publish_run_event.assert_called_once_with("run-1", "cancel_requested", {"username": "carol"})


def test_cancel_unknown_terminal_cancelled(env):
    assert env.c.post("/workflow/runs/run-x/cancel").status_code == 404
    env.state.get_run.return_value = {"run_id": "r", "status": "completed"}
    assert env.c.post("/workflow/runs/r/cancel").status_code == 409
    env.state.get_run.return_value = {"run_id": "r", "status": "cancelled"}
    assert env.c.post("/workflow/runs/r/cancel").status_code == 200
    env.state.publish_run_event.assert_not_called()


def test_cancel_from_opensearch_record(env):
    env.es.search_hits = [{"_source": {"run_id": "run-1", "status": "running"}}]
    assert env.c.post("/workflow/runs/run-1/cancel").status_code == 202


# ---- v1 is gone

def test_v1_endpoints_removed(env):
    assert env.c.post("/workflow/specs", json={}).status_code in (404, 405)
    assert env.c.get("/workflow/specs").status_code in (404, 405)
