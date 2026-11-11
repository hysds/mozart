"""``/workflow`` REST namespace, API v0.1 and v0.2 (Flask test client, fake ES)."""
import copy
import importlib.util
import os
import sys
import unittest.mock as umock

import pytest
from flask import Flask
from flask_restx import Api

from hysds_commons.workflow_examples import linear_spec, scatter_gather_spec

DEFAULT_WORKFLOW_CONNECTORS_IDS = ["job-workflow-pass:v1.0.0", "job-workflow-scatter:v1.0.0",
                                   "job-workflow-gather:v1.0.0"]
REPO = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class FakeES:
    """Minimal in-memory opensearch client: index/get/search with ignore= semantics."""

    def __init__(self):
        self.docs = {}  # (index, id) -> (source, seq_no)
        self.searches = []
        self.search_result = {"aggregations": {"steps": {"buckets": []}}, "hits": {"hits": []}}
        self._seq = 0

    def index(self, index, id, body, op_type=None, if_seq_no=None, if_primary_term=None,
              ignore=None):
        key = (index, id)
        if op_type == "create" and key in self.docs:
            return {"error": {"type": "version_conflict_engine_exception"}, "status": 409}
        if if_seq_no is not None and self.docs[key][1] != if_seq_no:
            return {"error": {}, "status": 409}
        self._seq += 1
        self.docs[key] = (copy.deepcopy(body), self._seq)
        return {"result": "created", "_id": id}

    def get(self, index, id, ignore=None):
        if (index, id) not in self.docs:
            return {"error": {}, "status": 404, "found": False}
        src, seq = self.docs[(index, id)]
        return {"found": True, "_source": copy.deepcopy(src), "_seq_no": seq,
                "_primary_term": 1}

    def search(self, index=None, body=None, **kw):
        self.searches.append((index, body, kw))
        if index == "specs":
            hits = [{"_id": k[1], "_source": v[0]} for k, v in self.docs.items()
                    if k[0] == "specs"]
            return {"hits": {"hits": hits}}
        return self.search_result

    def doc(self, index, id):
        return self.docs[(index, id)][0]


def _load(version, monkeypatch):
    flask_app = Flask("t")
    flask_app.config.update(WORKFLOW_SPECS_INDEX="specs", WORKFLOW_STATUS_INDEX="status",
                            JOB_STATUS_INDEX="job_status-current",
                            JOB_SPECS_INDEX="job_specs",
                            WORKFLOW_REVOKE_JOB_TYPE="job-lw-mozart-revoke:test")
    es = FakeES()
    for jt in DEFAULT_WORKFLOW_CONNECTORS_IDS:  # default connector job_specs are registered
        es.docs[("job_specs", jt)] = ({"id": jt}, 1)
    monkeypatch.setitem(sys.modules, "mozart",
                        umock.MagicMock(app=flask_app, mozart_es=umock.MagicMock(es=es)))

    def load(name, path):
        s = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(s)
        monkeypatch.setitem(sys.modules, name, m)
        s.loader.exec_module(m)
        return m

    common = load("mozart.workflow_api", os.path.join(REPO, "mozart", "workflow_api.py"))
    thin = load(f"_wf_{version}", os.path.join(
        REPO, "mozart", "services", f"api_v{version}", "workflow.py"))
    api = Api(flask_app)
    api.add_namespace(thin.workflow_ns, path="/workflow")
    submit = umock.MagicMock(return_value="payload-1")
    monkeypatch.setattr(common, "submit_workflow_job", submit)
    return flask_app.test_client(), es, submit


@pytest.fixture(params=["01", "02"])
def env(request, monkeypatch):
    return _load(request.param, monkeypatch)


@pytest.fixture
def stored(env):
    c, es, submit = env
    assert c.post("/workflow/specs", json=linear_spec()).status_code == 201
    return env


# ---- specs

@pytest.mark.parametrize("fixture", [linear_spec, scatter_gather_spec])
def test_spec_post_valid(env, fixture):
    c, es, _ = env
    spec = fixture()
    r = c.post("/workflow/specs", json=spec)
    assert r.status_code == 201, r.data
    sid = f"{spec['id']}:{spec['version']}"
    assert r.get_json()["spec_id"] == sid
    stored_doc = es.doc("specs", sid)
    assert stored_doc["entry"] == spec["entry"]
    # stored spec is fully resolved (contract 11.3); everything else is as posted
    for nid, n in stored_doc["nodes"].items():
        k = n["kind"]
        if k in ("pass", "scatter", "gather"):
            assert n.pop("connector") == f"job-workflow-{k}:v1.0.0"
            assert n.pop("connector_queue") == f"workflow-connector-{k}"
    assert stored_doc["nodes"] == spec["nodes"]


def test_spec_post_invalid_lists_all_errors(env):
    c, es, _ = env
    spec = linear_spec()
    spec["entry"] = "nope"
    spec["nodes"]["A"]["kind"] = "bogus"
    spec["extra_field"] = 1
    r = c.post("/workflow/specs", json=spec)
    assert r.status_code == 400
    body = r.get_json()
    assert body["success"] is False and len(body["errors"]) >= 2
    assert not any(k[0] == "specs" for k in es.docs)


def test_spec_connector_not_allowed_400(env):
    c, es, _ = env
    spec = scatter_gather_spec()
    spec["nodes"]["scatter_1"]["connector"] = "job-rogue:v1"
    r = c.post("/workflow/specs", json=spec)
    assert r.status_code == 400
    errs = r.get_json()["errors"]
    assert "scatter_1" in errs[0] and "job-workflow-scatter:v1.0.0" in errs[0]
    assert ("specs", "demo-scatter:1.0.0") not in es.docs and not any(k[0] == "specs" for k in es.docs)


def test_spec_connector_not_registered_400(env):
    c, es, _ = env
    del es.docs[("job_specs", "job-workflow-gather:v1.0.0")]
    r = c.post("/workflow/specs", json=scatter_gather_spec())
    assert r.status_code == 400
    errs = r.get_json()["errors"]
    assert errs and all("job-workflow-gather:v1.0.0" in e for e in errs)
    assert not any(k[0] == "specs" for k in es.docs)


def test_spec_mission_connector_stored_with_queue(env):
    c, es, _ = env
    c.application.config["WORKFLOW_CONNECTORS"] = {
        "pass": {"default": "job-workflow-pass:v1.0.0",
                 "allowed": {"job-workflow-pass:v1.0.0": "workflow-connector-pass"}},
        "scatter": {"default": "job-workflow-scatter:v1.0.0",
                    "allowed": {"job-workflow-scatter:v1.0.0": "workflow-connector-scatter",
                                "job-m-scatter:v1": "m-queue"}},
        "gather": {"default": "job-workflow-gather:v1.0.0",
                   "allowed": {"job-workflow-gather:v1.0.0": "workflow-connector-gather"}},
    }
    es.docs[("job_specs", "job-m-scatter:v1")] = ({}, 1)
    spec = scatter_gather_spec()
    spec["nodes"]["scatter_1"].update(connector="job-m-scatter:v1", connector_queue="ignored")
    r = c.post("/workflow/specs", json=spec)
    assert r.status_code == 201, r.data
    n = es.doc("specs", r.get_json()["spec_id"])["nodes"]["scatter_1"]
    assert n["connector"] == "job-m-scatter:v1" and n["connector_queue"] == "m-queue"


def test_spec_post_non_object(env):
    c, _, _ = env
    assert c.post("/workflow/specs", json=[1]).status_code == 400


def test_spec_duplicate_409(env):
    c, _, _ = env
    assert c.post("/workflow/specs", json=linear_spec()).status_code == 201
    assert c.post("/workflow/specs", json=linear_spec()).status_code == 409


def test_spec_get_and_list(stored):
    c, _, _ = stored
    r = c.get("/workflow/specs/demo-linear:1.0.0")
    assert r.status_code == 200 and r.get_json()["result"]["id"] == "demo-linear"
    assert c.get("/workflow/specs/nope:1").status_code == 404
    r = c.get("/workflow/specs")
    body = r.get_json()
    assert r.status_code == 200 and body["count"] == 1
    assert body["result"][0]["spec_id"] == "demo-linear:1.0.0"


# ---- runs

def _post_run(c, **over):
    body = {"spec_id": "demo-linear:1.0.0", "inputs": {"n": 3}, "user": "jdoe"}
    body.update(over)
    return c.post("/workflow/runs", json=body)


def test_run_happy_path(stored):
    c, es, submit = stored
    r = _post_run(c)
    assert r.status_code == 202, r.data
    run_id = r.get_json()["run_id"]
    assert run_id.startswith("run-") and len(run_id) == 36
    doc = es.doc("status", run_id)
    assert doc["status"] == "running" and doc["spec_id"] == "demo-linear:1.0.0"
    assert doc["user"] == "jdoe" and doc["error"] is None and doc["outputs"] is None
    assert doc["inputs"] == {"greeting": "hi", "n": 3}
    assert doc["submitted_at"].endswith("Z") and doc["updated_at"].endswith("Z")
    assert doc["nodes"]["A"] == {"state": "running", "payload_id": "payload-1"}
    assert doc["nodes"]["pass_1"] == {"state": "pending"}
    assert doc["nodes"]["B"] == {"state": "pending"}
    kw = submit.call_args.kwargs
    assert kw["job_type"] == "job-wf_test:v1.0"
    assert kw["queue"] == "hello_world_worker"
    assert kw["params"] == {"mode": "ok", "outputs": "hi"}
    assert kw["workflow"] == {"run_id": run_id, "step_id": "A",
                              "spec_id": "demo-linear:1.0.0",
                              "child_index": None, "child_total": None}
    assert kw["username"] == "jdoe"


def test_run_node_queue_overrides_default(env):
    c, es, submit = env
    spec = linear_spec()
    spec["nodes"]["A"]["queue"] = "special"
    c.post("/workflow/specs", json=spec)
    assert _post_run(c).status_code == 202
    assert submit.call_args.kwargs["queue"] == "special"


def test_run_no_queue_400(env):
    c, es, submit = env
    spec = linear_spec()
    del spec["defaults"]["queue"]
    c.post("/workflow/specs", json=spec)
    r = _post_run(c)
    assert r.status_code == 400 and "queue" in r.get_json()["message"]
    submit.assert_not_called()


def test_run_unknown_spec_404(env):
    c, _, _ = env
    assert _post_run(c, spec_id="nope:1").status_code == 404
    assert c.post("/workflow/runs", json={}).status_code == 400


def test_run_bad_inputs_400(stored):
    c, es, submit = stored
    r = _post_run(c, inputs={"n": "x", "bogus": 1})
    assert r.status_code == 400 and len(r.get_json()["errors"]) == 2
    r = _post_run(c, inputs={})
    assert r.status_code == 400 and "required" in r.get_json()["errors"][0]
    submit.assert_not_called()
    assert not [k for k in es.docs if k[0] == "status"]


def test_run_entry_ref_failure_marks_failed(env):
    c, es, submit = env
    spec = linear_spec()
    # passes spec validation but cannot resolve at runtime: no such input value supplied
    spec["nodes"]["A"]["in"] = {"x": "$.inputs.n"}
    spec["inputs"]["n"] = {"type": "int", "default": 1}
    c.post("/workflow/specs", json=spec)
    with umock.patch.object(sys.modules["mozart.workflow_api"], "resolve_inputs",
                            side_effect=sys.modules["mozart.workflow_api"].WorkflowRefError("boom")):
        r = _post_run(c, inputs={})
    assert r.status_code == 400
    run_id = r.get_json()["run_id"]
    doc = es.doc("status", run_id)
    assert doc["status"] == "failed" and "boom" in doc["error"]
    assert doc["nodes"]["A"]["state"] == "failed"
    submit.assert_not_called()


def test_run_submit_failure_500_marks_failed(stored):
    c, es, submit = stored
    submit.side_effect = RuntimeError("amqp down")
    r = _post_run(c)
    assert r.status_code == 500 and "amqp down" in r.get_json()["message"]
    doc = es.doc("status", r.get_json()["run_id"])
    assert doc["status"] == "failed" and "amqp down" in doc["error"]


def test_run_get(stored):
    c, es, _ = stored
    run_id = _post_run(c).get_json()["run_id"]
    r = c.get(f"/workflow/runs/{run_id}")
    assert r.status_code == 200
    assert r.get_json()["result"]["run_id"] == run_id
    assert c.get("/workflow/runs/run-missing").status_code == 404


def test_steps_aggregation(stored):
    c, es, _ = stored
    run_id = _post_run(c).get_json()["run_id"]
    es.search_result = {"aggregations": {"steps": {"buckets": [
        {"key": "A", "doc_count": 1,
         "statuses": {"buckets": [{"key": "job-completed", "doc_count": 1}]}}]}}}
    r = c.get(f"/workflow/runs/{run_id}/steps")
    assert r.status_code == 200
    assert r.get_json()["result"] == {"A": {"job-completed": 1}}
    index, body, _ = es.searches[-1]
    assert index == "job_status-current"
    assert body["size"] == 0
    assert body["query"] == {"term": {"workflow.run_id": run_id}}
    steps = body["aggs"]["steps"]
    assert steps["terms"]["field"] == "workflow.step_id"
    assert steps["aggs"]["statuses"]["terms"]["field"] == "status"


def test_steps_empty_and_404(stored):
    c, es, _ = stored
    run_id = _post_run(c).get_json()["run_id"]
    es.search_result = {"hits": {"hits": []}}
    assert c.get(f"/workflow/runs/{run_id}/steps").get_json()["result"] == {}
    assert c.get("/workflow/runs/run-missing/steps").status_code == 404


# ---- cancel (12.5)

def _hits(*ids):
    return {"hits": {"hits": [{"_id": i, "_source": {}, "sort": [n, i]}
                              for n, i in enumerate(ids)]}}


def test_cancel_running_run_202(stored):
    c, es, submit = stored
    run_id = _post_run(c).get_json()["run_id"]
    es.search_result = _hits("p1", "p2")
    r = c.post(f"/workflow/runs/{run_id}/cancel", json={"user": "bob"})
    assert r.status_code == 202, r.data
    assert r.get_json()["revoked"] == 2 and r.get_json()["run_id"] == run_id
    doc = es.doc("status", run_id)
    assert doc["status"] == "cancelled" and doc["error"] == "cancelled by bob"
    assert {n["state"] for n in doc["nodes"].values()} == {"cancelled"}
    kw = submit.call_args.kwargs  # the last call is the revoke (entry submit was earlier)
    assert kw["job_type"] == "job-lw-mozart-revoke:test" and kw["queue"] == "system-jobs-queue"
    assert kw["workflow"] is None and kw["params"]["operation"] == "revoke"
    assert kw["params"]["component"] == "mozart"
    import json as _j
    assert _j.loads(kw["params"]["query"])["query"]["ids"]["values"] == ["p1", "p2"]
    index, body, _ = es.searches[-1]
    assert index == "job_status-current"
    assert body["query"]["bool"]["filter"] == [{"term": {"workflow.run_id": run_id}}]


def test_cancel_no_jobs_submits_nothing(stored):
    c, es, submit = stored
    run_id = _post_run(c).get_json()["run_id"]
    submit.reset_mock()
    es.search_result = {"hits": {"hits": []}}
    r = c.post(f"/workflow/runs/{run_id}/cancel")
    assert r.status_code == 202 and r.get_json()["revoked"] == 0
    assert es.doc("status", run_id)["error"] == "cancelled by anonymous"
    submit.assert_not_called()


def test_cancel_unknown_404(stored):
    c, es, _ = stored
    assert c.post("/workflow/runs/run-missing/cancel").status_code == 404


@pytest.mark.parametrize("st", ["completed", "failed"])
def test_cancel_terminal_409(stored, st):
    c, es, submit = stored
    run_id = _post_run(c).get_json()["run_id"]
    doc = es.doc("status", run_id)
    doc["status"] = st
    es.index("status", run_id, doc)
    submit.reset_mock()
    r = c.post(f"/workflow/runs/{run_id}/cancel")
    assert r.status_code == 409 and st in r.get_json()["message"]
    submit.assert_not_called()
    assert es.doc("status", run_id)["status"] == st


def test_cancel_already_cancelled_200_sweeps(stored):
    c, es, submit = stored
    run_id = _post_run(c).get_json()["run_id"]
    es.search_result = _hits()
    assert c.post(f"/workflow/runs/{run_id}/cancel").status_code == 202
    es.search_result = _hits("late-1")
    before = es.doc("status", run_id)
    r = c.post(f"/workflow/runs/{run_id}/cancel")
    assert r.status_code == 200 and r.get_json()["revoked"] == 1
    assert es.doc("status", run_id) == before
    assert submit.call_args.kwargs["params"]["operation"] == "revoke"


def test_cancel_revoke_failure_500_run_stays_cancelled(stored):
    c, es, submit = stored
    run_id = _post_run(c).get_json()["run_id"]
    es.search_result = _hits("p1")
    submit.side_effect = RuntimeError("amqp down")
    r = c.post(f"/workflow/runs/{run_id}/cancel")
    assert r.status_code == 500 and "retry the cancel" in r.get_json()["message"]
    assert es.doc("status", run_id)["status"] == "cancelled"
