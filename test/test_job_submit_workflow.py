"""``workflow`` param on POST job/submit in API v0.1 and v0.2 (Flask test client).

The mozart package needs a rendered settings.cfg and live ES at import, so the
heavy modules are stubbed and the two ``jobs`` modules are loaded from source.
"""
import importlib.util
import json
import os
import sys
import unittest.mock as umock

import pytest
from flask import Flask
from flask_restx import Api

import hysds_commons  # noqa: F401  (real package; job_utils is patched below)

REPO = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
WF = {"run_id": "run-1", "step_id": "step-1", "child_index": 1}


def _load(version, monkeypatch):
    flask_app = Flask("t")
    flask_app.config.update(HYSDS_IOS_INDEX="h", JOB_SPECS_INDEX="j", JOB_STATUS_INDEX="s",
                            CONTAINERS_INDEX="c")
    job_utils = umock.MagicMock()
    job_utils.resolve_hysds_job.return_value = {"job": 1}
    job_utils.submit_hysds_job.return_value = "payload-id-1"
    stubs = {
        "mozart": umock.MagicMock(app=flask_app, mozart_es=umock.MagicMock()),
        "mozart.lib": umock.MagicMock(),
        "mozart.lib.queue_utils": umock.MagicMock(),
        "hysds.celery": umock.MagicMock(),
        "hysds.task_worker": umock.MagicMock(),
        "hysds_commons.job_utils": job_utils,
        "hysds_commons.action_utils": umock.MagicMock(),
    }
    for k, v in stubs.items():
        monkeypatch.setitem(sys.modules, k, v)
    monkeypatch.setattr(hysds_commons, "job_utils", job_utils, raising=False)
    path = os.path.join(REPO, "mozart", "services", f"api_v{version}", "jobs.py")
    spec = importlib.util.spec_from_file_location(f"_jobs_{version}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    api = Api(flask_app)
    api.add_namespace(mod.job_ns, path="/job")
    return flask_app.test_client(), job_utils


@pytest.fixture(params=["01", "02"])
def client(request, monkeypatch):
    return _load(request.param, monkeypatch)


BASE = {"type": "job-x:v1", "queue": "q", "params": "{}"}


def test_submit_without_workflow(client):
    c, ju = client
    r = c.post("/job/submit", data=BASE)
    assert r.status_code == 200, r.data
    assert r.get_json()["result"] == "payload-id-1"
    assert ju.resolve_hysds_job.call_args.kwargs["workflow"] is None


def test_submit_with_workflow_form(client):
    c, ju = client
    r = c.post("/job/submit", data={**BASE, "workflow": json.dumps(WF)})
    assert r.status_code == 200, r.data
    assert ju.resolve_hysds_job.call_args.kwargs["workflow"] == WF


def test_submit_with_workflow_query_string(client):
    c, ju = client
    r = c.post("/job/submit", query_string={**BASE, "workflow": json.dumps(WF)})
    assert r.status_code == 200, r.data
    assert ju.resolve_hysds_job.call_args.kwargs["workflow"] == WF


def test_submit_with_malformed_workflow(client):
    c, ju = client
    r = c.post("/job/submit", data={**BASE, "workflow": "{nope"})
    assert r.status_code == 500
    assert "workflow" in r.get_json()["message"]
    ju.submit_hysds_job.assert_not_called()
