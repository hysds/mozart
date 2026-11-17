"""mozart without the optional ``workflows`` extra (``hysds_commons[cwl]``: cwltool, cwl-utils).

Each check runs in a fresh interpreter with ``sys.modules['cwltool'] = sys.modules['cwl_utils'] = None``,
which makes any ``import cwltool`` raise ImportError exactly as on an install without the extra. The
full mozart Flask app is created (settings, celeryconfig and the OpenSearch client are stubbed), a
non-workflow endpoint must answer, the CWL authoring endpoints must answer 501 naming the extra, and a
single startup warning must be logged.
"""
import json
import os
import subprocess
import sys
import textwrap

REPO = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# the celeryconfig keys hysds reads at import time (values are never used: no broker, no OpenSearch)
CELERYCONFIG = textwrap.dedent("""
    JOBS_ES_ENGINE = "elasticsearch"
    JOBS_ES_URL = "http://127.0.0.1:1"
    WORKER_MOUNT_BLACKLIST = []
    WORKER_CONTIGUOUS_FAILURE_THRESHOLD = 10
    WORKER_CONTIGUOUS_FAILURE_TIME = 5.0
    USER_RULES_JOB_INDEX = "user_rules-mozart"
    USER_RULES_JOB_QUEUE = "user_rules_job"
    USER_RULES_TRIGGER_QUEUE = "user_rules_trigger"
    JOBS_PROCESSED_QUEUE = "jobs_processed"
    PROCESS_EVENTS_TASKS_QUEUE = "x"
    ON_DEMAND_JOB_QUEUE = "x"
    ON_DEMAND_DATASET_QUEUE = "x"
    DATASET_PROCESSED_QUEUE = "x"
    USER_RULES_DATASET_QUEUE = "x"
    USER_RULES_DATASET_INDEX = "x"
    DATASET_ALIAS = "x"
    GRQ_ES_URL = "x"
""")

PRELUDE = """
import sys
sys.modules["cwltool"] = None
sys.modules["cwl_utils"] = None
"""


def _run(tmp_path, body):
    (tmp_path / "celeryconfig.py").write_text(CELERYCONFIG)
    script = PRELUDE + textwrap.dedent(body)
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), REPO, os.environ.get("PYTHONPATH", "")]))
    p = subprocess.run([sys.executable, "-c", script], cwd=str(tmp_path), env=env, capture_output=True,
                       text=True, timeout=120)
    assert p.returncode == 0, p.stdout + p.stderr
    return p


def test_workflow_api_imports_without_cwl_extra(tmp_path):
    # the original failure: mozart.workflow_api -> hysds_commons.workflow_refs2 -> hysds_commons.cwl
    p = _run(tmp_path, """
        import importlib.util, flask
        from unittest import mock
        sys.modules["mozart"] = mock.MagicMock(app=flask.Flask("t"))
        s = importlib.util.spec_from_file_location("mozart.workflow_api", %r)
        w = importlib.util.module_from_spec(s)
        sys.modules["mozart.workflow_api"] = w
        s.loader.exec_module(w)
        assert w.cwl_available() is False
        try:
            import hysds_commons.cwl
        except ImportError as e:
            assert "hysds_commons[cwl]" in str(e)
        else:
            raise AssertionError("hysds_commons.cwl imported without cwltool")
        print("OK")
    """ % os.path.join(REPO, "mozart", "workflow_api.py"))
    assert "OK" in p.stdout


def test_app_starts_and_serves_non_workflow_endpoints(tmp_path):
    p = _run(tmp_path, """
        import json, logging
        from unittest import mock

        import flask
        # settings.cfg is rendered at deploy time; give the app a minimal one instead
        flask.Config.from_pyfile = lambda self, filename, silent=False: self.from_mapping(
            JOB_SPECS_INDEX="job_specs", CONTAINERS_INDEX="containers", HYSDS_IOS_INDEX="hysds_ios",
            JOB_STATUS_INDEX="job_status-current", USER_RULES_INDEX="user_rules-mozart",
            WORKFLOW_SPECS_INDEX="workflow_specs", WORKFLOW_STATUS_INDEX="workflow_status",
            SECRET_KEY="x") or True
        import hysds.es_util
        es = mock.MagicMock()
        es.query.return_value = [{"_id": "job-a:v1"}]
        hysds.es_util.get_mozart_es = lambda *a, **k: es

        records = []
        h = logging.Handler(); h.emit = records.append
        logging.getLogger("gunicorn.error").addHandler(h)
        logging.getLogger("gunicorn.error").setLevel(logging.INFO)

        import mozart
        out = {"startup_warnings": [rec.getMessage() for rec in records if rec.levelno == logging.WARNING]}
        c = mozart.app.test_client()
        for v in ("0.1", "0.2"):
            r = c.get("/api/v%s/job_spec/list" % v)
            out["job_spec_list_" + v] = [r.status_code, r.get_json()]
            r = c.post("/api/v%s/workflow/cwl/validate" % v, data="cwlVersion: v1.2\\nclass: Workflow\\n",
                       content_type="text/plain")
            out["cwl_validate_" + v] = [r.status_code, r.get_json()]
            r = c.get("/api/v%s/workflow/cwl/operation/job-a:v1" % v)
            out["cwl_operation_" + v] = [r.status_code]
        print("RESULT " + json.dumps(out))
    """)
    line = [x for x in p.stdout.splitlines() if x.startswith("RESULT ")][-1]
    out = json.loads(line[len("RESULT "):])
    for v in ("0.1", "0.2"):
        code, body = out["job_spec_list_" + v]
        assert code == 200 and body["result"] == ["job-a:v1"], out
        code, body = out["cwl_validate_" + v]
        assert code == 501, out
        assert body["success"] is False and "hysds_commons[cwl]" in body["message"] and "workflows" in body["message"]
    for v in ("0.1", "0.2"):
        assert out["cwl_operation_" + v][0] in (404, 501), out  # job type lookup is mocked; never a 500
    # exactly one startup warning (the per-request 501 warnings are logged later, on use)
    assert out["startup_warnings"] == [out["startup_warnings"][0]], out
    assert "not installed" in out["startup_warnings"][0]
