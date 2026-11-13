"""derive_fields of scripts/backfill_workflow_status.py (the pure part of the backfill)."""
import importlib.util
import os

spec = importlib.util.spec_from_file_location(
    "backfill_workflow_status",
    os.path.join(os.path.dirname(__file__), "..", "scripts", "backfill_workflow_status.py"))
bf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bf)

SPEC = {"nodes": {"A": {"kind": "job"}, "S": {"kind": "scatter"}}}


def doc(**kw):
    d = {"spec_id": "demo:1.0.0", "status": "failed", "submitted_at": "2026-11-13T10:00:00.000Z",
         "ended_at": "2026-11-13T10:01:30.500Z",
         "nodes": {"A": {"state": "completed"}, "S": {"state": "failed"}, "B": {"state": "failed"}}}
    d.update(kw)
    return d


def test_derives_everything_for_a_failed_run():
    assert bf.derive_fields(doc(), SPEC) == {
        "spec_name": "demo", "spec_version": "1.0.0", "has_scatter": True, "duration_s": 90.5, "failed_node": "S"}


def test_spec_name_splits_on_last_colon():
    out = bf.derive_fields(doc(spec_id="a:b:2"), None)
    assert (out["spec_name"], out["spec_version"]) == ("a:b", "2") and "has_scatter" not in out


def test_running_run_gets_no_duration_or_failed_node():
    out = bf.derive_fields(doc(status="running", ended_at=None), {"nodes": {"A": {"kind": "job"}}})
    assert out == {"spec_name": "demo", "spec_version": "1.0.0", "has_scatter": False}


def test_idempotent_never_overwrites():
    d = doc(spec_name="x", spec_version="9", has_scatter=False, duration_s=1.0, failed_node="A")
    assert bf.derive_fields(d, SPEC) == {}


def test_failed_run_without_a_failed_node_state():
    d = doc(nodes={"A": {"state": "cancelled"}})
    assert "failed_node" not in bf.derive_fields(d, SPEC)
