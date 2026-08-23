"""Shared pytest fixtures for the mozart test suite.

Everything here must run on a laptop with no cluster, no broker and no
network. Fixtures deliberately avoid importing the ``mozart`` Flask
application, which requires a rendered ``settings/settings.cfg`` and live
Elasticsearch/RabbitMQ endpoints at import time.

The workflow-orchestration fixtures (``workflow_spec``, ``workflow_status``,
``workflow_queue_message``, ``fake_redis``) implement the document shapes from
``hysds-wiki/features/hysds-workflow-orchestration.md`` ("Workflow
representation", "State model") and
``hysds-wiki/decisions/ADR-003-workflow-output-binding-job-scoped.md``.
"""

import json
import os

import pytest

REPO_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
CONFIGS_DIR = os.path.join(REPO_DIR, "configs")


@pytest.fixture(scope="session")
def repo_dir():
    """Absolute path to the mozart repo root."""
    return REPO_DIR


@pytest.fixture(scope="session")
def configs_dir():
    """Absolute path to ``mozart/configs``."""
    return CONFIGS_DIR


@pytest.fixture(scope="session")
def load_config():
    """Return a loader for a JSON document under ``mozart/configs``."""

    def _load(name):
        with open(os.path.join(CONFIGS_DIR, name)) as f:
            return json.load(f)

    return _load


@pytest.fixture
def workflow_spec():
    """An immutable ``workflow_spec`` document (index ``workflow_specs``).

    A trimmed version of the design doc's biomass map-reduce example: it
    exercises every node kind the connectors implement (job, pass, scatter,
    gather, gate, switch, halt) while staying small enough to assert on.
    """
    return {
        "id": "biomass-map-reduce",
        "version": "1.2.0",
        "schema_version": "1.0",
        "label": "Biomass map-reduce (GEDI L4A -> mosaic -> QA-gated publish)",
        "defaults": {
            "queue": "factotum-job_worker-small",
            "on_failure": "halt",
            "max_retries": 2,
            "timeout_hours": 48,
        },
        "inputs": {
            "collection": {"type": "string", "default": "GEDI_L4A_002"},
            "bbox": {"type": "float[]"},
            "datetime": {"type": "string"},
            "resolution": {"type": "int", "default": 30},
            "qa_threshold": {"type": "float", "default": 0.85},
        },
        "entry": "query_data",
        "nodes": {
            "query_data": {
                "kind": "job",
                "run": "stac-query:1.0.0",
                "in": {
                    "collection": "$.inputs.collection",
                    "bbox": "$.inputs.bbox",
                    "datetime": "$.inputs.datetime",
                },
                "out": ["items"],
                "next": ["pass_1"],
            },
            "pass_1": {
                "kind": "pass",
                "bind": {"process_granules.input_file": "$.query_data.items"},
                "next": ["scatter_1"],
            },
            "scatter_1": {
                "kind": "scatter",
                "child": "process_granules",
                "over": "$.query_data.items",
                "completion": {"policy": "m_of_n", "threshold": 0.9},
                "next": ["gather_1"],
            },
            "process_granules": {
                "kind": "job",
                "run": "biomass-estimation:2.1.0",
                "queue": "maap-dps-worker-large",
                "in": {
                    "input_file": "$.scatter.item",
                    "resolution": "$.inputs.resolution",
                },
                "out": ["biomass_tif"],
            },
            "gather_1": {
                "kind": "gather",
                "collect": "$.process_granules.biomass_tif",
                "next": ["validate"],
            },
            "validate": {
                "kind": "job",
                "run": "qa-check:1.0.0",
                "in": {"input_file": "$.gather_1.mosaic_tif"},
                "out": ["qa_score", "qa_report"],
                "next": ["gate_1"],
            },
            "gate_1": {
                "kind": "gate",
                "when": {
                    "left": "$.validate.qa_score",
                    "op": ">=",
                    "right": "$.inputs.qa_threshold",
                },
                "on_true": ["route_1"],
                "on_false": ["halt_1"],
            },
            "route_1": {
                "kind": "switch",
                "bind": {"level": "$.validate.qa_report"},
                "cases": [
                    {
                        "when": {"left": "$.level", "op": "==", "right": "L2"},
                        "next": ["publish"],
                    }
                ],
                "default": ["publish"],
            },
            "publish": {
                "kind": "job",
                "run": "stac-publish:1.0.0",
                "in": {"item": "$.validate.qa_report"},
                "out": ["published_item"],
                "next": [],
            },
            "halt_1": {"kind": "halt", "next": []},
        },
        "outputs": {"published_item": "$.publish.published_item"},
    }


@pytest.fixture
def workflow_status():
    """A mutable per-run ``workflow_status`` document (index ``workflow_status``).

    One coarse doc per run, written only at step boundaries under optimistic
    concurrency. Per-scatter-child detail deliberately lives in ``job_status``
    (by the ``workflow.*`` tag), never here.
    """
    return {
        "run_id": "run-abc123",
        "spec_id": "biomass-map-reduce:1.2.0",
        "schema_version": "1.0",
        "status": "running",
        "username": "jdoe",
        "submitted_at": "2026-08-21T10:00:00.000Z",
        "updated_at": "2026-08-21T10:04:00.000Z",
        "inputs": {
            "collection": "GEDI_L4A_002",
            "bbox": [-120.0, 34.0, -119.0, 35.0],
            "datetime": "2026-01-01/2026-02-01",
            "resolution": 30,
            "qa_threshold": 0.85,
        },
        "nodes": {
            "query_data": {
                "state": "completed",
                "job_id": "stac-query-20260821T100000-0001",
                "started_at": "2026-08-21T10:00:05.000Z",
                "ended_at": "2026-08-21T10:01:00.000Z",
                "outputs": [
                    {
                        "name": "items",
                        "value_s": ["granule_a.h5", "granule_b.h5", "granule_c.h5"],
                    }
                ],
            },
            "pass_1": {"state": "completed"},
            "scatter_1": {"state": "running", "expected": 3},
            "process_granules": {"state": "running"},
            "gather_1": {"state": "pending"},
            "validate": {"state": "pending"},
            "gate_1": {"state": "pending"},
            "route_1": {"state": "pending"},
            "publish": {"state": "pending"},
            "halt_1": {"state": "pending"},
        },
        "outputs": {},
        "error": None,
    }


@pytest.fixture
def workflow_queue_message():
    """A synthetic ``WORKFLOW_QUEUE`` message.

    Emitted from ``log_job_status`` (the D2 single emit point). The ``outputs``
    key carries the ADR-003 typed key/value bindings straight off the event, so
    a connector resolves ``$.<node>.<output>`` with no index read.
    """
    return {
        "run_id": "run-abc123",
        "step_id": "process_granules",
        "child_index": 42,
        "job_id": "process_granules-20260821T100400-0042",
        "status": "job-completed",
        "short_error": None,
        "outputs": [
            {"name": "biomass_tif", "value_s": "s3://bucket/biomass_0042.tif"},
            {"name": "qa_score", "value_d": 0.93},
            {"name": "needs_reprocess", "value_b": False},
        ],
    }


@pytest.fixture
def fake_redis():
    """An in-memory Redis client for barrier tests.

    ``fakeredis`` supports the hash ops and the atomic Lua ``EVAL`` the
    scatter/gather barrier is built on, so barrier accounting can be tested
    without a Redis server.
    """
    fakeredis = pytest.importorskip(
        "fakeredis", reason="fakeredis is required for barrier tests"
    )
    server = fakeredis.FakeServer()
    client = fakeredis.FakeStrictRedis(server=server, decode_responses=True)
    try:
        yield client
    finally:
        client.flushall()
