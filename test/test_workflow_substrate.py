"""Structural validation of the workflow-orchestration substrate.

Schema/data only -- nothing in mozart reads or writes any of this yet. These
tests assert the new index mappings and connector job_specs parse and are
structurally valid; they deliberately never touch a cluster.

Covers:
- ``configs/workflow_specs.mapping`` and ``configs/workflow_status.mapping``,
  created by ``scripts/create_workflow_indices.py``.
- (The ``job_status`` workflow mappings live in sdscli's ``job_status.template``
  and are tested there.)
- ``configs/workflow_connectors/`` -- one shared container plus a job_spec per
  connector primitive.
"""

import glob
import json
import os

import pytest

from test_configs import VALID_FIELD_TYPES, _iter_properties

WORKFLOW_MAPPINGS = ("workflow_specs.mapping", "workflow_status.mapping")

CONNECTORS = ("pass", "scatter", "gather", "gate", "switch", "halt")

SHARED_CONTAINER = "hysds/workflow-connectors:v1.0.0"

@pytest.fixture
def connectors_dir(configs_dir):
    return os.path.join(configs_dir, "workflow_connectors")


@pytest.fixture
def load_job_spec(connectors_dir):
    def _load(connector):
        path = os.path.join(connectors_dir, f"job-spec.json.workflow-{connector}")
        with open(path) as f:
            return json.load(f)

    return _load


# --- index mappings ----------------------------------------------------------


@pytest.mark.parametrize("doc", WORKFLOW_MAPPINGS)
def test_workflow_mapping_field_types_are_known(load_config, doc):
    mapping = load_config(doc)["mappings"]["properties"]
    assert mapping
    for name, definition in _iter_properties(mapping):
        if "properties" in definition and "type" not in definition:
            continue
        assert "type" in definition, f"{doc}: field {name} declares no type"
        assert definition["type"] in VALID_FIELD_TYPES, (
            f"{doc}: field {name} has unknown type {definition['type']}"
        )


def test_workflow_specs_mapping_keys_the_spec(load_config):
    props = load_config("workflow_specs.mapping")["mappings"]["properties"]
    for field in ("id", "version", "schema_version", "entry"):
        assert props[field]["type"] == "keyword"
    assert props["creation_time"]["type"] == "date"


def test_workflow_status_mapping_is_filterable_by_run(load_config):
    props = load_config("workflow_status.mapping")["mappings"]["properties"]
    for field in ("run_id", "spec_id", "status", "username", "current_nodes"):
        assert props[field]["type"] == "keyword", f"{field} must be term-filterable"
    for field in ("submitted_at", "updated_at", "ended_at"):
        assert props[field]["type"] == "date"


@pytest.mark.parametrize("doc", WORKFLOW_MAPPINGS)
def test_author_keyed_subdocuments_are_index_disabled(load_config, doc):
    """``nodes`` / ``inputs`` / ``outputs`` keys are author-chosen.

    Leaving them dynamically mapped is exactly the mapping-explosion and
    cross-workflow type-conflict hazard ADR-003 rejects for dataset metadata.
    Both docs are fetched by ``_id`` and read whole, so nothing queries into
    them.
    """
    props = load_config(doc)["mappings"]["properties"]
    for field in ("inputs", "nodes", "outputs"):
        assert props[field] == {"type": "object", "enabled": False}, (
            f"{doc}: {field} must be index-disabled"
        )


# job_status additions (workflow tags, nested workflow_outputs) live in sdscli:
# sdscli/test/test_job_status_template.py


def test_es_template_string_rule_is_why_the_declaration_is_needed(load_config):
    """Guard the premise: the catch-all is still there and still maps to text."""
    template = json.loads(
        open(
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                "configs",
                "es_template.json",
            )
        )
        .read()
        .replace("{{ index }}", "job_specs")
    )
    strings = template["mappings"]["dynamic_templates"][0]["strings"]
    assert strings["match"] == "*"
    assert strings["mapping"]["type"] == "text"
    assert "job_info" not in template["mappings"]["properties"]


# --- connector job_specs -----------------------------------------------------


def test_every_connector_primitive_has_a_job_spec(connectors_dir):
    found = {
        os.path.basename(p).rsplit("workflow-", 1)[1]
        for p in glob.glob(os.path.join(connectors_dir, "job-spec.json.workflow-*"))
    }
    assert found == set(CONNECTORS)


@pytest.mark.parametrize("connector", CONNECTORS)
def test_connector_job_spec_is_structurally_valid(load_job_spec, connector):
    spec = load_job_spec(connector)

    assert spec["id"] == f"job-workflow-{connector}:{spec['job-version']}"
    assert spec["command"].endswith(f"run_{connector}.sh")

    # required by hysds_commons/schemas/job-spec-schema.json
    for field in ("command", "params", "disk_usage", "soft_time_limit", "time_limit"):
        assert field in spec, f"{connector}: missing required job-spec field {field}"
    assert spec["disk_usage"].endswith(("GB", "MB", "KB"))
    assert isinstance(spec["soft_time_limit"], int)
    assert spec["soft_time_limit"] < spec["time_limit"]

    for param in spec["params"]:
        assert param["destination"] in ("positional", "localize", "context")

    # connectors are dispatched off the event; dedup would swallow re-entries
    assert spec["enable_dedup"] is False


@pytest.mark.parametrize("connector", CONNECTORS)
def test_connectors_share_one_container_image(load_job_spec, connector):
    assert load_job_spec(connector)["container"] == SHARED_CONTAINER


@pytest.mark.parametrize("connector", CONNECTORS)
def test_connector_has_its_own_queue(load_job_spec, connector):
    """Per-connector Celery queues, so one primitive cannot starve another."""
    assert load_job_spec(connector)["recommended-queues"] == [
        f"workflow-connector-{connector}"
    ]


def test_shared_container_doc(connectors_dir):
    path = os.path.join(connectors_dir, "container.json.workflow-connectors")
    with open(path) as f:
        container = json.load(f)
    assert container["id"] == f"{container['name']}:{container['version']}"
    assert container["name"] == "container-workflow-connectors"
    assert container["url"].startswith("s3://")


def test_connector_job_specs_validate_against_the_hysds_commons_schema(
    connectors_dir,
):
    """Optional: runs wherever hysds_commons' job-spec schema can be located.

    Either from an installed hysds_commons, or from a sibling checkout -- the
    layout in a hysds-core workspace.
    """
    jsonschema = pytest.importorskip("jsonschema")

    candidates = []
    try:
        import hysds_commons

        candidates.append(
            os.path.join(
                os.path.dirname(os.path.realpath(hysds_commons.__file__)),
                "..",
                "schemas",
                "job-spec-schema.json",
            )
        )
    except ImportError:
        pass
    candidates.append(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))),
            "hysds_commons",
            "schemas",
            "job-spec-schema.json",
        )
    )

    schema_file = next((p for p in candidates if os.path.exists(p)), None)
    if schema_file is None:
        pytest.skip("hysds_commons job-spec-schema.json not found")

    with open(schema_file) as f:
        schema = json.load(f)
    validator = jsonschema.Draft7Validator(schema)

    for connector in CONNECTORS:
        path = os.path.join(connectors_dir, f"job-spec.json.workflow-{connector}")
        with open(path) as f:
            errors = sorted(validator.iter_errors(json.load(f)), key=lambda e: e.path)
        assert errors == [], f"{connector}: {[e.message for e in errors]}"
