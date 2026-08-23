"""Structural validation of the JSON configs mozart ships in ``configs/``.

These files are installed into Elasticsearch/OpenSearch by
``scripts/install_es_template.py``, ``scripts/create_user_rules_index.py`` and
``scripts/create_hysds_ios_index.py``. A typo in one of them is only ever
discovered at cluster-install time today; these tests catch it at commit time
without needing a cluster.
"""

import json
import os

import pytest
from jinja2 import Template

# every JSON document under configs/ that ships as part of the package
CONFIG_DOCS = (
    "es_settings.json",
    "hysds_ios.mapping",
    "user_rules_job.mapping",
    "orchestrator_events.json",
    "orchestrator_jobs.json",
    "orchestrator_jobs-high.json",
    "orchestrator_products.json",
    "orchestrator_puccini.json",
    "orchestrator_test_parpython.json",
    "actions_config.json.example",
)

# mapping documents keyed by file, and the field types they must declare
MAPPING_DOCS = ("hysds_ios.mapping", "user_rules_job.mapping")

VALID_FIELD_TYPES = {
    "binary",
    "boolean",
    "date",
    "double",
    "float",
    "integer",
    "ip",
    "keyword",
    "long",
    "nested",
    "object",
    "text",
}


def _iter_properties(properties):
    """Yield ``(dotted_name, definition)`` for every field in a mapping."""
    for name, definition in properties.items():
        yield name, definition
        for sub_name, sub_def in _iter_properties(definition.get("properties", {})):
            yield f"{name}.{sub_name}", sub_def


@pytest.mark.parametrize("doc", CONFIG_DOCS)
def test_config_doc_is_valid_json(configs_dir, doc):
    """Every shipped config document parses as JSON."""
    path = os.path.join(configs_dir, doc)
    with open(path) as f:
        assert isinstance(json.load(f), dict)


@pytest.mark.parametrize("doc", MAPPING_DOCS)
def test_mapping_field_types_are_known(load_config, doc):
    """Every explicitly declared field in a mapping uses a real ES/OS type."""
    mapping = load_config(doc)
    properties = mapping["mappings"]["properties"]
    assert properties, f"{doc} declares no properties"
    for name, definition in _iter_properties(properties):
        if "properties" in definition and "type" not in definition:
            continue  # implicit object
        assert "type" in definition, f"{doc}: field {name} declares no type"
        assert (
            definition["type"] in VALID_FIELD_TYPES
        ), f"{doc}: field {name} has unknown type {definition['type']}"


def test_es_settings_declares_default_analyzer(load_config):
    """``es_settings.json`` is merged into every mozart-created index."""
    settings = load_config("es_settings.json")
    analyzer = settings["settings"]["analysis"]["analyzer"]["default"]
    assert analyzer["tokenizer"] == "keyword"
    assert "lowercase" in analyzer["filter"]


def test_es_template_renders_and_is_structurally_valid(configs_dir):
    """``es_template.json`` is a jinja template rendered per index name.

    ``install_es_template.py`` renders it once per index in
    ``("containers", "job_specs", "hysds_ios")`` and PUTs the result, so the
    rendered form -- not the raw file -- has to be valid JSON.
    """
    path = os.path.join(configs_dir, "es_template.json")
    with open(path) as f:
        raw = f.read()

    assert "{{ index }}" in raw, "es_template.json is expected to be templated"

    for index in ("containers", "job_specs", "hysds_ios"):
        tmpl = json.loads(Template(raw).render(index=index))
        assert tmpl["template"] == index
        assert tmpl["settings"]["index.refresh_interval"] == "5s"

        mappings = tmpl["mappings"]
        # the catch-all dynamic string rule every other mapping decision in
        # this repo has to work around (see ADR-003)
        strings = mappings["dynamic_templates"][0]["strings"]
        assert strings["match_mapping_type"] == "string"
        assert strings["mapping"]["type"] == "text"
        assert strings["mapping"]["copy_to"] == "text_fields"

        properties = mappings["properties"]
        assert properties["_timestamp"]["type"] == "date"
        assert properties["text_fields"]["type"] == "text"


def test_orchestrator_configs_declare_job_creators(load_config):
    """Orchestrator configs map a job_type onto one or more job creators."""
    for doc in (
        "orchestrator_jobs.json",
        "orchestrator_jobs-high.json",
        "orchestrator_products.json",
        "orchestrator_puccini.json",
    ):
        config = load_config(doc)
        assert config["queues"], f"{doc} declares no queues"
        for entry in config["configs"]:
            assert entry["job_type"], f"{doc} has a config with no job_type"
            for creator in entry["job_creators"]:
                assert creator["job_name"]
                assert creator["function"]
                assert isinstance(creator["job_queues"], list)
