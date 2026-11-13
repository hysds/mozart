#!/usr/bin/env python
"""Create the workflow-orchestration indices (workflow_specs, workflow_status).

Modeled on create_user_rules_index.py / create_hysds_ios_index.py: merge
configs/es_settings.json with the per-index mapping and create the index.

Both mappings deliberately declare `nodes`, `inputs`, `outputs` and `defaults`
as {"type": "object", "enabled": false}. Their keys are author-chosen node ids
and input names; leaving them dynamically mapped would let two unrelated
workflows collide on a field type, or walk the index into the default
1000-field limit -- the same hazard ADR-003 rejects for dataset metadata. Both
docs are fetched by _id and read whole, so nothing needs to query into them.
"""
from future import standard_library

standard_library.install_aliases()

import os
import json

from hysds.es_util import get_mozart_es
from mozart import app

mozart_es = get_mozart_es()

INDICES = (
    (app.config["WORKFLOW_SPECS_INDEX"], "workflow_specs.mapping"),
    (app.config["WORKFLOW_STATUS_INDEX"], "workflow_status.mapping"),
)

# get settings shared by every mozart-created index
path = os.path.join(app.root_path, "..", "configs", "es_settings.json")
with open(path) as f:
    settings_object = json.load(f)

for index, mapping_file in INDICES:
    body = {**settings_object}

    # get doc type mapping
    path = os.path.join(app.root_path, "..", "configs", mapping_file)
    with open(path) as f:
        body = {**body, **json.load(f)}

    # create destination index
    r = mozart_es.es.indices.create(index=index, body=body, ignore=400)
    if isinstance(r, dict) and r.get("status") == 400:
        # the index already exists: add any new mapping fields (additive; OpenSearch rejects a
        # change to an existing field's type, which leaves the index as it was)
        try:
            mozart_es.es.indices.put_mapping(index=index, body=body["mappings"])
        except Exception as e:  # noqa: BLE001
            print("WARNING: could not add mapping fields to %s: %s" % (index, e))
    # up.sh greps for "success": keep it in both cases
    print("Successfully created index %s" % index)
