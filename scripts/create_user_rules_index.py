#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()

import os
import json

from hysds.es_util import get_mozart_es
from mozart import app

mozart_es = get_mozart_es()
USER_RULES_INDEX = app.config["USER_RULES_INDEX"]

body = {}

# get settings
path = os.path.join(app.root_path, "..", "configs", "es_settings.json")
with open(path) as f:
    settings_object = json.load(f)
    body = {**body, **settings_object}

# get doc type mapping
path = os.path.join(app.root_path, "..", "configs", "user_rules_job.mapping")
with open(path) as f:
    user_rules_mapping = json.load(f)
    body = {**body, **user_rules_mapping}

# create destination index
mozart_es.es.indices.create(USER_RULES_INDEX, body, ignore=400)
