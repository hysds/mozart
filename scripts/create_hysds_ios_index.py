#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()

import os
import json

from hysds.es_util import get_mozart_es
from mozart import app, get_package_path

mozart_es = get_mozart_es()
HYSDS_IOS_INDEX = app.config["HYSDS_IOS_INDEX"]

body = {}

# get settings
path = get_package_path("configs", "es_settings.json")
with open(path) as f:
    settings_object = json.load(f)
    body = {**body, **settings_object}

# get doc type mapping
path = get_package_path("configs", "hysds_ios.mapping")
with open(path) as f:
    user_rules_mapping = json.load(f)
    body = {**body, **user_rules_mapping}

# create destination index
mozart_es.es.indices.create(HYSDS_IOS_INDEX, body, ignore=400)
