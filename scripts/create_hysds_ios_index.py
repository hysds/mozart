#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()

import os
import json

from hysds.es_util import get_mozart_es
from mozart import app

mozart_es = get_mozart_es()
HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']

body = {}

# get settings
path = os.path.join(app.root_path, '..', 'configs', 'es_settings.json')
with open(path) as f:
    settings_object = json.load(f)
    body = {**body, **settings_object}

# get doc type mapping
path = os.path.join(app.root_path, '..', 'configs', 'hysds_ios.mapping')
with open(path) as f:
    user_rules_mapping = json.load(f)
    body = {**body, **user_rules_mapping}

# create destination index
mozart_es.es.indices.create(HYSDS_IOS_INDEX, body, ignore=400)
