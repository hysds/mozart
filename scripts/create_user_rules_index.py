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
import requests
import sys
from elasticsearch import Elasticsearch

from mozart import app


# get destination index and doctype
dest = app.config['USER_RULES_INDEX']
doctype = '.percolator'

# get settings
body = {}
path = os.path.join(app.root_path, '..', 'configs', 'es_settings.json')
with open(path) as f:
    body.update(json.load(f))

# get doctype mapping
path = os.path.join(app.root_path, '..', 'configs', 'user_rules_job.mapping')
with open(path) as f:
    body.update(json.load(f))

# get connection and create destination index
es_url = app.config['ES_URL']
es = Elasticsearch(hosts=[es_url])
es.indices.create(dest, body, ignore=400)
