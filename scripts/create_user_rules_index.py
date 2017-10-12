#!/usr/bin/env python
import os, json, requests, sys
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
