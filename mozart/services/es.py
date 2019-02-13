from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import os
import json
import requests
import types
import re
from flask import jsonify, Blueprint, request, Response, render_template, make_response
from pprint import pformat
from dateutil.parser import parse

from mozart import app


mod = Blueprint('services/es', __name__)


@mod.route('/query/<index>', methods=['GET'])
def query(index=None):
    """Query interface for FacetView."""

    # get callback, source, and index
    callback = request.args.get('callback')
    source = request.args.get('source')
    if index is None:
        return jsonify({
            'success': False,
            'message': "Cannot recognize index: %s" % index,
        }), 500

    # query
    es_url = app.config['ES_URL']
    #app.logger.debug("ES query for query(): %s" % source)
    r = requests.post('%s/%s/_search' % (es_url, index), data=source)
    result = r.json()
    #app.logger.debug("result: %s" % pformat(r.json()))

    # convert dates to PST
    for hit in result['hits']['hits']:
        hit['fields'] = hit['_source']
        hit['fields']['_id'] = hit['_id']
        hit['fields']['_type'] = hit['_type']
        if "type" in hit['fields'] and hit['fields']['type'] == "job":
            job_info = hit['fields'].get('job', {}).get('job_info', {})
            job_info['time_queued'] = parse(job_info['time_queued']).isoformat(
                ' ').split('.')[0] if 'time_queued' in job_info else ''
            job_info['time_start'] = parse(job_info['time_start']).isoformat(
                ' ').split('.')[0] if 'time_start' in job_info else ''
            job_info['time_end'] = parse(job_info['time_end']).isoformat(
                ' ').split('.')[0] if 'time_end' in job_info else ''
        hit['fields']['@timestamp'] = parse(hit['fields']['@timestamp']).isoformat(
            ' ').split('.')[0] if '@timestamp' in hit['fields'] else ''

    # return JSONP
    return Response('%s(%s)' % (callback, json.dumps(result)),
                    mimetype="application/javascript")


@mod.route('/job_info', methods=['GET'])
def get_job_info():
    """Return job info json."""

    # get callback, source, and index
    job_id = request.args.get('id', None)
    if job_id is None:
        return jsonify({
            'success': False,
            'message': "Job ID was not specified."
        }), 500

    # query
    q = json.dumps({"query": {"term": {"_id": job_id}}})
    es_url = app.config['ES_URL']
    index = app.config['JOB_STATUS_INDEX']
    r = requests.post('%s/%s/_search' % (es_url, index), data=q)
    result = r.json()
    #app.logger.debug("result: %s" % pformat(r.json()))
    hit = result['hits']['hits'][0]['_source'] if len(
        result['hits']['hits']) > 0 else None
    #app.logger.debug("hit: %s" % pformat(hit))

    if hit:
        # build job info
        #app.logger.debug("hit: %s" % json.dumps(hit, indent=2))
        job_info = hit['job'].get('job_info', {})
        if 'facts' in job_info:
            del(job_info['facts'])
        job_url = job_info.get('job_url', None)
        ret = {
            'job_info': json.dumps(job_info, indent=2, sort_keys=True),
            'job_url': job_url,
            'traceback': hit.get('traceback', 'No traceback provided.')
        }
        return jsonify({
            'success': True,
            'message': "",
            'result': ret
        })
    else:
        return jsonify({
            'success': False,
            'message': "No results found.",
            'result': None
        })
