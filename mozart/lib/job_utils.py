from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import json
import requests

import hysds_commons.request_utils

from mozart import app


def get_job_status(_id):
    """
    Get the status of a job with the given identity
    @param _id - id of the job
    """
    if _id is None:
        raise Exception("'id' must be supplied by request")

    es_url = app.config['ES_URL']
    es_index = "job_status-current"
    full_url = "{0}/{1}/job/{2}?_source=status".format(es_url, es_index, _id)
    return hysds_commons.request_utils.get_requests_json_response(full_url)["_source"]["status"]


def get_job_list():
    """Get a listing of jobs"""

    data = {"query": {"match_all": {}}, "fields": []}
    es_url = app.config['ES_URL']
    es_index = "job_status-current"
    full_url = "{0}/{1}/_search".format(es_url, es_index)
    results = hysds_commons.request_utils.post_scrolled_json_responses(full_url, "{0}/_search".format(es_url),
                                                                       data=json.dumps(data))
    return sorted([result["_id"] for result in results])


def get_job_info(ident):
    """
    Get the full info of a job with the given identity
    @param ident - id of the job
    """
    if ident is None:
        raise Exception("'id' must be supplied by request")

    es_url = app.config['ES_URL']
    es_index = "job_status-current"
    full_url = "{0}/{1}/job/{2}".format(es_url, es_index, ident)
    return hysds_commons.request_utils.get_requests_json_response(full_url)["_source"]


def get_execute_nodes():
    """Return the names of all execute nodes."""

    query = {
        "size": 0,
        "facets": {
            "job.job_info.execute_node": {
                "terms": {
                    "field": "job.job_info.execute_node",
                    "all_terms": True
                }
            }
        }
    }
    es_url = app.config['ES_URL']
    index = app.config['JOB_STATUS_INDEX']
    r = requests.post('%s/%s/_search' % (es_url, index), data=json.dumps(query))
    r.raise_for_status()
    result = r.json()
    # app.logger.debug(pformat(result))
    total = len(result['facets']['job.job_info.execute_node']['terms'])
    nodes = []
    for terms in result['facets']['job.job_info.execute_node']['terms']:
        nodes.append(terms['term'])
    nodes.sort()
    return nodes
