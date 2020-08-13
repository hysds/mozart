from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import requests
import json
from pymongo import MongoClient
from pprint import pformat
import hysds_commons.request_utils

from mozart import app


def get_job_status(ident):
    '''
    Get the status of a job with the given identity
    @param ident - id of the job
    '''
    if ident is None:
        raise Exception("'id' must be supplied by request")
    # query
    es_url = app.config['ES_URL']
    es_index = "job_status-current"
    full_url = "{0}/{1}/job/{2}?_source=status".format(es_url, es_index, ident)
    return hysds_commons.request_utils.get_requests_json_response(full_url)["_source"]["status"]


def get_job_list(page_size=100, offset=0, username=None, detailed=False):
    '''
    Get a listing of jobs
    @param page_size - page size for pagination of jobs
    @param offset - starting offset for pagination
    '''
    # query
    if username is None:
        data = {"query": {"match_all": {}}, "fields": []}
    else:
        data = {"query": {"bool": {"must": [{"term": {"job.job.username": username}}]}}}

    es_url = app.config['ES_URL']
    es_index = "job_status-current"
    full_url = "{0}/{1}/_search".format(es_url, es_index)
    results = hysds_commons.request_utils.post_scrolled_json_responses(
        full_url, "{0}/_search".format(es_url), data=json.dumps(data))
    if detailed is False:
        return ([result["_id"] for result in results])
    else:
        return ([[result["_id"], result["_source"]["status"], result["_source"]["type"],
                  result["_source"]["job"]["params"]["job_specification"]["params"]] for result in results])


def get_job_info(ident):
    '''
    Get the full info of a job with the given identity
    @param ident - id of the job
    '''
    if ident is None:
        raise Exception("'id' must be supplied by request")
    # query
    es_url = app.config['ES_URL']
    es_index = "job_status-current"
    full_url = "{0}/{1}/job/{2}".format(es_url, es_index, ident)
    return hysds_commons.request_utils.get_requests_json_response(full_url)["_source"]

###
# Historic Job Util functions
#
###


def get_mongo_client():
    """Return MongoDB client."""

    return MongoClient('mongodb://localhost/')


def get_db():
    """Return mozart MongoDB database."""

    return get_mongo_client()['mozart']


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
    r = requests.post('%s/%s/_search' %
                      (es_url, index), data=json.dumps(query))
    r.raise_for_status()
    result = r.json()
    # app.logger.debug(pformat(result))
    total = len(result['facets']['job.job_info.execute_node']['terms'])
    nodes = []
    for terms in result['facets']['job.job_info.execute_node']['terms']:
        nodes.append(terms['term'])
    nodes.sort()
    return nodes
