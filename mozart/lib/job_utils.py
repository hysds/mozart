from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from mozart import app, mozart_es


def get_job_status(_id):
    """
    Get the status of a job with the given identity
    @param _id - id of the job
    """
    if _id is None:
        raise Exception("'id' must be supplied by request")

    es_index = "job_status-current"
    status = mozart_es.get_by_id(es_index, id=_id, _source_includes=['status'], ignore=404)
    if status['found'] is False:
        raise Exception("job _id not found")

    return status['_source']['status']


def get_job_list():
    """Get a listing of jobs"""
    query = {
        "query": {
            "match_all": {}
        }
    }
    es_index = "job_status-current"
    results = mozart_es.query(es_index=es_index, body=query, _source=False)
    return sorted([result["_id"] for result in results])


def get_job_info(_id):
    """
    Get the full info of a job with the given identity
    @param _id - id of the job
    """
    if _id is None:
        raise Exception("'id' must be supplied by request")

    es_index = "job_status-current"
    result = mozart_es.get_by_id(es_index, id=_id, ignore=404)
    if result['found'] is False:
        raise Exception('job _id not found: %s' % _id)

    return result['_source']


def get_execute_nodes():
    """Return the names of all execute nodes."""

    # TODO: facets have been deprecated: https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-facets.html
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
    index = app.config['JOB_STATUS_INDEX']
    result = mozart_es.search(index, body=query)

    nodes = []
    for terms in result['facets']['job.job_info.execute_node']['terms']:
        nodes.append(terms['term'])
    nodes.sort()
    return nodes
