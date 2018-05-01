import os, json, time, re, requests, traceback, cgi
from flask import jsonify, Blueprint, request, Response
from flask_login import login_required
from datetime import datetime, timedelta
from dateutil.parser import parse
from pprint import pprint, pformat
from urlparse import urlparse

from hysds.celery import app as celapp
from mozart import app


mod = Blueprint('services/jobs', __name__)

# job name regex
JOB_NAME_RE = re.compile(r'^(.+)-\d{8}T\d{6}.*$')


@mod.route('/job_count')
def job_count():
    """Return total number of jobs and counts by status."""

    query = {
        "size": 0,
        "facets": {
            "status": {
                "terms": {
                    "field": "status"
                }
            }
        }
    }
    es_url = app.config['ES_URL']
    index = app.config['JOB_STATUS_INDEX']
    r = requests.post('%s/%s/job/_search' % (es_url, index), data=json.dumps(query))
    r.raise_for_status()
    result = r.json()
    counts = { 'total': result['facets']['status']['total'] }
    for terms in result['facets']['status']['terms']:
        counts[terms['term']] = terms['count']

    return jsonify({
        'success': True,
        'message': '',
        'counts': counts
    })


@mod.route('/get_text')
def get_text():
    """ Return text content for a job file."""

    # check file param
    url_file = request.args.get('file', None)
    if url_file == None:
        return jsonify({
            'success': False,
            'content': 'No job file specified'
        })

    # read contents
    try: r = requests.get(url_file, verify=False)
    except Exception, e:
        return jsonify({
            'success': False,
            'content': "%s\n%s" % (str(e), traceback.format_exc())
        })

    # check if 404
    success = True
    if r.status_code == 404:
        success = False
        content = '<font color="red"><b>Got 404 trying to access:<br/><br/>%s</b><br/><br/>' % url_file
        content += 'Check that WebDAV has been configured on worker node.<br/><br/>'
        content += "If WebDAV is up and running on the worker node, then this job's<br/>"
        content += 'work directory may have been cleaned out by verdi to free up<br/>'
        content += 'disk space to run new jobs.'
    else: content = cgi.escape(r.text)
 
    return jsonify({
        'success': success,
        'content': content
    })


@mod.route('/task/stop/<index>', methods=['GET'])
@login_required
def stop_running(index=None):
    ''' Stops tasks '''
    return purge(index, False)

@mod.route('/task/purge/<index>', methods=['GET'])
@login_required
def purge_complete(index=None):
    ''' Purges non-active tasks '''
    return purge(index,True)

def purge(index,purge):
    """Purge job."""

    # get callback, source, and job status index
    source = request.args.get('source')
    if index is None:
        return jsonify({
            'success': False,
            'message': "Cannot recognize index: %s" % index,
        }), 500
    elif source is None or source is "":
        return jsonify({
            'success': False,
            'message': "Unbounded job purge",
        }), 500
    # query
    es_url = app.config['ES_URL']
    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index), data=source)
    if r.status_code != 200:
        app.logger.debug("Failed to query ES. Got status code %d:\n%s" %
                         (r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    #app.logger.debug("result: %s" % pformat(r.json()))

    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']

    # get list of results
    results = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            results.append(hit['_source'])

    # stream purge output
    def stream_purge(results, es_url, index):
        # purge job from index and delete work dir via DAV
        yield 'Starting...\n'
        for res in results:
            uuid = res['uuid']
            payload_id = res['payload_id']
            doctype = res['type']
            #Always grab latest state (not state from query result)
            task = celapp.AsyncResult(uuid)
            state = task.state
            #Active states may only revoke
            yield "Job state: %s\n" % state
            if state in ["RETRY","STARTED"] or (state == "PENDING" and not purge):
                if not purge:
                    yield 'Revoking %s\n' % (uuid)
                    celapp.control.revoke(uuid,terminate=True)
                else:
                    yield 'Cannot remove active job %s\n' % (uuid)
                continue
            elif not purge:
                yield 'Cannot stop inactive job: %s\n' % (uuid)
                continue
            #Saftey net to revoke job if in PENDING state
            if state == "PENDING":
                yield 'Revoking %s\n' % (uuid)
                celapp.control.revoke(uuid,terminate=True)
            #Inactive states remove from ES, WebDav etc.
            if doctype == "job":
                url = res.get('job', {}).get('job_info', {}).get('job_url', None)
                yield 'Purging %s (%s)...' % (uuid, url)
                #Delete from WebDAV
                if not url is None:
                    yield 'Removing WebDAV directory...'
                    try:
                        r = requests.delete(url, timeout=5)
                        yield 'done.\n'
                    except Exception, e:
                        yield 'failed (%s).\n' % str(e)
            # Both associated task and job from ES
            yield 'Removing ES for %s:%s' % (doctype,payload_id)
            r = requests.delete("%s/%s/%s/_query?q=_id:%s" % (es_url, index, doctype, payload_id))
            r.raise_for_status()
            res = r.json()
            yield 'done.\n'
        yield 'Finished\n'

    return Response(stream_purge(results, es_url, index), mimetype="text/plain")
