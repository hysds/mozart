from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
import re
import requests
import traceback
import cgi
from flask import jsonify, Blueprint, request, Response
from flask_login import login_required

from hysds.celery import app as celapp
from mozart import app, mozart_es


mod = Blueprint('services/jobs', __name__)

# job name regex
JOB_NAME_RE = re.compile(r'^(.+)-\d{8}T\d{6}.*$')


@mod.route('/job_count')
def job_count():
    """Return total number of jobs and counts by status."""

    body = {
        "size": 0,
        "aggs": {
            "result": {
                "terms": {
                    "field": "status"
                }
            }
        }
    }
    results = mozart_es.search(index="job_status-*", body=body)
    app.logger.info(json.dumps(results, indent=2))

    buckets = results['aggregations']['result']['buckets']

    total = 0
    counts = {}
    for bucket in buckets:
        counts[bucket['key']] = bucket['doc_count']
        total += bucket['doc_count']
    counts['total'] = total

    return jsonify({
        'success': True,
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
    try:
        r = requests.get(url_file, verify=False)
        r.raise_for_status()
    except Exception as e:
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
    else:
        content = cgi.escape(r.text)

    return jsonify({
        'success': success,
        'content': content
    })


@mod.route('/task/stop/<index>', methods=['GET'])
@login_required
def stop_running(index=None):
    """ Stops tasks """
    return purge(index, False)


@mod.route('/task/purge/<index>', methods=['GET'])
@login_required
def purge_complete(index=None):
    """ Purges non-active tasks """
    return purge(index, True)


def purge(es_index, purge):
    """Purge job."""

    # get callback, source, and job status index
    source = request.args.get('source')
    if es_index is None:
        return jsonify({
            'success': False,
            'message': "Cannot recognize index: %s" % es_index,
        }), 500
    if source is None or source is "":
        return jsonify({
            'success': False,
            'message': "Unbounded job purge",
        }), 500
    docs = mozart_es.query(es_index, body=source)

    # stream purge output
    def stream_purge(results, index):
        # purge job from index and delete work dir via DAV
        yield 'Starting...\n'
        for res in results:
            uuid = res['uuid']
            payload_id = res['payload_id']
            task = celapp.AsyncResult(uuid)  # Always grab latest state (not state from query result)
            state = task.state

            # Active states may only revoke
            yield "Job state: %s\n" % state
            if state in ["RETRY", "STARTED"] or (state == "PENDING" and not purge):
                if not purge:
                    yield 'Revoking %s\n' % (uuid)
                    celapp.control.revoke(uuid, terminate=True)
                else:
                    yield 'Cannot remove active job %s\n' % (uuid)
                continue
            elif not purge:
                yield 'Cannot stop inactive job: %s\n' % (uuid)
                continue

            # Safety net to revoke job if in PENDING state
            if state == "PENDING":
                yield 'Revoking %s\n' % (uuid)
                celapp.control.revoke(uuid, terminate=True)

            # Inactive states remove from ES, WebDav etc.
            if index.startswith('job_status'):
                url = res.get('job', {}).get('job_info', {}).get('job_url', None)
                yield 'Purging %s (%s)...' % (uuid, url)

                # Delete from WebDAV
                if not url is None:
                    yield 'Removing WebDAV directory...'
                    try:
                        r = requests.delete(url, timeout=5)
                        yield 'done.\n'
                    except Exception as e:
                        yield 'failed (%s).\n' % str(e)

            # Both associated task and job from ES
            yield 'Removing ES for %s' % payload_id
            mozart_es.delete_by_id(index, id=payload_id)
            yield 'done: %s\n' % payload_id
        yield 'Finished\n'

    return Response(stream_purge(docs, es_index), mimetype="text/plain")
