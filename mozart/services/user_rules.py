import os, json, requests, types, re, time
from flask import (jsonify, Blueprint, request, Response, render_template,
make_response, g, url_for, redirect)
from flask.ext.login import login_required
from pprint import pformat
from string import Template
from urlparse import urljoin

from hysds.orchestrator import submit_job

from mozart import app

import mozart.lib.queue_utils

import hysds_commons.action_utils
import hysds_commons.mozart_utils
import hysds_commons.job_rest_utils

mod = Blueprint('services/user_rules', __name__)


def create_user_rules_index(es_url, es_index):
    """Create user rules index applying percolator mapping."""

    # create index with percolator mapping
    mapping_file = os.path.normpath(os.path.join(
        app.root_path, '..', 'configs',
        'user_rules_job.mapping'))
    with open(mapping_file) as f:
        mapping = f.read()
    r = requests.put("%s/%s" % (es_url, es_index), data=mapping)
    r.raise_for_status()


def add_status_mappings(es_url, es_index):
    """Add mappings from job, task and worker indexes."""

    # get current mappings in user rules
    r = requests.get("%s/%s/_mapping" % (es_url, es_index))
    r.raise_for_status()
    user_rules_mappings = r.json()[es_index]['mappings']

    # get all mappings from mozart status indexes using alias
    status_index = app.config['JOB_STATUS_INDEX']
    r = requests.get("%s/%s/_mapping" % (es_url, status_index))
    r.raise_for_status()
    mappings = r.json()
    for idx in mappings:
        for doc_type in mappings[idx]['mappings']:
            if doc_type not in user_rules_mappings:
                r = requests.put("%s/%s/_mapping/%s" % (es_url, es_index, doc_type),
                                 data=json.dumps(mappings[idx]['mappings'][doc_type]))
                r.raise_for_status()


@mod.route('/user_rules/actions_config', methods=['GET'])
@login_required
def get_actions_config():
    """Return actions config."""

    # which interface is asking
    ifc = request.args.get('ifc')
    if ifc == 'monitor':
        ifc_filter = 'monitoring_allowed'
    elif ifc == 'process':
        ifc_filter = 'processing_allowed'
    else:
        raise RuntimeError("Invalid interface: %s" % ifc)
    # enable actions that need special auth
    actions = []
    for action in sorted(hysds_commons.action_utils.get_action_spec(app.config["ES_URL"],
                                                                    app.config["MOZART_URL"],
                                                                    app.config["OPS_USER"]),
                         key=lambda s: s['label'].lower()):
        if not action[ifc_filter]: continue
        if action['public'] is False:
            if g.user.id in action.get('allowed_accounts', []):
                action['public'] = True
        actions.append(action)

    return jsonify({'actions': actions})
@mod.route('/user_rules/get_job_queues', methods=['GET'])
@login_required
def get_job_queues():
    ''' '''
    job_type = request.args.get("job_type")
    queues = mozart.lib.queue_utils.get_queue_names(job_type)
    return jsonify(queues)

@mod.route('/user_rules/add', methods=['POST'])
@login_required
def add_user_rule():
    """Add a user rule."""

    # get rule
    rule_name = request.form['rule_name']
    workflow = request.form['workflow']
    priority = int(request.form.get('priority', 0))
    query_string = request.form['query_string']
    kwargs = request.form['kwargs']
    queue = request.form['queue']
    if workflow is None:
        return jsonify({
            'success': False,
            'message': "Workflow not specified.",
            'result': None,
        }), 500

    #app.logger.debug("user: %s" % g.user.id)
    #app.logger.debug("rule_name: %s" % rule_name)
    #app.logger.debug("workflow: %s" % workflow)
    #app.logger.debug("priority: %s" % priority)
    #app.logger.debug("query_string: %s" % query_string)
    #app.logger.debug("kwargs: %s" % kwargs)
    #app.logger.debug("Adding tag '%s' to id '%s'." % (tag, id))

    # get es url and index
    es_url = app.config['ES_URL']
    es_index = app.config['USER_RULES_INDEX']

    # if doesn't exist, create index
    r = requests.get('%s/%s' % (es_url, es_index))
    if r.status_code == 404:
        create_user_rules_index(es_url, es_index)

    # ensure status index mappings exist in percolator index
    add_status_mappings(es_url, es_index)

    # query
    query = {
        "query": {
            "bool": {
                "must": [
                    { "term": { "username": g.user.id }  },
                    { "term": { "rule_name": rule_name } },
                ]
            }
        }
    }
    r = requests.post('%s/%s/.percolator/_search' % (es_url, es_index), data=json.dumps(query))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to query ES. Got status code %d:\n%s" % 
                         (r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    if result['hits']['total'] == 1:
        app.logger.debug("Found a rule using that name already: %s" % rule_name)
        return jsonify({
            'success': False,
            'message': "Found a rule using that name already: %s" % rule_name,
            'result': None,
        }), 500

    # get job type
    job_type = None
    for action in sorted(hysds_commons.action_utils.get_action_spec(app.config["ES_URL"],
                                                                    app.config["MOZART_URL"],
                                                                    app.config["OPS_USER"]),
                         key=lambda s: s['label'].lower()):
        if action['type'] == workflow:
            job_type = action['job_type']
    if job_type is None: 
        app.logger.debug("No job_type find for '%s'." % workflow)
        return jsonify({
            'success': False,
            'message': "No job_type found for '%s'." % workflow,
            'result': None,
        }), 500

    # upsert new document
    new_doc = {
        "workflow": workflow,
        "priority": priority,
        "rule_name": rule_name,
        "username": g.user.id,
        "query_string": query_string,
        "kwargs": kwargs,
        "job_type": job_type,
        "enabled": True,
        "query": json.loads(query_string),
        "queue": queue
    }
    r = requests.post('%s/%s/.percolator/' % (es_url, es_index), data=json.dumps(new_doc))
    result = r.json()
    if r.status_code != 201:
        app.logger.debug("Failed to insert new rule for %s. Got status code %d:\n%s" % 
                         (g.user.id, r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    
    return jsonify({
        'success': True,
        'message': "",
        'result': result,
    })


@mod.route('/user_rules/list', methods=['GET'])
@login_required
def get_user_rules():
    """Get user rules."""

    # get url and index
    es_url = app.config['ES_URL']
    es_index = app.config['USER_RULES_INDEX']

    # if doesn't exist, create index
    r = requests.get('%s/%s' % (es_url, es_index))
    if r.status_code == 404:
        create_user_rules_index(es_url, es_index)

    # ensure status index mappings exist in percolator index
    add_status_mappings(es_url, es_index)

    # query
    if g.user.id == app.config['OPS_USER']:
        query = {"query":{"match_all": {}}}
    else:
        query = {
            "query": {
                "bool": {
                    "must": [
                        { "term": { "username": g.user.id }  }
                    ]
                }
            }
        }
    r = requests.post('%s/%s/.percolator/_search?search_type=scan&scroll=10m&size=100' % (es_url, es_index),
                      data=json.dumps(query))
    if r.status_code != 200:
        app.logger.debug("Failed to query ES. Got status code %d:\n%s" % 
                         (r.status_code, json.dumps(r.json(), indent=2)))
    r.raise_for_status()
    #app.logger.debug("result: %s" % pformat(r.json()))

    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']

    # get list of rules
    rules = []
    rule_count = 0
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            rule_count += 1
            rule = hit['_source']
            rule['#'] = rule_count 
            rule['id'] = hit['_id']
            rules.append(rule)

    return jsonify({ "success": True, "rules": rules})


@mod.route('/user_rules/remove', methods=['POST'])
@login_required
def remove_user_rule():
    """Remove a user rule."""

    # get tag
    id = request.form['id']
    if id is None:
        return jsonify({
            'success': False,
            'message': "Rule ID not specified."
        }), 500

    app.logger.debug("Removing rule id '%s'." % id)

    # delete
    es_url = app.config['ES_URL']
    es_index = app.config['USER_RULES_INDEX']
    r = requests.delete('%s/%s/.percolator/%s' % (es_url, es_index, id))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to delete rule with ID %s. Got status code %d" %
                         (id, r.status_code))
    r.raise_for_status()
    
    return jsonify({
        'success': True,
        'message': "",
        'result': result,
    })


@mod.route('/user_rules/toggle_status', methods=['POST'])
@login_required
def toggle_status():
    """Toggle enabled parameter."""

    # get id and enabled status
    id = request.form['id']
    if id is None:
        return jsonify({
            'success': False,
            'message': "Product ID not specified."
        }), 500
    enabled = request.form['enabled']

    app.logger.debug("Setting enabled to '%s' to id '%s'." % (enabled, id))

    # update enabled
    if enabled == "true": enabled = True
    else: enabled = False
    new_doc = {
        "doc": { "enabled": enabled },
        "doc_as_upsert": True
    }
    es_url = app.config['ES_URL']
    es_index = app.config['USER_RULES_INDEX']
    r = requests.post('%s/%s/.percolator/%s/_update' % (es_url, es_index, id), data=json.dumps(new_doc))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to update enabled field for %s. Got status code %d:\n%s" % 
                         (id, r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    
    return jsonify({
        'success': True,
        'message': ""
    })


@mod.route('/user_rules/edit', methods=['POST'])
@login_required
def edit_user_rule():
    """Edit a user rule."""

    # get rule
    id = request.form['id']
    rule_name = request.form['rule_name']
    workflow = request.form['workflow']
    queue = request.form['queue']
    priority = int(request.form.get('priority', 0))
    query_string = request.form['query_string']
    kwargs = request.form['kwargs']
    if workflow is None:
        return jsonify({
            'success': False,
            'message': "Workflow not specified.",
            'result': None,
        }), 500

    #app.logger.debug("user: %s" % g.user.id)
    #app.logger.debug("rule_id: %s" % id)
    #app.logger.debug("rule_name: %s" % rule_name)
    #app.logger.debug("workflow: %s" % workflow)
    #app.logger.debug("priority: %s" % priority)
    #app.logger.debug("query_string: %s" % query_string)
    #app.logger.debug("kwargs: %s" % kwargs)
    #app.logger.debug("Adding tag '%s' to id '%s'." % (tag, id))

    # get job type
    job_type = None
    for action in sorted(hysds_commons.action_utils.get_action_spec(app.config["ES_URL"],
                                                                    app.config["MOZART_URL"],
                                                                    app.config["OPS_USER"]),
                         key=lambda s: s['label'].lower()):
        if action['type'] == workflow:
            job_type = action['job_type']

    if job_type is None: 
        app.logger.debug("No job_type find for '%s'." % workflow)
        return jsonify({
            'success': False,
            'message': "No job_type found for '%s'." % workflow,
            'result': None,
        }), 500

    # upsert new document
    new_doc = {
        "doc": {
            "workflow": workflow,
            "priority": priority,
            "rule_name": rule_name,
            "username": g.user.id,
            "query_string": query_string,
            "kwargs": kwargs,
            "job_type": job_type,
            "query": json.loads(query_string),
            "queue": queue
        },
        "doc_as_upsert": True
    }
    es_url = app.config['ES_URL']
    es_index = app.config['USER_RULES_INDEX']
    #url = '%s/%s/%s/%s/_update' % (es_url, es_index, 'user_rule', id)
    #app.logger.debug("url: %s" % url)
    #app.logger.debug("data: %s" % json.dumps(new_doc, indent=2))
    r = requests.post('%s/%s/.percolator/%s/_update' % (es_url, es_index, id), data=json.dumps(new_doc))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to update rule for %s. Got status code %d:\n%s" % 
                         (id, r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    
    return jsonify({
        'success': True,
        'message': "",
        'result': result,
    })


@mod.route('/user_rules/submit_job', methods=['POST'])
@login_required
def process_this():
    """Submit job from 'Process This'."""

    # get args
    name = request.form['name']
    workflow = request.form['workflow']
    priority = int(request.form.get('priority', 0))
    queue = request.form.get('queue',None)
    query_string = request.form['query_string']
    kwargs = request.form['kwargs']
    if workflow is None:
        return jsonify({
            'success': False,
            'message': "Workflow not specified.",
            'result': None,
        }), 500

    # enable actions that need special auth
    matched_action = None
    for action in sorted(hysds_commons.action_utils.get_action_spec(app.config["ES_URL"],
                                                                    app.config["MOZART_URL"],
                                                                    app.config["OPS_USER"]),
                         key=lambda s: s['label'].lower()):
        if action['type'] == workflow:
            matched_action = action
            if matched_action['public'] is False:
                if g.user.id in matched_action.get('allowed_accounts', []):
                    matched_action['public'] = True
            break

    # fail if no match found
    if matched_action is None:
        app.logger.debug("Found no action for type: %s" % workflow)
        return jsonify({
            'success': False,
            'message': "Found no action for type: %s" % workflow,
            'result': None,
        }), 500

    # fail if not authorized
    if matched_action['public'] is False:
        app.logger.debug("You are unauthorized to run job type: %s" % workflow)
        return jsonify({
            'success': False,
            'message': "You are unauthorized to run job type: %s" % workflow,
            'result': None,
        }), 500

    # create rule
    rule = {
        'username': g.user.id,
        'workflow': workflow,
        'queue': queue,
        'priority': priority,
        'enabled': True,
        'job_type': matched_action['job_type'],
        'rule_name': name,
        'kwargs': kwargs,
        'query_string': query_string,
        'query': json.loads(query_string),
    }

    # if job_type is a url type, pass it on
    url_match = re.search(r'^url:/(.*)$', matched_action['job_type'])
    if url_match:
        url_tmpl = Template(url_match.group(1))
        result_link = urljoin(request.url_root, url_tmpl.substitute(rule))

        # html result
        html = "<b>Execute and retrieve the result of your job "
        html += "<a href='%s' target='_blank'>here</a>.</b>" % result_link
        
        return jsonify({
            'success': True,
            'message': "",
            'html': html
        })
    #Setup input arguments here
    rule_temp = {"rule_name":rule["rule_name"],"queue":app.config.get("JOB_SUBMISSION_QUEUE",rule["queue"]),"priority":rule["priority"],"kwargs":"{}"}
    param_temp = [{"name":"submitter","from":"value","value":"mozart"},{"name":"submitter_rule","from":"value","value":json.dumps(rule)}]

    hysds_commons.job_rest_utils.single_process_and_submission(app.config["MOZART_URL"], {}, rule_temp , hysdsio={"id":"internal-temporary-wiring","params":param_temp,"job-specification":app.config["JOB_SUBMISSION_JOB_SPEC"]})

    #SKIP User Run History -- Historical reason

    # monitor jobs by tag and username
    monitor_url = url_for('.monitor_jobs', tag=rule['rule_name'], username=rule['username'])
    html = "<b>You're HySDS jobs were submitted. Monitor their execution "
    html += '<a href="%s" target="_blank">here</a>.</b>' % monitor_url

    return jsonify({
        'success': True,
        'message': "",
        'html': html
    })


@mod.route('/user_rules/monitor_job', methods=['GET'])
@login_required
def monitor_job():
    """Page redirector for job monitor."""

    # get id and enabled status
    orch_task_id = request.args.get('task_id', None)
    if orch_task_id is None:
        return jsonify({
            'success': False,
            'message': "Task ID not specified."
        }), 500
    app.logger.debug("orch_task_id: %s" % orch_task_id)

    # get orchestrator task ID of job
    r = requests.get('%s/api/task/info/%s' % (app.config['FLOWER_URL'], orch_task_id))
    orch_task_info = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to retrieve orchestrator task info for %s." % orch_task_id)
    r.raise_for_status()

    # check state and get task id
    if orch_task_info['state'] == "SUCCESS":
        task_id = eval(eval(orch_task_info['result']))[0]
        app.logger.debug("task_id: %s" % task_id)
    else:
        app.logger.debug("Failed to retrieve orchestrator task state of 'SUCCESS'.")
        return jsonify({
            'success': False,
            'message': "Failed to retrieve orchestrator task with state 'SUCCESS'.",
            'task_info': orch_task_info,
        })
    
    # get Mozart link to job
    moz_url = '?source={"query":{"query_string":{"query":"task_id:%s"}}}' % task_id

    # hand craft redirect so that { and } chars are not escaped
    return redirect(url_for('views/main.index') + moz_url)


@mod.route('/user_rules/monitor_jobs', methods=['GET'])
@login_required
def monitor_jobs():
    """Page redirector for job monitor by tag."""

    # get tag and user
    tag = request.args.get('tag', None)
    if tag is None:
        return jsonify({
            'success': False,
            'message': "Tag not specified."
        }), 500
    username = request.args.get('username', None)
    if username is None:
        return jsonify({
            'success': False,
            'message': "User not specified."
        }), 500
    app.logger.debug("tag: %s" % tag)
    app.logger.debug("username: %s" % username)

    # get Mozart link to job
    moz_url = '?source={"query":{"bool":{"must":[{"term":{"job.job.tag":"%s"}},{"term":{"job.job.username":"%s"}}]}}}'

    # hand craft redirect so that { and } chars are not escaped
    return redirect(url_for('views/main.index') + moz_url % (tag, username))
