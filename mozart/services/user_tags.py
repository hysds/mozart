import json, requests, types, re
from flask import jsonify, Blueprint, request, Response, render_template, make_response
from flask_login import login_required
from pprint import pformat

from mozart import app


mod = Blueprint('services/user_tags', __name__)


@mod.route('/user_tags/add', methods=['POST'])
@login_required
def add_user_tag():
    """Add a user tag."""

    # get tag
    id = request.form['id']
    tag = request.form['tag']
    if id is None:
        return jsonify({
            'success': False,
            'message': "Product ID not specified."
        }), 500

    app.logger.debug("Adding tag '%s' to id '%s'." % (tag, id))

    # query
    es_url = app.config['ES_URL']
    es_index = "%s-current" % app.config['JOB_STATUS_INDEX']
    query = {
        "fields": [ "user_tags" ],
        "query": { "term": { "_id": id } }
    }
    r = requests.post('%s/%s/_search' % (es_url, es_index), data=json.dumps(query))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to query ES. Got status code %d:\n%s" % 
                         (r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    if result['hits']['total'] != 1:
        app.logger.debug("Failed to only find 1 result for _id query %s" % id)
        return jsonify({
            'success': False,
            'message': "Failed to only find 1 result for _id query %s" % id
        }), 500

    # get actual index (no aliases), doctype and user tags
    actual_index = result['hits']['hits'][0]['_index']
    doctype = result['hits']['hits'][0]['_type']
    user_tags = result['hits']['hits'][0].get('fields', {}).get('user_tags', [])

    # add tag if not already there
    if tag not in user_tags: user_tags.append(tag)

    # upsert new document
    new_doc = {
        "doc": { "user_tags": user_tags },
        "doc_as_upsert": True
    }
    r = requests.post('%s/%s/%s/%s/_update' % (es_url, actual_index, doctype, id), data=json.dumps(new_doc))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to update user_tags for %s. Got status code %d:\n%s" % 
                         (id, r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    
    return jsonify({
        'success': True,
        'message': ""
    })


@mod.route('/user_tags/remove', methods=['POST'])
@login_required
def remove_user_tag():
    """Remove a user tag."""

    # get tag
    id = request.form['id']
    tag = request.form['tag']
    if id is None:
        return jsonify({
            'success': False,
            'message': "Product ID not specified."
        }), 500

    app.logger.debug("Removing tag '%s' to id '%s'." % (tag, id))

    # query
    es_url = app.config['ES_URL']
    es_index = "%s-current" % app.config['JOB_STATUS_INDEX']
    query = {
        "fields": [ "user_tags" ],
        "query": { "term": { "_id": id } }
    }
    r = requests.post('%s/%s/_search' % (es_url, es_index), data=json.dumps(query))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to query ES. Got status code %d:\n%s" % 
                         (r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    if result['hits']['total'] != 1:
        app.logger.debug("Failed to only find 1 result for _id query %s" % id)
        return jsonify({
            'success': False,
            'message': "Failed to only find 1 result for _id query %s" % id
        }), 500

    # get actual index (no aliases), doctype and user tags
    actual_index = result['hits']['hits'][0]['_index']
    doctype = result['hits']['hits'][0]['_type']
    user_tags = result['hits']['hits'][0].get('fields', {}).get('user_tags', [])

    # add tag if not already there
    if tag in user_tags: user_tags.remove(tag)

    # upsert new document
    new_doc = {
        "doc": { "user_tags": user_tags },
        "doc_as_upsert": True
    }
    r = requests.post('%s/%s/%s/%s/_update' % (es_url, actual_index, doctype, id), data=json.dumps(new_doc))
    result = r.json()
    if r.status_code != 200:
        app.logger.debug("Failed to update user_tags for %s. Got status code %d:\n%s" % 
                         (id, r.status_code, json.dumps(result, indent=2)))
    r.raise_for_status()
    
    return jsonify({
        'success': True,
        'message': ""
    })
