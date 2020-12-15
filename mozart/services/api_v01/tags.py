from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

from flask import request
from flask_restx import Resource

from mozart import app, mozart_es
from mozart.services.api_v01 import api, user_tags_ns, user_rules_tags_ns


@user_tags_ns.route('', endpoint='user-tags')
@api.doc(responses={200: "Success", 500: "Execution failed"}, description="User tags for Mozart jobs")
class UserTags(Resource):
    def put(self):
        request_data = request.json or request.form
        _id = request_data.get('id')
        _index = request_data.get('index')
        tag = request_data.get('tag')
        app.logger.info('_id: %s\n _index: %s\n tag: %s' % (_id, _index, tag))

        if _index != 'job_status-current':
            app.logger.error('user tags only for index: job_status-current')
            return {
                'success': False,
                'message': 'user tags only for index: job_status-current'
            }, 400

        if _id is None or _index is None or tag is None:
            return {
                'success': False,
                'message': 'id, index and tag must be supplied'
            }, 400

        dataset = mozart_es.get_by_id(index=_index, id=_id, ignore=404)
        if dataset['found'] is False:
            return {
                'success': False,
                'message': "dataset not found"
            }, 404

        source = dataset['_source']
        user_tags = source.get('user_tags', [])
        app.logger.info('found user tags: %s' % str(user_tags))

        if tag not in user_tags:
            user_tags.append(tag)
            app.logger.info('tags after adding: %s' % str(user_tags))

        update_doc = {
            'doc_as_upsert': True,
            'doc': {
                'user_tags': user_tags
            }
        }
        mozart_es.update_document(index=_index, id=_id, body=update_doc, refresh=True)

        return {
            'success': True,
            'tags': user_tags
        }

    def delete(self):
        _id = request.args.get('id')
        _index = request.args.get('index')
        tag = request.args.get('tag')
        app.logger.info('_id: %s _index: %s tag: %s' % (_id, _index, tag))

        if _index != 'job_status-current':
            app.logger.error('user tags only for index: job_status-current')
            return {
                'success': False,
                'message': 'user tags only for index: job_status-current'
            }, 400

        if _id is None or _index is None:
            return {
                'success': False,
                'message': 'id and index must be supplied'
            }, 400

        dataset = mozart_es.get_by_id(index=_index, id=_id, ignore=404)
        if dataset['found'] is False:
            return {
                'success': False,
                'message': "dataset not found"
            }, 404

        source = dataset['_source']
        user_tags = source.get('user_tags', [])
        app.logger.info('found user tags %s' % str(user_tags))

        if tag in user_tags:
            user_tags.remove(tag)
            app.logger.info('tags after removing: %s' % str(user_tags))
        else:
            app.logger.warning('tag not found: %s' % tag)

        update_doc = {
            'doc_as_upsert': True,
            'doc': {
                'user_tags': user_tags
            }
        }
        mozart_es.update_document(index=_index, id=_id, body=update_doc, refresh=True)

        return {
            'success': True,
            'tags': user_tags
        }


@user_rules_tags_ns.route('', endpoint='user-rules-tags')
@api.doc(responses={200: "Success", 500: "Execution failed"}, description="User tags for Mozart user rules")
class UserRulesTags(Resource):
    def get(self):
        index = app.config['USER_RULES_INDEX']
        body = {
            "size": 0,
            "aggs": {
                "my_buckets": {
                    "composite": {
                        "size": 1000,
                        "sources": [
                            {
                                "tags": {
                                    "terms": {
                                        "field": "tags"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        results = mozart_es.search(index=index, body=body)
        buckets = results['aggregations']['my_buckets']['buckets']
        buckets = sorted(buckets, key=lambda k: k['doc_count'], reverse=True)
        app.logger.info(buckets)
        return {
            'success': True,
            'tags': [{
                'key': tag['key']['tags'],
                'count': tag['doc_count']
            } for tag in buckets]
        }

