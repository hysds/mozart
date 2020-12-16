from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
import traceback

from flask import request
from flask_restx import Namespace, Resource, fields

from hysds.celery import app as celery_app
from hysds.task_worker import do_submit_task
import hysds_commons.job_utils
from hysds_commons.action_utils import check_passthrough_query

from mozart import app, mozart_es

# Library backend imports
import mozart.lib.job_utils
import mozart.lib.queue_utils


JOB_NS = "job"
job_ns = Namespace(JOB_NS, description="Mozart job operations")

QUEUE_NS = "queue"
queue_ns = Namespace(QUEUE_NS, description="Mozart queue operations")

ON_DEMAND_NS = "on-demand"
on_demand_ns = Namespace(ON_DEMAND_NS, description="For retrieving and submitting on-demand jobs for mozart")

HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
JOB_SPECS_INDEX = app.config['JOB_SPECS_INDEX']
JOB_STATUS_INDEX = app.config['JOB_STATUS_INDEX']
CONTAINERS_INDEX = app.config['CONTAINERS_INDEX']


@job_ns.route('/list', endpoint='job-list')
@job_ns.doc(responses={200: "Success", 500: "Query execution failed"},
            description="Get list of submitted job IDs.")
class GetJobs(Resource):
    """Get list of job IDs."""

    resp_model = job_ns.model('Jobs Listing Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.List(fields.String, required=True,
                               description="list of job IDs")
    })
    parser = job_ns.parser()
    parser.add_argument('page_size', required=False, type=str,
                        help="Job Listing Pagination Size")
    parser = job_ns.parser()
    parser.add_argument('offset', required=False, type=str,
                        help="Job Listing Pagination Offset")

    @job_ns.marshal_with(resp_model)
    def get(self):
        """Paginated list submitted jobs"""
        jobs = mozart_es.query(index=JOB_STATUS_INDEX, _source=False)
        return {
            'success': True,
            'message': "",
            'result': sorted([job["_id"] for job in jobs])
        }


@job_ns.route('/submit', endpoint='job-submit')
@job_ns.doc(responses={200: "Success", 400: "Invalid parameters", 500: "Job submission failed"},
            description="Submit job for execution in HySDS.")
class SubmitJob(Resource):
    """Submit job for execution in HySDS."""

    resp_model = job_ns.model('SubmitJobResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.String(required=True, description="HySDS job ID")
    })

    parser = job_ns.parser()
    parser.add_argument('type', required=True, type=str,
                        help="a job type from jobspec/list")
    parser.add_argument('queue', required=True, type=str,
                        help="Job queue from /queue/list e.g. grfn-job_worker-small")
    parser.add_argument('priority', required=False, type=int,
                        help='Job priority in the range of 0 to 9')
    parser.add_argument('tags', required=False, type=str,
                        help='JSON list of tags, e.g. ["dumby", "test_job"]')
    parser.add_argument('name', required=False,
                        type=str, help='base job name override; defaults to job type')
    parser.add_argument('payload_hash', required=False,
                        type=str, help='user-generated payload hash')
    parser.add_argument('enable_dedup', required=False,
                        type=bool, help='flag to enable/disable job dedup')
    parser.add_argument('params', required=False, type=str,
                        help="""JSON job context, e.g. {
                             "entity_id": "LC80101172015002LGN00",
                             "min_lat": -79.09923,
                             "max_lon": -125.09297,
                             "id": "dumby-product-20161114180506209624",
                             "acq_time": "2015-01-02T15:49:05.571384",
                             "min_sleep": 1,
                             "max_lat": -77.7544,
                             "min_lon": -139.66082,
                             "max_sleep": 10
                            }""")

    @job_ns.marshal_with(resp_model)
    @job_ns.expect(parser, validate=True)
    def post(self):
        """Submits a job to run inside HySDS"""
        job_type = request.form.get('type', request.args.get('type', None))
        job_queue = request.form.get('queue', request.args.get('queue', None))

        priority = int(request.form.get('priority', request.args.get('priority', 0)))
        tags = request.form.get('tags', request.args.get('tags', None))

        job_name = request.form.get('name', request.args.get('name', None))

        payload_hash = request.form.get('payload_hash', request.args.get('payload_hash', None))
        enable_dedup = str(request.form.get('enable_dedup', request.args.get('enable_dedup', "true")))

        try:
            if enable_dedup.strip().lower() == "true":
                enable_dedup = True
            elif enable_dedup.strip().lower() == "false":
                enable_dedup = False
            else:
                raise Exception("Invalid value for param 'enable_dedup': {0}".format(enable_dedup))

            try:
                if not tags is None:
                    tags = json.loads(tags)
            except Exception as e:
                raise Exception("Failed to parse input tags. '{0}' is malformed".format(tags))

            params = request.form.get('params', request.args.get('params', "{}"))
            app.logger.warning(params)
            try:
                if not params is None:
                    params = json.loads(params)
            except Exception as e:
                raise Exception("Failed to parse input params. '{0}' is malformed".format(params))

            app.logger.warning(job_type)
            app.logger.warning(job_queue)
            job_json = hysds_commons.job_utils.resolve_hysds_job(job_type, job_queue, priority, tags, params,
                                                                 job_name=job_name,
                                                                 payload_hash=payload_hash,
                                                                 enable_dedup=enable_dedup)
            ident = hysds_commons.job_utils.submit_hysds_job(job_json)
        except Exception as e:
            message = "Failed to submit job. {0}:{1}".format(type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500

        return {
            'success': True,
            'message': '',
            'result': ident
        }


@queue_ns.route('/list', endpoint='queue-list')
@queue_ns.doc(responses={200: "Success", 500: "Queue listing failed"},
              description="Get list of available job queues and return as JSON.")
class GetQueueNames(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = queue_ns.model('Queue Listing Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="queue response")
    })
    parser = queue_ns.parser()
    parser.add_argument('id', required=False, type=str,
                        help="Job Type Specification ID")

    @queue_ns.expect(parser)
    @queue_ns.marshal_with(resp_model)
    def get(self):
        """Gets a listing of non-celery queues handling jobs."""
        try:
            ident = request.form.get('id', request.args.get('id', None))
            queues = mozart.lib.queue_utils.get_queue_names(ident)
            app.logger.warn("Queues: " + str(queues))
        except Exception as e:
            message = "Failed to list job queues. {0}:{1}".format(type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {
            'success': True,
            'message': "",
            'result': queues
        }


@job_ns.route('/status', endpoint='job-status')
@job_ns.doc(responses={200: "Success", 500: "Query execution failed"},
            description="Get status of job by ID.")
class GetJobStatus(Resource):
    """Get status of job ID."""

    resp_model = job_ns.model('Job Status Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'status':  fields.String(required=True,
                                 enum=['job-queued', 'job-started', 'job-failed',
                                       'job-completed', 'job-offline', 'job-revoked'],
                                 description='job status')
    })

    parser = job_ns.parser()
    parser.add_argument('id', required=True, type=str, help="Job ID")

    @job_ns.expect(parser)
    @job_ns.marshal_with(resp_model)
    def get(self):
        """Gets the status of a submitted job based on job id"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'id not supplied'}, 400

        job_status = mozart_es.get_by_id(index=JOB_STATUS_INDEX, id=_id, ignore=404)
        if job_status['found'] is False:
            return {
                'success': False,
                'message': 'job status not found: %s' % _id
            }, 404

        return {
            'success': True,
            'message': "",
            'status': job_status['_source']['status']
        }


@job_ns.route('/info', endpoint='job-info')
@job_ns.doc(responses={200: "Success", 500: "Query execution failed"},
            description="Gets the complete info for a job.")
class GetJobInfo(Resource):
    """Get info of job IDs."""

    resp_model = job_ns.model('Job Info Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="Job Info Object")
    })
    parser = job_ns.parser()
    parser.add_argument('id', required=True, type=str, help="Job ID")

    @job_ns.expect(parser)
    @job_ns.marshal_with(resp_model)
    def get(self):
        """Get complete info for submitted job based on id"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'id must be supplied'}, 400

        info = mozart_es.get_by_id(index=JOB_STATUS_INDEX, id=_id, ignore=404)
        if info['found'] is False:
            return {
                'success': False,
                'message': 'job info not found: %s' % _id
            }, 404

        return {
            'success': True,
            'message': "",
            'result': info['_source']
        }


@on_demand_ns.route('', endpoint='on-demand')
@on_demand_ns.doc(responses={200: "Success", 500: "Execution failed"},
                  description="Retrieve on demand jobs")
class OnDemandJobs(Resource):
    """On Demand Jobs API."""

    resp_model = on_demand_ns.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = on_demand_ns.parser()

    # @api.marshal_with(resp_model)
    def get(self):
        """List available on demand jobs"""
        query = {
            "_source": ["id", "job-specification", "label", "job-version"],
            "sort": [{"label.keyword": {"order": "asc"}}],
            "query": {
                "exists": {
                    "field": "job-specification"
                }
            }
        }

        documents = mozart_es.query(index=HYSDS_IOS_INDEX, body=query)
        documents = [{
            'hysds_io': row['_source']['id'],
            'job_spec': row['_source']['job-specification'],
            'version': row['_source']['job-version'],
            'label': row['_source']['label']
        } for row in documents]

        return {
            'success': True,
            'result': documents
        }

    def post(self):
        """
        submits on demand job
        :return: submit job id
        """
        # TODO: add user auth and permissions
        request_data = request.json
        if not request_data:
            request_data = request.form

        tag = request_data.get('tags', None)
        job_type = request_data.get('job_type', None)
        hysds_io = request_data.get('hysds_io', None)
        queue = request_data.get('queue', None)
        priority = int(request_data.get('priority', 0))
        query_string = request_data.get('query', None)
        kwargs = request_data.get('kwargs', '{}')
        time_limit = request_data.get('time_limit', None)
        soft_time_limit = request_data.get('soft_time_limit', None)
        disk_usage = request_data.get('disk_usage', None)

        try:
            query = json.loads(query_string)
            query_string = json.dumps(query)
        except (ValueError, TypeError, Exception) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid JSON query'
            }, 400

        if tag is None or job_type is None or hysds_io is None or queue is None or query_string is None:
            return {
                'success': False,
                'message': 'missing field: [tags, job_type, hysds_io, queue, query]'
            }, 400

        doc = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
        if doc['found'] is False:
            app.logger.error('failed to fetch %s, not found in hysds_ios' % hysds_io)
            return {
                'success': False,
                'message': '%s not found' % hysds_io
            }, 404

        params = doc['_source']['params']
        is_passthrough_query = check_passthrough_query(params)

        rule = {
            'username': 'example_user',
            'workflow': hysds_io,
            'priority': priority,
            'enabled': True,
            'job_type': job_type,
            'rule_name': tag,
            'kwargs': kwargs,
            'query_string': query_string,
            'query': query,
            'passthru_query': is_passthrough_query,
            'query_all': False,
            'queue': queue
        }

        if time_limit and isinstance(time_limit, int):
            if time_limit <= 0 or time_limit > 86400 * 7:
                return {
                    'success': False,
                    'message': 'time_limit must be between 0 and 604800 (sec)'
                }, 400
            else:
                rule['time_limit'] = time_limit

        if soft_time_limit and isinstance(soft_time_limit, int):
            if soft_time_limit <= 0 or soft_time_limit > 86400 * 7:
                return {
                    'success': False,
                    'message': 'soft_time_limit must be between 0 and 604800 (sec)'
                }, 400
            else:
                rule['soft_time_limit'] = soft_time_limit

        if disk_usage:
            rule['disk_usage'] = disk_usage

        payload = {
            'type': 'job_iterator',
            'function': 'hysds_commons.job_iterator.iterate',
            'args': ["figaro", rule],
        }

        on_demand_job_queue = celery_app.conf['ON_DEMAND_JOB_QUEUE']
        celery_task = do_submit_task(payload, on_demand_job_queue)

        return {
            'success': True,
            'result': celery_task.id
        }


@on_demand_ns.route('/job-params', endpoint='job-params')
@on_demand_ns.doc(responses={200: "Success", 500: "Execution failed"},
                  description="Retrieve on job params for specific jobs")
class JobParams(Resource):
    """Job Params API."""

    resp_model = on_demand_ns.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = on_demand_ns.parser()

    # @api.marshal_with(resp_model)
    def get(self):
        job_type = request.args.get('job_type')
        if not job_type:
            return {'success': False, 'message': 'job_type not provided'}, 400

        query = {
            "query": {
                "term": {"job-specification.keyword": job_type}
            }
        }
        hysds_io = mozart_es.search(index=HYSDS_IOS_INDEX, body=query)

        if hysds_io['hits']['total']['value'] == 0:
            return {
                'success': False,
                'message': '%s not found in hysds_ios' % job_type
            }, 404

        hysds_io = hysds_io['hits']['hits'][0]
        job_params = hysds_io['_source']['params']
        job_params = list(filter(lambda x: x['from'] == 'submitter', job_params))

        job_spec = mozart_es.get_by_id(index=JOB_SPECS_INDEX, id=job_type, ignore=404)
        if job_spec.get('found', False) is False:
            return {
                'success': False,
                'message': '%s not found in job_specs' % job_type
            }, 404

        return {
            'success': True,
            'submission_type': hysds_io['_source'].get('submission_type'),
            'hysds_io': hysds_io['_source']['id'],
            'params': job_params,
            'time_limit': job_spec['_source']['time_limit'],
            'soft_time_limit': job_spec['_source']['soft_time_limit'],
            'disk_usage': job_spec['_source']['disk_usage']
        }