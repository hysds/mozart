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
from datetime import datetime

from flask import jsonify, Blueprint, request, Response, render_template, make_response

from hysds.celery import app as celery_app
from hysds.task_worker import do_submit_task
from flask_restx import Api, apidoc, Resource, fields

import hysds_commons.container_utils
import hysds_commons.hysds_io_utils
import hysds_commons.job_spec_utils
import hysds_commons.job_utils
from hysds_commons.action_utils import check_passthrough_query

from hysds.log_utils import log_custom_event

from mozart import app, mozart_es

# Library backend imports
import mozart.lib.job_utils
import mozart.lib.queue_utils


services = Blueprint('api_v0-2', __name__, url_prefix='/api/v0.2')
api = Api(services, ui=False, version="0.2", title="Mozart API", description="API for HySDS job submission and query.")

# Namespace declarations
QUEUE_NS = "queue"
queue_ns = api.namespace(QUEUE_NS, description="Mozart queue operations")

JOBSPEC_NS = "job_spec"
job_spec_ns = api.namespace(JOBSPEC_NS, description="Mozart job-specification operations")

JOB_NS = "job"
job_ns = api.namespace(JOB_NS, description="Mozart job operations")

CONTAINER_NS = "container"
container_ns = api.namespace(CONTAINER_NS, description="Mozart container operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")

EVENT_NS = "event"
event_ns = api.namespace(EVENT_NS, description="HySDS event stream operations")

ON_DEMAND_NS = "on-demand"
on_demand_ns = api.namespace(ON_DEMAND_NS, description="For retrieving and submitting on-demand jobs for mozart")

USER_RULE_NS = "user-rules"
user_rule_ns = api.namespace(USER_RULE_NS, description="C.R.U.D. for Mozart user rules")

USER_TAGS_NS = "user-tags"
user_tags_ns = api.namespace(USER_TAGS_NS, description="user tags for Mozart jobs")

USER_RULES_TAGS = "user-rules-tags"
user_rules_tags_ns = api.namespace(USER_RULES_TAGS, description="user tags for Mozart jobs")

HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
JOB_SPECS_INDEX = app.config['JOB_SPECS_INDEX']
JOB_STATUS_INDEX = app.config['JOB_STATUS_INDEX']
CONTAINERS_INDEX = app.config['CONTAINERS_INDEX']


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@job_spec_ns.route('/list', endpoint='job_specs')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Get list of registered job types and return as JSON.")
class GetJobTypes(Resource):
    """Get list of registered job types and return as JSON."""
    resp_model_job_types = api.model('Job Type List Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing " + "success or failure"),
        'result':  fields.List(fields.String, required=True, description="list of job types")
    })

    @api.marshal_with(resp_model_job_types)
    def get(self):
        """Gets a list of Job Type specifications"""
        query = {
            "query": {
                "match_all": {}
            }
        }
        job_specs = mozart_es.query(index=JOB_SPECS_INDEX, body=query)
        ids = [job_spec['_id'] for job_spec in job_specs]
        return {
            'success': True,
            'message': "",
            'result': ids
        }


@job_spec_ns.route('', endpoint='job_spec')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Get list of registered job types and return as JSON.")
class JobSpecs(Resource):
    """Rest APIs for all job_specs (GET, POST, DELETE)"""

    resp_model_job_type = api.model('Job Type List Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result': fields.Raw(fields.String, required=True, description="list of job types")
    })

    job_spec_parser = api.parser()
    job_spec_parser.add_argument('id', required=False, type=str, help="Job Type ID")

    post_job_spec_parser = api.parser()
    post_job_spec_parser.add_argument('spec', required=True, type=str, help="Job Type Specification JSON Object")

    @api.marshal_with(resp_model_job_type)
    @api.expect(job_spec_parser)
    def get(self):
        """Gets a Job Type specification object for the given ID."""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'missing parameter: id'}, 400

        job_spec = mozart_es.get_by_id(index=JOB_SPECS_INDEX, id=_id, ignore=404)
        if job_spec['found'] is False:
            app.logger.error('job_spec not found %s' % _id)
            return {
                'success': False,
                'message': 'Failed to retrieve job_spec: %s' % _id
            }, 404

        return {
            'success': True,
            'message': "job spec for %s" % _id,
            'result': job_spec['_source']
        }

    @api.marshal_with(resp_model_job_type)
    @api.expect(post_job_spec_parser)
    def post(self):
        """Add a Job Type specification JSON object."""
        spec = request.form.get('spec', request.args.get('spec', None))
        if spec is None:
            return {'success': False, 'message': 'spec object missing'}, 400

        try:
            obj = json.loads(spec)
            _id = obj['id']
        except (ValueError, KeyError, json.decoder.JSONDecodeError, Exception) as e:
            return {'success': False, 'message': e}, 400

        mozart_es.index_document(index=JOB_SPECS_INDEX, body=obj, id=_id)
        return {
            'success': True,
            'message': "job_spec %s added to index %s" % (_id, HYSDS_IOS_INDEX),
            'result': _id
        }

    @api.marshal_with(resp_model_job_type)
    @api.expect(job_spec_parser)
    def delete(self):
        """Remove Job Spec for the given ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {
                'success': False,
                'message': 'id parameter not included'
            }, 400

        mozart_es.delete_by_id(index=JOB_SPECS_INDEX, id=_id)
        app.logger.info('Deleted job_spec %s from index: %s' % (_id, JOB_SPECS_INDEX))
        return {
            'success': True,
            'message': ""
        }


@queue_ns.route('/list', endpoint='queue-list')
@api.doc(responses={200: "Success",
                    500: "Queue listing failed"},
         description="Get list of available job queues and return as JSON.")
class JobQueues(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = api.model('Queue Listing Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.Raw(required=True, description="queue response")
    })

    parser = api.parser()
    parser.add_argument('id', required=False, type=str, help="Job Type Specification ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
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


@job_ns.route('/submit', endpoint='job-submit')
@api.doc(responses={200: "Success",
                    400: "Invalid parameters",
                    500: "Job submission failed"},
         description="Submit job for execution in HySDS.")
class SubmitJob(Resource):
    """Submit job for execution in HySDS."""

    resp_model = api.model('SubmitJobResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.String(required=True, description="HySDS job ID")
    })

    parser = api.parser()
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

    @api.marshal_with(resp_model)
    @api.expect(parser, validate=True)
    def post(self):
        """Submits a job to run inside HySDS"""
        try:
            app.logger.warning(request.form)

            job_type = request.form.get('type', request.args.get('type', None))
            job_queue = request.form.get('queue', request.args.get('queue', None))

            priority = int(request.form.get('priority', request.args.get('priority', 0)))

            tags = request.form.get('tags', request.args.get('tags', None))
            job_name = request.form.get('name', request.args.get('name', None))

            payload_hash = request.form.get('payload_hash', request.args.get('payload_hash', None))
            enable_dedup = str(request.form.get('enable_dedup', request.args.get('enable_dedup', "true")))

            if enable_dedup.strip().lower() == "true":
                enable_dedup = True
            elif enable_dedup.strip().lower() == "false":
                enable_dedup = False
            else:
                raise Exception(
                    "Invalid value for param 'enable_dedup': {0}".format(enable_dedup))
            try:
                if not tags is None:
                    tags = json.loads(tags)
            except Exception as e:
                raise Exception(
                    "Failed to parse input tags. '{0}' is malformed".format(tags))

            params = request.form.get(
                'params', request.args.get('params', "{}"))
            app.logger.warning(params)

            try:
                if not params is None:
                    params = json.loads(params)
            except Exception as e:
                raise Exception(
                    "Failed to parse input params. '{0}' is malformed".format(params))

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


@job_ns.route('', endpoint='job-status')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Get status of job by ID.")
class JobStatus(Resource):
    """Get status of job ID."""

    statuses = ['job-queued', 'job-started', 'job-failed', 'job-completed', 'job-offline', 'job-revoked']
    resp_model = api.model('Job Status Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'status':  fields.String(required=True, enum=statuses, description='job status')
    })

    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Job ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
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


@job_ns.route('/list', endpoint='job-list')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Get list of submitted job IDs.")
class Jobs(Resource):
    """Get list of job IDs."""

    resp_model = api.model('Jobs Listing Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.List(fields.String, required=True, description="list of job IDs")
    })
    parser = api.parser()
    parser.add_argument('page_size', required=False, type=str, help="Job Listing Pagination Size")
    parser.add_argument('offset', required=False, type=str, help="Job Listing Pagination Offset")

    @api.marshal_with(resp_model)
    def get(self):
        """Paginated list submitted jobs"""
        jobs = mozart_es.query(index=JOB_STATUS_INDEX, _source=False)
        return {
            'success': True,
            'message': "",
            'result': sorted([job["_id"] for job in jobs])
        }


@job_ns.route('/info', endpoint='job-info')
@api.doc(responses={200: "Success", 500: "Query execution failed"}, description="Gets the complete info for a job.")
class JobInfo(Resource):
    """Get info of job IDs."""

    resp_model = api.model('Job Info Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.Raw(required=True, description="Job Info Object")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Job ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
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


@container_ns.route('/list', endpoint='containers')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Get list of registered containers and return as JSON.")
class ContainerTypes(Resource):
    """Get list of registered containers and return as JSON."""
    resp_model_job_types = api.model('Container List Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.List(fields.String, required=True, description="list of hysds-io types")
    })

    @api.marshal_with(resp_model_job_types)
    def get(self):
        """Get a list of containers managed by Mozart"""
        containers = mozart_es.query(index=CONTAINERS_INDEX, _source=False)
        ids = [container['_id'] for container in containers]

        return {
            'success': True,
            'message': "",
            'result': ids
        }


@container_ns.route('', endpoint='container')
@api.doc(responses={200: "Success", 500: "Query execution failed"}, description="Containers endpoint")
class Containers(Resource):
    """Container Rest APIs (GET, POST, DELETE)"""

    resp_model = api.model('Job Info Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result': fields.Raw(required=True, description="Container information or ID")
    })

    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Container ID")

    post_parser = api.parser()
    post_parser.add_argument('name', required=True, type=str, help="Container Name")
    post_parser.add_argument('url', required=True, type=str, help="Container URL")
    post_parser.add_argument('version', required=True, type=str, help="Container Version")
    post_parser.add_argument('digest', required=True, type=str, help="Container Digest")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        """Get information on container by ID"""
        _id = request.form.get('id', request.args.get('id', None))

        container = mozart_es.get_by_id(index=CONTAINERS_INDEX, id=_id, ignore=404)
        if container['found'] is False:
            return {
                'success': False,
                'message': ""
            }, 404

        return {
            'success': True,
            'message': "",
            'result': container['_source']
        }

    @api.expect(post_parser)
    @api.marshal_with(resp_model)
    def post(self):
        """Add a container specification to Mozart"""
        name = request.form.get('name', request.args.get('name', None))
        url = request.form.get('url', request.args.get('url', None))
        version = request.form.get('version', request.args.get('version', None))
        digest = request.form.get('digest', request.args.get('digest', None))

        if not all((name, url, version, digest)):
            return {
                'success': False,
                'message': 'Parameters (name, url, version, digest) must be supplied'
            }, 400

        container_obj = {
            'id': name,
            'digest': digest,
            'url': url,
            'version': version
        }
        mozart_es.index_document(index=CONTAINERS_INDEX, body=container_obj, id=name)

        return {
            'success': True,
            'message': "%s added to index %s" % (name, CONTAINERS_INDEX),
            'result': name
        }

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def delete(self):
        """Remove container based on ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {
                'success': False,
                'message': 'id must be supplied'
            }, 400

        mozart_es.delete_by_id(index=CONTAINERS_INDEX, id=_id)
        app.logger.info('Deleted container %s from index: %s' % (_id, CONTAINERS_INDEX))
        return {
            'success': True,
            'message': "job_spec deleted: %s" % _id,
            'result': _id
        }


@hysds_io_ns.route('/list', endpoint='hysds_ios')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Gets list of registered hysds-io specifications and return as JSON.")
class HySDSIOTypes(Resource):
    """Get list of registered hysds-io and return as JSON."""
    resp_model_job_types = api.model('HySDS IO List Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.List(fields.String, required=True, description="list of hysds-io types")
    })

    @api.marshal_with(resp_model_job_types)
    def get(self):
        """List HySDS IO specifications"""
        hysds_ios = mozart_es.query(index=HYSDS_IOS_INDEX, _source=False)
        ids = [hysds_io['_id'] for hysds_io in hysds_ios]
        return {
            'success': True,
            'message': "",
            'result': ids
        }


@hysds_io_ns.route('', endpoint='hysds_io')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Gets list of registered hysds-io specifications and return as JSON.")
class HySDSio(Resource):
    resp_model = api.model('HySDS IO Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result': fields.Raw(required=True, description="HySDS IO Object or ID")
    })

    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

    post_parser = api.parser()
    post_parser.add_argument('spec', required=True, type=dict, help="HySDS IO JSON Object")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        """Gets a HySDS-IO specification by ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'missing parameter: id'}, 400

        hysds_io = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=_id, ignore=404)
        if hysds_io['found'] is False:
            return {'success': False, 'message': ""}, 404

        return {
            'success': True,
            'message': "",
            'result': hysds_io['_source']
        }

    @api.expect(post_parser)
    @api.marshal_with(resp_model)
    def post(self):
        """Add a HySDS IO specification"""
        spec = request.form.get('spec', request.args.get('spec', None))
        if spec is None:
            app.logger.error("spec not specified")
            raise Exception("'spec' must be supplied")

        try:
            obj = json.loads(spec)
            _id = obj['id']
        except (ValueError, KeyError, json.decoder.JSONDecodeError, Exception) as e:
            return {
                'success': False,
                'message': e
            }, 400

        mozart_es.index_document(index=HYSDS_IOS_INDEX, body=obj, id=_id)
        return {
            'success': True,
            'message': "%s added to index %s" % (_id, HYSDS_IOS_INDEX),
            'result': _id
        }

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def delete(self):
        """Remove HySDS IO for the given ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'id parameter not included'}, 400

        mozart_es.delete_by_id(index=HYSDS_IOS_INDEX, id=_id)
        app.logger.info('deleted %s from index: %s' % (_id, HYSDS_IOS_INDEX))

        return {
            'success': True,
            'message': "deleted hysds_io: %s" % _id
        }


@event_ns.route('/add', endpoint='event-add', methods=['POST'])
@api.doc(responses={200: "Success",
                    500: "Event log failed"},
         description="Logs a HySDS custom event")
class AddLogEvent(Resource):
    """Add log event."""

    resp_model = api.model('HySDS Event Log Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.String(required=True, description="HySDS custom event log ID")
    })
    parser = api.parser()
    parser.add_argument('type', required=True, type=str,
                        help="Event type, e.g. aws_autoscaling, verdi_anomalies")
    parser.add_argument('status', required=True, type=str,
                        help="Event status, e.g. spot_termination, docker_daemon_failed")
    parser.add_argument('event', required=True, type=str,
                        help="""Arbitrary JSON event payload, e.g. {} or {
        "ec2_instance_id": "i-07b8989f41ce23880",
        "private_ip": "100.64.134.145",
        "az": "us-west-2a",
        "reservation": "r-02fd006170749a0a8",
        "termination_date": "2015-01-02T15:49:05.571384"
    }""")
    parser.add_argument('tags', required=False, type=str,
                        help='JSON list of tags, e.g. ["dumby", "test_job"]')
    parser.add_argument('hostname', required=False, type=str,
                        help='Event-related hostname, e.g. "job.hysds.net", "192.168.0.1"')

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        """Log HySDS custom event."""

        try:
            #app.logger.info("data: %s %d" % (request.data, len(request.data)))
            #app.logger.info("form: %s" % request.form)
            #app.logger.info("args: %s" % request.args)
            if len(request.data) > 0:
                try:
                    form = json.loads(request.data)
                except Exception as e:
                    raise Exception(
                        "Failed to parse request data. '{0}' is malformed JSON".format(request.data))
            else:
                form = request.form
            event_type = form.get('type', request.args.get('type', None))
            event_status = form.get('status', request.args.get('status', None))
            event = form.get('event', request.args.get('event', '{}'))
            try:
                if event is not None and not isinstance(event, dict):
                    event = json.loads(event)
            except Exception as e:
                raise Exception(
                    "Failed to parse input event. '{0}' is malformed JSON".format(event))
            tags = form.get('tags', request.args.get('tags', None))
            try:
                if tags is not None and not isinstance(tags, list):
                    tags = json.loads(tags)
            except Exception as e:
                raise Exception(
                    "Failed to parse input tags. '{0}' is malformed JSON".format(tags))
            hostname = form.get('hostname', request.args.get('hostname', None))
            app.logger.info("type: %s" % event_type)
            app.logger.info("status: %s" % event_status)
            app.logger.info("event: %s" % event)
            app.logger.info("tags: %s" % tags)
            app.logger.info("hostname: %s" % hostname)
            uuid = log_custom_event(
                event_type, event_status, event, tags, hostname)
        except Exception as e:
            message = "Failed to log custom event. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': '',
                'result': uuid}


@on_demand_ns.route('', endpoint='on-demand')
@api.doc(responses={200: "Success",
                    500: "Execution failed"},
         description="Retrieve on demand jobs")
class OnDemandJobs(Resource):
    """On Demand Jobs API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = api.parser()
    # parser.add_argument('dataset_info', required=True, type=str,
    #                     location='form',  help="HySDS dataset info JSON")

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
@api.doc(responses={200: "Success",
                    500: "Execution failed"},
         description="Retrieve on job params for specific jobs")
class JobParams(Resource):
    """Job Params API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = api.parser()
    # parser.add_argument('dataset_info', required=True, type=str,
    #                     location='form',  help="HySDS dataset info JSON")

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
        documents = mozart_es.search(index=HYSDS_IOS_INDEX, body=query)

        if documents['hits']['total']['value'] == 0:
            error_message = '%s not found' % job_type
            return {
                'success': False,
                'message': error_message
            }, 404

        job_type = documents['hits']['hits'][0]
        job_params = job_type['_source']['params']
        job_params = list(filter(lambda x: x['from'] == 'submitter', job_params))

        return {
            'success': True,
            'submission_type': job_type['_source'].get('submission_type'),
            'hysds_io': job_type['_source']['id'],
            'params': job_params
        }


@user_rule_ns.route('', endpoint='user-rules')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Retrieve on job params for specific jobs")
class UserRules(Resource):
    """User Rules API"""

    def get(self):
        # TODO: add user role and permissions
        _id = request.args.get('id')
        user_rules_index = app.config['USER_RULES_INDEX']

        if _id:
            rule = mozart_es.get_by_id(index=user_rules_index, id=_id, ignore=404)
            if rule['found'] is False:
                return {
                    'success': False,
                    'message': rule['message']
                }, 404
            else:
                rule = {**rule, **rule['_source']}
                return {
                    'success': True,
                    'rule': rule
                }

        user_rules = mozart_es.query(index=user_rules_index)

        parsed_user_rules = []
        for rule in user_rules:
            rule_copy = rule.copy()
            rule_temp = {**rule_copy, **rule['_source']}
            rule_temp.pop('_source')
            parsed_user_rules.append(rule_temp)

        return {
            'success': True,
            'rules': parsed_user_rules
        }

    def post(self):
        user_rules_index = app.config['USER_RULES_INDEX']

        request_data = request.json or request.form
        rule_name = request_data.get('rule_name')
        hysds_io = request_data.get('workflow')
        job_spec = request_data.get('job_spec')
        priority = int(request_data.get('priority', 0))
        query_string = request_data.get('query_string')
        kwargs = request_data.get('kwargs', '{}')
        queue = request_data.get('queue')
        tags = request_data.get('tags', [])

        username = "ops"  # TODO: add user role and permissions, hard coded to "ops" for now

        if not rule_name or not hysds_io or not job_spec or not query_string or not queue:
            missing_params = []
            if not rule_name:
                missing_params.append('rule_name')
            if not hysds_io:
                missing_params.append('workflow')
            if not job_spec:
                missing_params.append('job_spec')
            if not query_string:
                missing_params.append('query_string')
            if not queue:
                missing_params.append('queue')
            return {
                'success': False,
                'message': 'Params not specified: %s' % ', '.join(missing_params),
                'result': None,
            }, 400

        try:
            json.loads(query_string)
        except (ValueError, TypeError) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid elasticsearch query JSON'
            }, 400

        try:
            json.loads(kwargs)
        except (ValueError, TypeError) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid JSON: kwargs'
            }, 400

        # check if rule name already exists
        rule_exists_query = {
            "query": {
                "term": {
                    "rule_name": rule_name
                }
            }
        }
        existing_rules_count = mozart_es.get_count(index=user_rules_index, body=rule_exists_query)
        if existing_rules_count > 0:
            return {
                'success': False,
                'message': 'user rule already exists: %s' % rule_name
            }, 409

        # check if job_type (hysds_io) exists in elasticsearch
        job_type = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
        if job_type['found'] is False:
            return {
                'success': False,
                'message': '%s not found' % hysds_io
            }, 400

        params = job_type['_source']['params']
        is_passthrough_query = check_passthrough_query(params)

        if type(tags) == str:
            tags = [tags]

        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        new_doc = {
            "workflow": hysds_io,
            "job_spec": job_spec,
            "priority": priority,
            "rule_name": rule_name,
            "username": username,
            "query_string": query_string,
            "kwargs": kwargs,
            "job_type": hysds_io,
            "enabled": True,
            "passthru_query": is_passthrough_query,
            "query_all": False,
            "queue": queue,
            "modified_time": now,
            "creation_time": now,
            "tags": tags
        }

        result = mozart_es.index_document(index=user_rules_index, body=new_doc, refresh=True)
        return {
            'success': True,
            'message': 'rule created',
            'result': result
        }

    def put(self):  # TODO: add user role and permissions
        request_data = request.json or request.form

        _id = request_data.get('id')
        if not _id:
            return {
                'result': False,
                'message': 'id not included'
            }, 400

        user_rules_index = app.config['USER_RULES_INDEX']

        rule_name = request_data.get('rule_name')
        hysds_io = request_data.get('workflow')
        job_spec = request_data.get('job_spec')
        priority = request_data.get('priority')
        query_string = request_data.get('query_string')
        kwargs = request_data.get('kwargs')
        queue = request_data.get('queue')
        enabled = request_data.get('enabled')
        tags = request_data.get('tags')

        # check if job_type (hysds_io) exists in elasticsearch (only if we're updating job_type)
        if hysds_io:
            job_type = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
            if job_type['found'] is False:
                return {
                    'success': False,
                    'message': 'job_type not found: %s' % hysds_io
                }, 404

        app.logger.info('finding existing user rule: %s' % _id)
        existing_rule = mozart_es.get_by_id(index=user_rules_index, id=_id, ignore=404)
        if existing_rule['found'] is False:
            app.logger.info('rule not found %s' % _id)
            return {
                'result': False,
                'message': 'user rule not found: %s' % _id
            }, 404

        update_doc = {}
        if rule_name:
            update_doc['rule_name'] = rule_name
        if hysds_io:
            update_doc['workflow'] = hysds_io
            update_doc['job_type'] = hysds_io
        if job_spec:
            update_doc['job_spec'] = job_spec
        if priority:
            update_doc['priority'] = int(priority)
        if query_string:
            update_doc['query_string'] = query_string
            try:
                json.loads(query_string)
            except (ValueError, TypeError) as e:
                app.logger.error(e)
                return {
                    'success': False,
                    'message': 'invalid elasticsearch query JSON'
                }, 400
        if kwargs:
            update_doc['kwargs'] = kwargs
            try:
                json.loads(kwargs)
            except (ValueError, TypeError) as e:
                app.logger.error(e)
                return {
                    'success': False,
                    'message': 'invalid JSON: kwargs'
                }, 400
        if queue:
            update_doc['queue'] = queue
        if enabled is not None:
            update_doc['enabled'] = enabled
        if tags is not None:
            if type(tags) == str:
                tags = [tags]
            update_doc['tags'] = tags
        update_doc['modified_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        app.logger.info('editing document id %s in user_rule index' % _id)

        doc = {
            'doc_as_upsert': True,
            'doc': update_doc
        }

        result = mozart_es.update_document(index=user_rules_index, id=_id, body=doc, refresh=True)
        app.logger.info(result)
        app.logger.info('document updated: %s' % _id)
        return {
            'success': True,
            'id': _id,
            'updated': update_doc
        }

    def delete(self):
        # TODO: need to add user rules and permissions
        user_rules_index = app.config['USER_RULES_INDEX']
        _id = request.args.get('id')
        if not _id:
            return {
                'result': False,
                'message': 'id not included'
            }, 400

        result = mozart_es.delete_by_id(index=user_rules_index, id=_id, ignore=404)
        if result['found'] is False:
            app.logger.error('failed to delete %s from user_rules index' % _id)
            return {
                'success': False,
                'message': 'user rule not found: %s' % _id
            }, 404

        app.logger.info('user rule deleted: %s' % _id)
        return {
            'success': True,
            'message': 'user rule deleted',
            'id': _id
        }


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
        app.logger.info(buckets)
        return {
            'success': True,
            'tags': [tag['key']['tags'] for tag in buckets]
        }
