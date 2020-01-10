from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()
import os
import sys
import json
import requests
import types
import re
import traceback

from flask import jsonify, Blueprint, request, Response, render_template, make_response
from flask_restplus import Api, apidoc, Resource, fields
from flask_login import login_required

import hysds_commons.container_utils
import hysds_commons.hysds_io_utils
import hysds_commons.job_spec_utils
import hysds_commons.job_utils

from hysds.log_utils import log_custom_event

from mozart import app

# Library backend imports
import mozart.lib.job_utils
import mozart.lib.queue_utils


services = Blueprint('api_v0-2', __name__, url_prefix='/api/v0.2')
api = Api(services, ui=False, version="0.2", title="Mozart API",
          description="API for HySDS job submission and query.")


# Namespace declarations
QUEUE_NS = "queue"
queue_ns = api.namespace(QUEUE_NS, description="Mozart queue operations")

JOBSPEC_NS = "job_spec"
job_spec_ns = api.namespace(
    JOBSPEC_NS, description="Mozart job-specification operations")

JOB_NS = "job"
job_ns = api.namespace(JOB_NS, description="Mozart job operations")

CONTAINER_NS = "container"
container_ns = api.namespace(
    CONTAINER_NS, description="Mozart container operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")

EVENT_NS = "event"
event_ns = api.namespace(EVENT_NS, description="HySDS event stream operations")


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@job_spec_ns.route('/list', endpoint='job_spec-list')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Get list of registered job types and return as JSON.")
class GetJobTypes(Resource):
    """Get list of registered job types and return as JSON."""
    resp_model_job_types = api.model('Job Type List Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.List(fields.String, required=True,
                               description="list of job types")
    })
    @api.marshal_with(resp_model_job_types)
    def get(self):
        '''
        Gets a list of Job Type specifications
        '''
        try:
            ids = hysds_commons.job_spec_utils.get_job_spec_types(
                app.config['ES_URL'], logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for Job types. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ids}


@job_spec_ns.route('/type', endpoint='job_spec-type')
@api.doc(responses={200: "Success",
                    500: "Queue listing failed"},
         description="Get a full JSON specification of job type from id.")
class GetJobSpecType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = api.model('Job Type Specification Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="Job Type Specification")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Job Type ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Gets a Job Type specification object for the given ID.
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            spec = hysds_commons.job_spec_utils.get_job_spec(
                app.config['ES_URL'], ident, logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for Job spec. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': spec}


@job_spec_ns.route('/add', endpoint='job_spec-add')
@api.doc(responses={200: "Success",
                    500: "Adding JSON failed"},
         description="Adds a job type specification from JSON.")
class AddJobSpecType(Resource):
    """Add job spec"""

    resp_model = api.model('Job Type Specification Addition Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.String(required=True, description="Job Type Specification ID")
    })
    parser = api.parser()
    parser.add_argument('spec', required=True, type=str,
                        help="Job Type Specification JSON Object")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        '''
        Add a Job Type specification JSON object.
        '''
        try:
            spec = request.form.get('spec', request.args.get('spec', None))
            if spec is None:
                raise Exception("'spec' must be supplied")
            obj = json.loads(spec)
            ident = hysds_commons.job_spec_utils.add_job_spec(
                app.config['ES_URL'], obj, logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for Job spec. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ident}


@job_spec_ns.route('/remove', endpoint='job_spec-remove')
@api.doc(responses={200: "Success",
                    500: "Remove JSON failed"},
         description="Removes a job type specification.")
class RemoveJobSpecType(Resource):
    """Remove job spec"""

    resp_model = api.model('Job Type Specification Removal Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str,
                        help="Job Type Specification ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Remove Job Type specification for the given ID.
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            hysds_commons.job_spec_utils.remove_job_spec(
                app.config['ES_URL'], ident, logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for Job spec. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': ""}


@queue_ns.route('/list', endpoint='queue-list')
@api.doc(responses={200: "Success",
                    500: "Queue listing failed"},
         description="Get list of available job queues and return as JSON.")
class GetQueueNames(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = api.model('Queue Listing Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="queue response")
    })
    parser = api.parser()
    parser.add_argument('id', required=False, type=str,
                        help="Job Type Specification ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Gets a listing of non-celery queues handling jobs.
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            queues = mozart.lib.queue_utils.get_queue_names(ident)
            app.logger.warn("Queues:"+str(queues))
        except Exception as e:
            message = "Failed to list job queues. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': queues}


@job_ns.route('/submit', endpoint='job-submit')
@api.doc(responses={200: "Success",
                    400: "Invalid parameters",
                    500: "Job submission failed"},
         description="Submit job for execution in HySDS.")
class SubmitJob(Resource):
    """Submit job for execution in HySDS."""

    resp_model = api.model('SubmitJobResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
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
        '''
        Submits a job to run inside HySDS
        '''
        try:
            app.logger.warning(request.form)
            job_type = request.form.get('type', request.args.get('type', None))
            job_queue = request.form.get(
                'queue', request.args.get('queue', None))
            priority = int(request.form.get(
                'priority', request.args.get('priority', 0)))
            tags = request.form.get('tags', request.args.get('tags', None))
            job_name = request.form.get(
                'name', request.args.get('name', None))
            payload_hash = request.form.get(
                'payload_hash', request.args.get('payload_hash', None))
            enable_dedup = str(request.form.get(
                'enable_dedup', request.args.get('enable_dedup', "true")))
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
            job_json = hysds_commons.job_utils.resolve_hysds_job(job_type, job_queue, priority,
                                                                 tags, params,
                                                                 job_name=job_name,
                                                                 payload_hash=payload_hash,
                                                                 enable_dedup=enable_dedup)
            ident = hysds_commons.job_utils.submit_hysds_job(job_json)
        except Exception as e:
            message = "Failed to submit job. {0}:{1}".format(type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': '',
                'result': ident}


@job_ns.route('/status', endpoint='job-status')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Get status of job by ID.")
class GetJobStatus(Resource):
    """Get status of job ID."""

    resp_model = api.model('Job Status Response(JSON)', {
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

    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Job ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Gets the status of a submitted job based on job id
        '''
        try:
            # get job id
            ident = request.form.get('id', request.args.get('id', None))
            status = mozart.lib.job_utils.get_job_status(ident)
        except Exception as e:
            message = "Failed to get job status for {2}. {0}:{1}".format(
                type(e), str(e), ident)
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        # return result
        return {'success': True,
                'message': "",
                'status': status}


@job_ns.route('/list', endpoint='job-list')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Get list of submitted job IDs.")
class GetJobs(Resource):
    """Get list of job IDs."""

    resp_model = api.model('Jobs Listing Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.List(fields.String, required=True,
                               description="list of job IDs")
    })
    parser = api.parser()
    parser.add_argument('page_size', required=False, type=str,
                        help="Job Listing Pagination Size")
    parser = api.parser()
    parser.add_argument('offset', required=False, type=str,
                        help="Job Listing Pagination Offset")

    @api.marshal_with(resp_model)
    def get(self):
        '''
        Paginated list submitted jobs 
        '''
        try:
            page_size = request.form.get(
                'page_size', request.args.get('page_size', 100))
            offset = request.form.get('offset', request.args.get('id', 0))
            jobs = mozart.lib.job_utils.get_job_list(page_size, offset)
        except Exception as e:
            message = "Failed to get job listing(page: {2}, offset: {3}). {0}:{1}".format(
                type(e), str(e), page_size, offset)
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': jobs}


@job_ns.route('/info', endpoint='job-info')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Gets the complete info for a job.")
class GetJobInfo(Resource):
    """Get info of job IDs."""

    resp_model = api.model('Job Info Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="Job Info Object")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Job ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Get complete infor on submitted job based on id
        '''
        try:
            # get job id
            ident = request.form.get('id', request.args.get('id', None))
            info = mozart.lib.job_utils.get_job_info(ident)
        except Exception as e:
            message = "Failed to get job info for {2}. {0}:{1}".format(
                type(e), str(e), ident)
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': info}


@container_ns.route('/list', endpoint='container-list')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Get list of registered containers and return as JSON.")
class GetContainerTypes(Resource):
    """Get list of registered containers and return as JSON."""
    resp_model_job_types = api.model('Container List Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.List(fields.String, required=True,
                               description="list of hysds-io types")
    })
    @api.marshal_with(resp_model_job_types)
    def get(self):
        '''
        Get a list of containers managed by Mozart
        '''
        try:
            ids = hysds_commons.container_utils.get_container_types(
                app.config['ES_URL'], logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for container types. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ids}


@container_ns.route('/add', endpoint='container-add')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Add a new container.")
class GetContainerAdd(Resource):
    """Add a container"""

    resp_model = api.model('Container Add Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.String(required=True, description="Container ID")
    })
    parser = api.parser()
    parser.add_argument('name', required=True, type=str, help="Container Name")
    parser.add_argument('url', required=True, type=str, help="Container URL")
    parser.add_argument('version', required=True,
                        type=str, help="Container Version")
    parser.add_argument('digest', required=True,
                        type=str, help="Container Digest")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        '''
        Add a container specification to Mozart
        '''
        try:
            # get job id
            print((request.form))
            print((request.args))
            name = request.form.get('name', request.args.get('name', None))
            url = request.form.get('url', request.args.get('url', None))
            version = request.form.get(
                'version', request.args.get('version', None))
            digest = request.form.get(
                'digest', request.args.get('digest', None))
            if name is None:
                raise Exception("'name' must be supplied")
            if url is None:
                raise Exception("'url' must be supplied")
            if version is None:
                raise Exception("'version' must be supplied")
            if digest is None:
                raise Exception("'digest' must be supplied")
            ident = hysds_commons.container_utils.add_container(app.config['ES_URL'],
                                                                name, url, version,
                                                                digest, logger=app.logger)
        except Exception as e:
            message = "Failed to add container {2}. {0}:{1}".format(
                type(e), str(e), name)
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ident}


@container_ns.route('/remove', endpoint='container-remove')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Remove a container.")
class GetContainerRemove(Resource):
    """Remove a container"""

    resp_model = api.model('Container Removal Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Container ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Remove container based on ID
        '''
        try:
            # get job id
            ident = request.form.get('id', request.args.get('id', None))
            hysds_commons.container_utils.remove_container(
                app.config['ES_URL'], ident, logger=app.logger)
        except Exception as e:
            message = "Failed to remove container {2}. {0}:{1}".format(
                type(e), str(e), ident)
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': ""}


@container_ns.route('/info', endpoint='container-info')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Get info on a container.")
class GetContainerInfo(Resource):
    """Info a container"""

    resp_model = api.model('Container Info Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="Container Info")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="Container ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Get information on container by ID
        '''
        try:
            # get job id
            ident = request.form.get('id', request.args.get('id', None))
            info = hysds_commons.container_utils.get_container(
                app.config['ES_URL'], ident, logger=app.logger)
        except Exception as e:
            message = "Failed to get info for container {2}. {0}:{1}".format(
                type(e), str(e), ident)
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': info}


@hysds_io_ns.route('/list', endpoint='hysds_io-list')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Gets list of registered hysds-io specifications and return as JSON.")
class GetHySDSIOTypes(Resource):
    """Get list of registered hysds-io and return as JSON."""
    resp_model_job_types = api.model('HySDS IO List Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.List(fields.String, required=True,
                               description="list of hysds-io types")
    })
    @api.marshal_with(resp_model_job_types)
    def get(self):
        '''
        List HySDS IO specifications
        '''
        try:
            ids = hysds_commons.hysds_io_utils.get_hysds_io_types(
                app.config["ES_URL"], logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for HySDS IO types. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ids}


@hysds_io_ns.route('/type', endpoint='hysds_io-type')
@api.doc(responses={200: "Success",
                    500: "Queue listing failed"},
         description="Gets info on a hysds-io specification.")
class GetHySDSIOType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = api.model('HySDS IO Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="HySDS IO Object")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Gets a HySDS-IO specification by ID
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            spec = hysds_commons.hysds_io_utils.get_hysds_io(
                app.config["ES_URL"], ident, logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for HySDS IO object. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': spec}


@hysds_io_ns.route('/add', endpoint='hysds_io-add')
@api.doc(responses={200: "Success",
                    500: "Adding JSON failed"},
         description="Adds a hysds-io specification")
class AddHySDSIOType(Resource):
    """Add job spec"""

    resp_model = api.model('HySDS IO Addition Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.String(required=True, description="HySDS IO ID")
    })
    parser = api.parser()
    parser.add_argument('spec', required=True, type=str,
                        help="HySDS IO JSON Object")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        '''
        Add a HySDS IO specification
        '''
        try:
            spec = request.form.get('spec', request.args.get('spec', None))
            if spec is None:
                raise Exception("'spec' must be supplied")
            obj = json.loads(spec)
            ident = hysds_commons.hysds_io_utils.add_hysds_io(
                app.config["ES_URL"], obj, logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ident}


@hysds_io_ns.route('/remove', endpoint='hysds_io-remove')
@api.doc(responses={200: "Success",
                    500: "Remove JSON failed"},
         description="Removes a hysds-io specification.")
class RemoveHySDSIOType(Resource):
    """Remove job spec"""

    resp_model = api.model('HySDS IO Removal Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Remove HySDS IO for the given ID
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            hysds_commons.hysds_io_utils.remove_hysds_io(
                app.config["ES_URL"], ident, logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': ""}


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
