from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

import re
from requests.exceptions import HTTPError
from flask import Blueprint, Response, request
from flask_restx import Api, apidoc, Resource

from subprocess import Popen, PIPE, STDOUT

from jenkins import NotFoundException, JenkinsException

from mozart import app, jenkins_wrapper


services = Blueprint('jenkins', __name__, url_prefix='/api/jenkins')
api = Api(services, ui=False, version="0.1", title="Mozart API",
          description="API for HySDS job submission and building (Jenkins)")

job_registration_ns = api.namespace('register', description="Register Jenkins jobs")
job_builder_ns = api.namespace('build', description="Build Jenkins jobs")

VENUE = app.config['VENUE']
REPO_RE = re.compile(r'.+//.*?/(.*?)/(.*?)(?:\.git)?$')


def get_ci_job_name(repo, branch=None):  # taken from sdscli
    match = REPO_RE.search(repo)
    if not match:
        raise RuntimeError("Failed to parse repo owner and name: %s" % repo)
    owner, name = match.groups()
    if branch is None:
        job_name = "%s_container-builder_%s_%s" % (VENUE, owner, name)
    else:
        job_name = "%s_container-builder_%s_%s_%s" % (VENUE, owner, name, branch)
    return job_name


def execute_cmd(cmd, event_source=False):
    """
    :param cmd: sds ci command
    :param event_source: boolean, if the source of the request uses the EventSource javascript API
    """
    with Popen(cmd, stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True) as p:
        for stdout_line in iter(p.stdout.readline, ''):
            # TODO: this is a workaround to mask the github oauth token, need to find a better way to do this
            if 'GIT_OAUTH_TOKEN' in stdout_line or 'git' in stdout_line:
                continue
            if event_source:
                yield 'data: ' + stdout_line + '\n'
            else:
                yield stdout_line
        p.stdout.close()


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@job_registration_ns.route('', endpoint='register')
@api.doc(responses={200: "Success", 500: "Query execution failed"}, description="Register and build jenkins jobs")
class JobRegistration(Resource):
    """
    sdscli wrapper to register jobs in jenkins

    sds -d ci add_job -b <branch> --token <github link> s3
    """

    def post(self):
        """Register jobs in jenkins"""
        request_data = request.json or request.form
        repo = request_data.get('repo')
        branch = request_data.get('branch')
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        if repo is None:
            return {
                'success': False,
                'message': 'repo must be supplied'
            }, 400

        if branch:
            cmd = 'exec sds -d ci add_job -b %s --token %s s3' % (branch, repo)  # exec
        else:
            cmd = 'exec sds -d ci add_job --token %s s3' % repo
        resp = Response(execute_cmd(cmd), mimetype="text/event-stream")
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers["Access-Control-Allow-Origin"] = '*'
        resp.headers['Content-Type'] = 'text/event-stream'
        resp.headers['Connection'] = 'keep-alive'
        return resp

    def delete(self):
        """
        deletes Jenkins job
        """
        request_data = request.json or request.form
        repo = request_data.get('repo')
        branch = request_data.get('branch')
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        job_name = get_ci_job_name(repo, branch)
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException:
            return {
                'success': False,
                'message': 'jenkins job not found: %s' % job_name
            }, 404

        try:
            jenkins_wrapper.delete_job(job_name)
        except JenkinsException as e:
            app.logger.error(str(e))
            return {
                'success': False,
                'message': 'unable to delete job: %s' % job_name
            }, 500

        return {
            'success': True,
            'message': 'job deleted %s' % job_name
        }


@job_builder_ns.route('', endpoint='build')
@api.doc(responses={200: "Success", 500: "Query execution failed"}, description="Register and build jenkins jobs")
class JobBuilder(Resource):
    """
    Job build management using the Jenkins rest API
    """

    def post(self):
        """build jobs in jenkins (using a regular curl command)"""
        if not app.config.get('JENKINS_ENABLED', False):
            return {
                'success': False,
                'message': 'set JENKINS_ENABLED (settings.cfg) to True to enable this endpoint'
            }, 404

        request_data = request.json or request.form
        repo = request_data.get('repo')
        branch = request_data.get('branch')
        input_params = request_data.get('parameters', {})
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)
        app.logger.info("parameters: %s" % input_params)

        if repo is None:
            return {
                'success': False,
                'message': 'repo must be supplied'
            }, 400

        job_name = get_ci_job_name(repo, branch)
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException:
            return {
                'success': False,
                'message': 'jenkins job not found: %s' % job_name
            }, 404

        job_info = jenkins_wrapper.get_job_info(job_name)
        next_build_number = job_info['nextBuildNumber']

        if job_info['inQueue'] is True:  # check if in queue
            return {
                'success': True,
                'message': 'job is currently in queue: %s (%d)' % (job_name, next_build_number)
            }, 303

        # check if job is running
        if job_info['lastBuild'] is not None:
            build_number = job_info['lastBuild']['number']
            build_info = jenkins_wrapper.get_build_info(job_name, build_number)
            if build_info['building'] is True:
                return {
                    'success': True,
                    'message': 'job is already building: %s (%d)' % (job_name, build_number)
                }, 303

        default_params = {}
        for prop in job_info['property']:
            if prop['_class'] == 'hudson.model.ParametersDefinitionProperty':
                params = prop['parameterDefinitions']
                for param in params:
                    param_name = param['name']
                    default_params[param_name] = param['defaultParameterValue']['value']

        input_params = {**default_params, **input_params}  # combining parameters
        try:
            queue = jenkins_wrapper.build_job(job_name, parameters=input_params)
            app.logger.info('job build submitted %s' % job_name)
            return {
                'success': True,
                'message': 'job successfully submitted to build %s (%d)' % (job_name, next_build_number),
                'queue': queue
            }
        except (HTTPError, Exception) as e:
            app.logger.error(str(e))
            return {
                'success': False,
                'message': 'job failed to submit build %s' % job_name
            }, 400

    def delete(self):
        """
        Maybe we can stop builds through the use of the jenkins rest api
        """
        if not app.config.get('JENKINS_ENABLED', False):
            return {
                'success': False,
                'message': 'set JENKINS_ENABLED in settings.cfg to True to use this functionality'
            }, 404

        request_data = request.json or request.form
        repo = request_data.get('repo')
        branch = request_data.get('branch')
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        job_name = get_ci_job_name(repo, branch)
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException:
            return {
                'success': False,
                'message': 'jenkins job not found: %s' % job_name
            }, 404

        job_info = jenkins_wrapper.get_job_info(job_name)

        if job_info['inQueue'] is True:
            queue_id = job_info['queueItem']['id']
            jenkins_wrapper.cancel_queue(queue_id)
            return {
                'success': True,
                'message': 'job %s removed from queue %d' % (job_name, queue_id)
            }

        # check if job is running
        if job_info['lastBuild']:
            build_number = job_info['lastBuild']['number']
            build_info = jenkins_wrapper.get_build_info(job_name, build_number)
            if build_info['building'] is True:
                jenkins_wrapper.stop_build(job_name, build_number)
                return {
                    'success': True,
                    'message': 'job build stopped: %s' % job_name
                }
            else:  # job is currently not building (or already finished building)
                return {
                    'success': True,
                    'message': 'job is currently not running: %s' % job_name
                }

        return {
            'success': False,
            'message': 'job has no previous and current builds'
        }, 303
