from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

from flask import Blueprint, Response, request
from flask_restx import Api, apidoc, Resource, fields

from subprocess import Popen, PIPE, STDOUT

from mozart import app


services = Blueprint('jenkins', __name__, url_prefix='/api/jenkins')
api = Api(services, ui=False, version="0.1", title="Mozart API",
          description="API for HySDS job submission and building (Jenkins)")

jenkins_ns = api.namespace('', description="Register and build Jenkins jobs")


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@jenkins_ns.route('/job', endpoint='jenkins')
@api.doc(responses={200: "Success", 500: "Query execution failed"}, description="Register and build jenkins jobs")
class JenkinsJob(Resource):
    """
    sdscli wrapper to register and build jobs and start in jenkins

    sds -d ci add_job -b <branch> --token <github link> s3
    sds -d ci build_job -b <branch> <github link>
    sds -d ci remove_job -b <branch> <github link>
    """

    @staticmethod
    def execute(cmd):
        with Popen(cmd, stdout=PIPE, stderr=STDOUT, universal_newlines=True, bufsize=1, shell=True) as p:
            for stdout_line in iter(p.stdout.readline, ''):
                if 'GIT_OAUTH_TOKEN' not in stdout_line and '@github.com' not in stdout_line:
                    # TODO: this is a workaround to mask the github oauth token, need to find a better way to do this
                    yield 'data: ' + stdout_line + '\n'
            p.stdout.close()

    def put(self):
        """Register jobs in jenkins"""
        request_data = request.json or request.form
        repo = request_data.get('repo')
        branch = request_data.get('branch')
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        if repo is None:
            return {'success': False, 'message': 'repo must be supplied'}, 400

        if branch:
            cmd = 'exec sds -d ci add_job -b %s --token %s s3' % (branch, repo)  # exec
        else:
            cmd = 'exec sds -d ci add_job --token %s s3' % repo
        resp = Response(self.execute(cmd), mimetype="text/event-stream")
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers["Access-Control-Allow-Origin"] = '*'
        resp.headers['Content-Type'] = 'text/event-stream'
        resp.headers['Connection'] = 'keep-alive'
        return resp

    def get(self):
        """build jobs in jenkins (using EventSource javascript api)"""
        repo = request.args.get('repo')
        branch = request.args.get('branch')
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        if repo is None:
            return {'success': False, 'message': 'repo must be supplied'}, 400

        if branch:
            cmd = 'exec sds -d ci build_job -b %s %s' % (branch, repo)  # exec
        else:
            cmd = 'exec sds -d ci build_job %s' % repo
        resp = Response(self.execute(cmd), mimetype="text/event-stream")
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers["Access-Control-Allow-Origin"] = '*'
        resp.headers['Content-Type'] = 'text/event-stream'
        resp.headers['Connection'] = 'keep-alive'
        return resp

    def post(self):
        """build jobs in jenkins (using a regular curl command)"""
        request_data = request.json or request.form
        repo = request_data.get('repo')
        branch = request_data.get('branch')
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        if repo is None:
            return {'success': False, 'message': 'repo must be supplied'}, 400

        if branch:
            cmd = 'exec sds -d ci build_job -b %s %s' % (branch, repo)  # exec
        else:
            cmd = 'exec sds -d ci build_job %s' % repo
        resp = Response(self.execute(cmd), mimetype="text/event-stream")
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers["Access-Control-Allow-Origin"] = '*'
        resp.headers['Content-Type'] = 'text/event-stream'
        resp.headers['Connection'] = 'keep-alive'
        return resp
