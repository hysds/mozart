from future import standard_library

standard_library.install_aliases()

import os
import re
import requests
from flask import Blueprint, Response, request
from flask_restx import Api, apidoc, Resource

from subprocess import Popen, PIPE, STDOUT
from jenkins import NotFoundException, JenkinsException

from mozart import app, jenkins_wrapper


services = Blueprint("ci", __name__, url_prefix="/api/ci")
api = Api(
    services,
    ui=False,
    version="0.1",
    title="Mozart API",
    description="API for HySDS job submission and building (Jenkins)",
)

job_registration_ns = api.namespace("register", description="Register Jenkins jobs")
build_job_ns = api.namespace("job-builder", description="Build Jenkins jobs")
job_build_ns = api.namespace("build", description="Check Jenkins build status")

VENUE = app.config.get("VENUE", "")
REPO_RE = re.compile(r".+//.*?/(.*?)/(.*?)(?:\.git)?$")


def get_ci_job_name(repo, branch=None):  # taken from sdscli
    match = REPO_RE.search(repo)
    if not match:
        return None
    owner, name = match.groups()
    if branch is None:
        job_name = "{}_container-builder_{}_{}".format(VENUE, owner, name)
    else:
        job_name = "{}_container-builder_{}_{}_{}".format(VENUE, owner, name, branch)
    return job_name


def execute_cmd(cmd, event_source=False):
    """
    :param cmd: sds ci command
    :param event_source: boolean, if the source of the request uses the EventSource javascript API
    """
    with Popen(
        cmd, stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True
    ) as p:
        for stdout_line in iter(p.stdout.readline, ""):
            # TODO: this is a workaround to mask the github oauth token, need to find a better way to do this
            if "GIT_OAUTH_TOKEN" in stdout_line or "git" in stdout_line:
                continue
            if event_source:
                yield "data: " + stdout_line + "\n"
            else:
                yield stdout_line
        p.stdout.close()


@services.route("/doc/", endpoint="api_doc")
def swagger_ui():
    return apidoc.ui_for(api)


@job_registration_ns.route("", endpoint="register")
@api.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="jenkins job registration",
)
class JobRegistration(Resource):
    """
    sdscli wrapper to register jobs in jenkins:
    sds -d ci add_job -b <branch> --token <github link> s3
    """

    parser = job_registration_ns.parser()
    parser.add_argument(
        "repo",
        required=True,
        type=str,
        location="form",
        help="Code repository (Github, etc.)",
    )
    parser.add_argument(
        "branch",
        required=False,
        type=str,
        location="form",
        help="Code repository branch",
    )

    delete_parser = job_registration_ns.parser()
    delete_parser.add_argument(
        "repo", required=True, type=str, help="Code repository (Github, etc.)"
    )
    delete_parser.add_argument(
        "branch", required=False, type=str, help="Code repository branch"
    )

    def get(self):
        """list of all registered Jenkins jobs"""
        jobs = jenkins_wrapper.get_jobs()
        return {
            "success": True,
            "results": [
                job["name"]
                for job in jobs
                if job.get("name") and job["name"].startswith(VENUE)
            ],
        }

    @job_registration_ns.expect(parser)
    def post(self):
        """Register jobs in jenkins"""
        request_data = request.form
        repo = request_data.get("repo")
        branch = request_data.get("branch")

        # if repo is None at this point, the request body may have come in as JSON - see Otello issue #14
        #
        # Note that the python requests module does some magic with json payloads passed as data but which
        # does not set "Content-type:application/json". In this case, the request data is available as
        # request.form above.
        #
        # However, in the event the endpoint is reached by another means (e.g. curl), Content-type must be
        # specified for JSON. In this event, request.form will not be None, but 'repo' and 'branch' will be
        # and the request data is accessed via request.json.
        #
        # It is an error to pass the data as JSON w/o specifying Content-type in all cases
        # besides using python requests as described above. Nothing can be done here for that
        # case.
        #
        if repo is None:
            try:
                request_data = request.json
                repo = request_data.get("repo")
                branch = request_data.get("branch")
            except Exception as e:
                # fall through to empty repo/branch
                app.logger.warning(f"no JSON data found {e}")

        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        if repo is None:
            return {"success": False, "message": "repo must be supplied"}, 400

        if branch:
            cmd = "exec sds -d ci add_job -b {} --token {} s3".format(
                branch, repo
            )  # exec
        else:
            cmd = "exec sds -d ci add_job --token %s s3" % repo
        resp = Response(execute_cmd(cmd), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Content-Type"] = "text/event-stream"
        resp.headers["Connection"] = "keep-alive"
        return resp

    @job_registration_ns.expect(delete_parser)
    def delete(self):
        """deletes Jenkins job"""
        repo = request.args.get("repo")
        branch = request.args.get("branch")
        app.logger.info("repo: %s" % repo)
        app.logger.info("branch: %s" % branch)

        if repo is None:
            return {"success": True, "message": "repo not supplied"}, 400

        job_name = get_ci_job_name(repo, branch)
        if job_name is None:
            return {
                "success": False,
                "message": "Failed to parse repo owner and name: %s" % repo,
            }, 400
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "jenkins job not found: %s" % job_name,
            }, 404

        try:
            jenkins_wrapper.delete_job(job_name)
        except JenkinsException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "unable to delete job: %s" % job_name,
            }, 500

        return {"success": True, "message": "job deleted %s" % job_name}


@build_job_ns.route("/<job_name>", endpoint="build")
@build_job_ns.route("")
@api.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Register and build jenkins jobs",
)
class JobBuilder(Resource):
    """Job build management using the Jenkins rest API (start and stop builds)"""

    parser = build_job_ns.parser()
    parser.add_argument(
        "repo",
        required=False,
        type=str,
        location="form",
        help="Code repository (Github, etc.)",
    )
    parser.add_argument(
        "branch",
        required=False,
        type=str,
        location="form",
        help="Code repository branch",
    )

    arg_parser = build_job_ns.parser()
    arg_parser.add_argument(
        "repo", required=False, type=str, help="Code repository (Github, etc.)"
    )
    arg_parser.add_argument(
        "branch", required=False, type=str, help="Code repository branch"
    )

    @build_job_ns.expect(arg_parser)
    def get(self, job_name=None):
        """Checks if Jenkins job exists"""
        if job_name is None:
            repo = request.args.get("repo")
            branch = request.args.get("branch")
            if repo is None:
                return {
                    "success": False,
                    "message": "repo must be supplied if Jenkins job_name not given",
                }, 400

            app.logger.info("repo: %s" % repo)
            app.logger.info("branch: %s" % branch)
            job_name = get_ci_job_name(repo, branch)
            if job_name is None:
                return {
                    "success": False,
                    "message": "Failed to parse repo owner and name: %s" % repo,
                }, 400

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
            return {"success": True, "message": "job found: %s" % job_name}
        except NotFoundException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "jenkins job not found: %s" % job_name,
            }, 404

    @build_job_ns.expect(parser)
    def post(self, job_name=None):
        """start Jenkins build"""
        request_data = request.form

        if job_name is None:
            repo = request_data.get("repo")
            branch = request_data.get("branch")

            # Check json if repo not found - see JobRegistration.post() above.
            if repo is None:
                try:
                    request_data = request.json
                    repo = request_data.get("repo")
                    branch = request_data.get("branch")
                except Exception as e:
                    # fall through to empty repo/branch
                    app.logger.warning(f"no JSON data found {e}")

            if repo is None:
                return {
                    "success": False,
                    "message": "repo must be supplied if Jenkins job_name not given",
                }, 400

            app.logger.info("repo: %s" % repo)
            app.logger.info("branch: %s" % branch)
            job_name = get_ci_job_name(repo, branch)
            if job_name is None:
                return {
                    "success": False,
                    "message": "Failed to parse repo owner and name: %s" % repo,
                }, 400
        app.logger.info("jenkins job name: %s" % job_name)
        input_params = request_data.get("parameters", {})
        app.logger.info("parameters: %s" % input_params)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "jenkins job not found: %s" % job_name,
            }, 404

        job_info = jenkins_wrapper.get_job_info(job_name)
        next_build_number = job_info["nextBuildNumber"]

        if job_info["inQueue"] is True:  # check if in queue
            return {
                "success": True,
                "message": "job is currently in queue: %s (%d)"
                % (job_name, next_build_number),
            }, 203

        # check if job is running
        if job_info["lastBuild"] is not None:
            build_number = job_info["lastBuild"]["number"]
            build_info = jenkins_wrapper.get_build_info(job_name, build_number)
            if build_info["building"] is True:
                return {
                    "success": True,
                    "message": "job is already building: %s (%d)"
                    % (job_name, build_number),
                }, 203

        default_params = {}
        for prop in job_info["property"]:
            if prop["_class"] == "hudson.model.ParametersDefinitionProperty":
                params = prop["parameterDefinitions"]
                for param in params:
                    param_name = param["name"]
                    default_params[param_name] = param["defaultParameterValue"]["value"]

        input_params = {**default_params, **input_params}  # combining parameters
        try:
            queue = jenkins_wrapper.build_job(job_name, parameters=input_params)
            app.logger.info("job build submitted %s" % job_name)
            return {
                "success": True,
                "message": "job successfully submitted to build %s (%d)"
                % (job_name, next_build_number),
                "queue": queue,
                "build_number": next_build_number,
            }
        except (requests.exceptions.HTTPError, Exception) as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "job failed to submit build %s" % job_name,
            }, 400

    @build_job_ns.expect(arg_parser)
    def delete(self, job_name=None):
        """stop Jenkins build"""
        if job_name is None:
            repo = request.args.get("repo")
            branch = request.args.get("branch")
            if repo is None:
                return {
                    "success": False,
                    "message": "repo must be supplied if Jenkins job_name not given",
                }, 400

            app.logger.info("repo: %s" % repo)
            app.logger.info("branch: %s" % branch)
            job_name = get_ci_job_name(repo, branch)
            if job_name is None:
                return {
                    "success": False,
                    "message": "Failed to parse repo owner and name: %s" % repo,
                }, 400
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "jenkins job not found: %s" % job_name,
            }, 404

        job_info = jenkins_wrapper.get_job_info(job_name)

        if job_info["inQueue"] is True:
            queue_id = job_info["queueItem"]["id"]
            jenkins_wrapper.cancel_queue(queue_id)
            return {
                "success": True,
                "message": "job %s removed from queue %d" % (job_name, queue_id),
            }

        # check if job is running
        if job_info["lastBuild"]:
            build_number = job_info["lastBuild"]["number"]
            build_info = jenkins_wrapper.get_build_info(job_name, build_number)
            if build_info["building"] is True:
                jenkins_wrapper.stop_build(job_name, build_number)
                return {"success": True, "message": "job build stopped: %s" % job_name}
            else:  # job is currently not building (or already finished building)
                return {
                    "success": True,
                    "message": "job is currently not building: %s" % job_name,
                }

        return {
            "success": False,
            "message": "job has no previous and current builds",
        }, 403


@job_build_ns.route("/<job_name>/<int:build_number>", endpoint="status")
@job_build_ns.route("/<job_name>")
@job_build_ns.route("")
class Build(Resource):
    """Checking the job build status for Jenkins jobs (and deleting job builds)"""

    parser = job_build_ns.parser()
    parser.add_argument(
        "repo",
        required=False,
        type=str,
        help="Code repo if job_name is not supplied (Github, etc.)",
    )
    parser.add_argument(
        "branch", required=False, type=str, help="Code repository branch"
    )

    @job_build_ns.expect(parser)
    def get(self, job_name=None, build_number=None):
        """Get current status of Jenkins build"""
        if (
            job_name is None
        ):  # get job_name from request query parameters if not supplied in url
            repo = request.args.get("repo", None)
            branch = request.args.get("branch", None)
            app.logger.info("repo %s" % repo)
            app.logger.info("branch %s" % branch)
            if repo is None:
                return {
                    "success": False,
                    "message": "repo must be supplied if job_name is not given (url parameter)",
                }, 400

            job_name = get_ci_job_name(repo, branch)
            if job_name is None:
                return {
                    "success": False,
                    "message": "Failed to parse repo owner and name: %s" % repo,
                }, 400
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "jenkins job not found: %s" % job_name,
            }, 404

        if build_number is None:
            build_number = request.args.get("build_number")
            if build_number is None:  # get most recent build number if not supplied
                app.logger.info(
                    "build_number not supplied for %s, will look for latest build_number"
                    % job_name
                )
                job_info = jenkins_wrapper.get_job_info(job_name)
                last_build = job_info["lastBuild"]
                if not last_build:
                    return {
                        "success": False,
                        "message": "no builds listed for %s, submit a build" % job_name,
                    }, 404
                build_number = last_build["number"]
            else:
                if build_number.isdigit():
                    build_number = int(build_number)
                else:
                    return {
                        "success": False,
                        "message": "build_number is not an integer",
                    }, 400
        app.logger.info("build number %d" % build_number)

        build_info = jenkins_wrapper.get_build_info(job_name, build_number)

        return {
            "job_name": job_name,
            "build_number": build_number,
            "result": build_info.get("result", None),
            "timestamp": build_info.get("timestamp", None),
            "url": build_info.get("url", None),
            "duration": build_info.get("duration", None),
            "building": build_info.get("building", None),
        }

    @job_build_ns.expect(parser)
    def delete(self, job_name=None, build_number=None):
        """Remove Jenkins build"""
        if (
            job_name is None
        ):  # get job_name from request query parameters if not supplied in url
            repo = request.args.get("repo", None)
            branch = request.args.get("branch", None)
            app.logger.info("repo %s" % repo)
            app.logger.info("branch %s" % branch)
            if repo is None:
                return {
                    "success": False,
                    "message": "repo must be supplied if job_name is not given (url parameter)",
                }, 400

            job_name = get_ci_job_name(repo, branch)
            if job_name is None:
                return {
                    "success": False,
                    "message": "Failed to parse repo owner and name: %s" % repo,
                }, 400
        app.logger.info("jenkins job name: %s" % job_name)

        try:
            job_found = jenkins_wrapper.job_exists(job_name)
            if not job_found:
                raise NotFoundException
        except NotFoundException as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "jenkins job not found: %s" % job_name,
            }, 404

        job_info = jenkins_wrapper.get_job_info(job_name)

        if build_number is None:
            build_number = request.args.get("build_number")
            if build_number is None:  # get most recent build number if not supplied
                app.logger.info(
                    "build_number not supplied for %s, will look for latest build_number"
                    % job_name
                )
                last_build = job_info["lastBuild"]
                if not last_build:
                    return {
                        "success": False,
                        "message": "job has never been built %s, submit a build with /build (POST)"
                        % job_name,
                    }, 404
                build_number = last_build["number"]
            else:
                if build_number.isdigit():
                    build_number = int(build_number)
                else:
                    return {
                        "success": False,
                        "message": "build_number is not an integer",
                    }, 400
        app.logger.info("build number %d" % build_number)

        try:
            build_info = jenkins_wrapper.get_build_info(job_name, build_number)
            if build_info["building"] is True:
                return {
                    "success": False,
                    "message": "job is building: %s (%d), please stop build before removing"
                    % (job_name, build_number),
                }, 400

            """
            python-jenkins delete_build() breaking, so will fallback to using a request in the meantime:
                https://www.mail-archive.com/python-jenkins-developers@lists.launchpad.net/msg00533.html
            curl -u <user>:<token> -X POST https://<jenkins_url>/job/<job_name>/<build_number>/doDelete
            """
            jenkins_host = app.config.get("JENKINS_HOST", "")
            jenkins_user = app.config.get("JENKINS_USER", "")
            jenkins_token = app.config.get("JENKINS_API_KEY", "")
            if not jenkins_host or not jenkins_user or not jenkins_token:
                return {
                    "success": False,
                    "message": "interval server error (missing configuration)",
                }, 500

            base_endpoint = os.path.join(jenkins_host, "job")
            endpoint = "%s/%s/%d/doDelete" % (base_endpoint, job_name, build_number)
            req = requests.post(endpoint, auth=(jenkins_user, jenkins_token))
            req.raise_for_status()
            return {
                "success": True,
                "message": "job build deleted %s (%d)" % (job_name, build_number),
            }
        except (NotFoundException, JenkinsException) as e:
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "job build not found %s (%d)" % (job_name, build_number),
            }, 404
        except (requests.exceptions.HTTPError, Exception) as e:
            app.logger.error(
                "unable to delete job build %s (%d)" % (job_name, build_number)
            )
            app.logger.error(str(e))
            return {
                "success": False,
                "message": "unable to delete job build %s (%d)"
                % (job_name, build_number),
            }, 500
