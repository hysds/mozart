from future import standard_library

standard_library.install_aliases()

import json

from flask import request
from flask_restx import Namespace, Resource, fields

from mozart import app, mozart_es


JOB_SPEC_NS = "job_spec"
job_spec_ns = Namespace(JOB_SPEC_NS, description="Mozart job-specification operations")

CONTAINER_NS = "container"
container_ns = Namespace(CONTAINER_NS, description="Mozart container operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = Namespace(HYSDS_IO_NS, description="HySDS IO operations")


HYSDS_IOS_INDEX = app.config["HYSDS_IOS_INDEX"]
JOB_SPECS_INDEX = app.config["JOB_SPECS_INDEX"]
JOB_STATUS_INDEX = app.config["JOB_STATUS_INDEX"]
CONTAINERS_INDEX = app.config["CONTAINERS_INDEX"]


@job_spec_ns.route("/list", endpoint="job_spec-list")
@job_spec_ns.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Get list of registered job types and return as JSON.",
)
class GetJobTypes(Resource):
    """Get list of registered job types and return as JSON."""

    resp_model_job_types = job_spec_ns.model(
        "Job Type List Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing " + "success or failure"
            ),
            "result": fields.List(
                fields.String, required=True, description="list of job types"
            ),
        },
    )

    @job_spec_ns.marshal_with(resp_model_job_types)
    def get(self):
        """Gets a list of Job Type specifications"""
        query = {"query": {"match_all": {}}}
        job_specs = mozart_es.query(index=JOB_SPECS_INDEX, body=query)
        ids = [job_spec["_id"] for job_spec in job_specs]
        return {"success": True, "message": "", "result": ids}


@job_spec_ns.route("/type", endpoint="job_spec-type")
@job_spec_ns.doc(
    responses={200: "Success", 500: "Queue listing failed"},
    description="Get a full JSON specification of job type from id.",
)
class GetJobSpecType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = job_spec_ns.model(
        "Job Type Specification Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.Raw(required=True, description="Job Type Specification"),
        },
    )
    parser = job_spec_ns.parser()
    parser.add_argument("id", required=True, type=str, help="Job Type ID")

    @job_spec_ns.expect(parser)
    @job_spec_ns.marshal_with(resp_model)
    def get(self):
        """Gets a Job Type specification object for the given ID."""
        _id = request.form.get("id", request.args.get("id", None))
        if _id is None:
            return {"success": False, "message": "missing parameter: id"}, 400

        job_spec = mozart_es.get_by_id(index=JOB_SPECS_INDEX, id=_id, ignore=404)
        if job_spec["found"] is False:
            app.logger.error("job_spec not found %s" % _id)
            return {
                "success": False,
                "message": "Failed to retrieve job_spec: %s" % _id,
            }, 404

        return {
            "success": True,
            "message": "job spec for %s" % _id,
            "result": job_spec["_source"],
        }


@job_spec_ns.route("/add", endpoint="job_spec-add")
@job_spec_ns.doc(
    responses={200: "Success", 500: "Adding JSON failed"},
    description="Adds a job type specification from JSON.",
)
class AddJobSpecType(Resource):
    """Add job spec"""

    resp_model = job_spec_ns.model(
        "Job Type Specification Addition Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.String(
                required=True, description="Job Type Specification ID"
            ),
        },
    )
    parser = job_spec_ns.parser()
    parser.add_argument(
        "spec", required=True, type=str, help="Job Type Specification JSON Object"
    )

    @job_spec_ns.expect(parser)
    @job_spec_ns.marshal_with(resp_model)
    def post(self):
        """Add a Job Type specification JSON object."""
        spec = request.form.get("spec", request.args.get("spec", None))
        if spec is None:
            return {"success": False, "message": "spec object missing"}, 400

        try:
            obj = json.loads(spec)
            _id = obj["id"]
        except (ValueError, KeyError, json.decoder.JSONDecodeError, Exception) as e:
            return {"success": False, "message": e}, 400

        mozart_es.index_document(index=JOB_SPECS_INDEX, body=obj, id=_id)
        return {
            "success": True,
            "message": "job_spec {} added to index {}".format(_id, HYSDS_IOS_INDEX),
            "result": _id,
        }


@job_spec_ns.route("/remove", endpoint="job_spec-remove")
@job_spec_ns.doc(
    responses={200: "Success", 500: "Remove JSON failed"},
    description="Removes a job type specification.",
)
class RemoveJobSpecType(Resource):
    """Remove job spec"""

    resp_model = job_spec_ns.model(
        "Job Type Specification Removal Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
        },
    )
    parser = job_spec_ns.parser()
    parser.add_argument("id", required=True, type=str, help="Job Type Specification ID")

    @job_spec_ns.expect(parser)
    @job_spec_ns.marshal_with(resp_model)
    def get(self):
        """Remove Job Spec for the given ID"""
        _id = request.form.get("id", request.args.get("id", None))
        if _id is None:
            return {"success": False, "message": "id parameter not included"}, 400

        mozart_es.delete_by_id(index=JOB_SPECS_INDEX, id=_id)
        app.logger.info(
            "Deleted job_spec {} from index: {}".format(_id, JOB_SPECS_INDEX)
        )
        return {"success": True, "message": "job spec removed: %s" % _id}


@container_ns.route("/list", endpoint="container-list")
@container_ns.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Get list of registered containers and return as JSON.",
)
class GetContainerTypes(Resource):
    """Get list of registered containers and return as JSON."""

    resp_model_job_types = container_ns.model(
        "Container List Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.List(
                fields.String, required=True, description="list of hysds-io types"
            ),
        },
    )

    @container_ns.marshal_with(resp_model_job_types)
    def get(self):
        """Get a list of containers managed by Mozart"""
        containers = mozart_es.query(index=CONTAINERS_INDEX, _source=False)
        ids = [container["_id"] for container in containers]

        return {"success": True, "message": "", "result": ids}


@container_ns.route("/add", endpoint="container-add")
@container_ns.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Add a new container.",
)
class GetContainerAdd(Resource):
    """Add a container"""

    resp_model = container_ns.model(
        "Container Add Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', "
                + "encountered exception; otherwise no errors "
                + "occurred",
            ),
            "message": fields.String(
                required=True, description="message describing " + "success or failure"
            ),
            "result": fields.String(required=True, description="Container ID"),
        },
    )
    parser = container_ns.parser()
    parser.add_argument("name", required=True, type=str, help="Container Name")
    parser.add_argument("url", required=True, type=str, help="Container URL")
    parser.add_argument("version", required=True, type=str, help="Container Version")
    parser.add_argument("digest", required=True, type=str, help="Container Digest")

    @container_ns.expect(parser)
    @container_ns.marshal_with(resp_model)
    def post(self):
        """Add a container specification to Mozart"""
        name = request.form.get("name", request.args.get("name", None))
        url = request.form.get("url", request.args.get("url", None))
        version = request.form.get("version", request.args.get("version", None))
        digest = request.form.get("digest", request.args.get("digest", None))

        if not all((name, url, version, digest)):
            return {
                "success": False,
                "message": "Parameters (name, url, version, digest) must be supplied",
            }, 400

        container_obj = {"id": name, "digest": digest, "url": url, "version": version}
        mozart_es.index_document(index=CONTAINERS_INDEX, body=container_obj, id=name)

        return {
            "success": True,
            "message": "{} added to index {}".format(name, CONTAINERS_INDEX),
            "result": name,
        }


@container_ns.route("/remove", endpoint="container-remove")
@container_ns.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Remove a container.",
)
class GetContainerRemove(Resource):
    """Remove a container"""

    resp_model = container_ns.model(
        "Container Removal Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
        },
    )
    parser = container_ns.parser()
    parser.add_argument("id", required=True, type=str, help="Container ID")

    @container_ns.expect(parser)
    @container_ns.marshal_with(resp_model)
    def get(self):
        """Remove container based on ID"""
        _id = request.form.get("id", request.args.get("id", None))
        if _id is None:
            return {"success": False, "message": "id must be supplied"}, 400

        mozart_es.delete_by_id(index=CONTAINERS_INDEX, id=_id)
        app.logger.info(
            "Deleted container {} from index: {}".format(_id, CONTAINERS_INDEX)
        )
        return {"success": True, "message": "job_spec deleted: %s" % _id}


@container_ns.route("/info", endpoint="container-info")
@container_ns.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Get info on a container.",
)
class GetContainerInfo(Resource):
    """Info a container"""

    resp_model = container_ns.model(
        "Container Info Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.Raw(required=True, description="Container Info"),
        },
    )
    parser = container_ns.parser()
    parser.add_argument("id", required=True, type=str, help="Container ID")

    @container_ns.expect(parser)
    @container_ns.marshal_with(resp_model)
    def get(self):
        """Get information on container by ID"""
        _id = request.form.get("id", request.args.get("id", None))

        container = mozart_es.get_by_id(index=CONTAINERS_INDEX, id=_id, ignore=404)
        if container["found"] is False:
            return {"success": False, "message": ""}, 404

        return {"success": True, "message": "", "result": container["_source"]}


@hysds_io_ns.route("/list", endpoint="hysds_io-list")
@container_ns.doc(
    responses={200: "Success", 500: "Query execution failed"},
    description="Gets list of registered hysds-io specifications and return as JSON.",
)
class GetHySDSIOTypes(Resource):
    """Get list of registered hysds-io and return as JSON."""

    resp_model_job_types = container_ns.model(
        "HySDS IO List Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.List(
                fields.String, required=True, description="list of hysds-io types"
            ),
        },
    )

    @container_ns.marshal_with(resp_model_job_types)
    def get(self):
        """List HySDS IO specifications"""
        hysds_ios = mozart_es.query(index=HYSDS_IOS_INDEX, _source=False)
        ids = [hysds_io["_id"] for hysds_io in hysds_ios]
        return {"success": True, "message": "", "result": ids}


@hysds_io_ns.route("/type", endpoint="hysds_io-type")
@hysds_io_ns.doc(
    responses={200: "Success", 500: "Queue listing failed"},
    description="Gets info on a hysds-io specification.",
)
class GetHySDSIOType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = hysds_io_ns.model(
        "HySDS IO Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.Raw(required=True, description="HySDS IO Object"),
        },
    )
    parser = hysds_io_ns.parser()
    parser.add_argument("id", required=True, type=str, help="HySDS IO Type ID")

    @hysds_io_ns.expect(parser)
    @hysds_io_ns.marshal_with(resp_model)
    def get(self):
        """Gets a HySDS-IO specification by ID"""
        _id = request.form.get("id", request.args.get("id", None))
        if _id is None:
            return {"success": False, "message": "missing parameter: id"}, 400

        hysds_io = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=_id, ignore=404)
        if hysds_io["found"] is False:
            return {"success": False, "message": "hysds io not found: %s" % _id}, 404

        return {"success": True, "message": "", "result": hysds_io["_source"]}


@hysds_io_ns.route("/add", endpoint="hysds_io-add")
@hysds_io_ns.doc(
    responses={200: "Success", 500: "Adding JSON failed"},
    description="Adds a hysds-io specification",
)
class AddHySDSIOType(Resource):
    """Add job spec"""

    resp_model = hysds_io_ns.model(
        "HySDS IO Addition Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.String(required=True, description="HySDS IO ID"),
        },
    )
    parser = hysds_io_ns.parser()
    parser.add_argument("spec", required=True, type=str, help="HySDS IO JSON Object")

    @hysds_io_ns.expect(parser)
    @hysds_io_ns.marshal_with(resp_model)
    def post(self):
        """Add a HySDS IO specification"""
        spec = request.form.get("spec", request.args.get("spec", None))
        if spec is None:
            app.logger.error("spec not specified")
            raise Exception("'spec' must be supplied")

        try:
            obj = json.loads(spec)
            _id = obj["id"]
        except (ValueError, KeyError, json.decoder.JSONDecodeError, Exception) as e:
            return {"success": False, "message": e}, 400

        mozart_es.index_document(index=HYSDS_IOS_INDEX, body=obj, id=_id)
        return {
            "success": True,
            "message": "{} added to index {}".format(_id, HYSDS_IOS_INDEX),
            "result": _id,
        }


@hysds_io_ns.route("/remove", endpoint="hysds_io-remove")
@hysds_io_ns.doc(
    responses={200: "Success", 500: "Remove JSON failed"},
    description="Removes a hysds-io specification.",
)
class RemoveHySDSIOType(Resource):
    """Remove job spec"""

    resp_model = hysds_io_ns.model(
        "HySDS IO Removal Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception; "
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
        },
    )
    parser = hysds_io_ns.parser()
    parser.add_argument("id", required=True, type=str, help="HySDS IO ID")

    @hysds_io_ns.expect(parser)
    @hysds_io_ns.marshal_with(resp_model)
    def get(self):
        """Remove HySDS IO for the given ID"""
        _id = request.form.get("id", request.args.get("id", None))
        if _id is None:
            return {"success": False, "message": "id parameter not included"}, 400

        mozart_es.delete_by_id(index=HYSDS_IOS_INDEX, id=_id)
        app.logger.info("deleted {} from index: {}".format(_id, HYSDS_IOS_INDEX))

        return {"success": True, "message": "deleted hysds_io: %s" % _id}
