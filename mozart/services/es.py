from future import standard_library

standard_library.install_aliases()

from flask import jsonify, Blueprint, request

from mozart.lib.job_utils import get_job_info


mod = Blueprint("services/es", __name__)


@mod.route("/job_info", methods=["GET"])
def get_job_info():
    """Return job info json."""

    # get callback, source, and index
    job_id = request.args.get("id", None)
    if job_id is None:
        return jsonify({"success": False, "message": "Job ID was not specified."}), 500

    job_info = get_job_info(job_id)

    return jsonify({"success": False, "result": job_info})
