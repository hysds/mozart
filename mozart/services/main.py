from future import standard_library

standard_library.install_aliases()
from flask import jsonify, Blueprint

mod = Blueprint("services/main", __name__)


@mod.route("/services")
def index():
    return jsonify(
        {"success": True, "content": "HySDS job orchestration/worker library"}
    )
