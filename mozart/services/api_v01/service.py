from future import standard_library

standard_library.install_aliases()

from flask import Blueprint
from flask_restx import Api, apidoc

from mozart.services.api_v01.specs import job_spec_ns, container_ns, hysds_io_ns
from mozart.services.api_v01.events import event_ns
from mozart.services.api_v01.jobs import job_ns, queue_ns, on_demand_ns
from mozart.services.api_v01.tags import user_tags_ns, user_rules_tags_ns
from mozart.services.api_v01.user_rules import user_rule_ns


services = Blueprint("api_v0-1", __name__, url_prefix="/api/v0.1")
api = Api(
    services,
    ui=False,
    version="0.1",
    title="Mozart API",
    description="Rest API for HySDS job related functionality",
)

# specs.py
api.add_namespace(job_spec_ns)
api.add_namespace(container_ns)
api.add_namespace(hysds_io_ns)

# events.py
api.add_namespace(event_ns)

# jobs.py
api.add_namespace(job_ns)
api.add_namespace(queue_ns)
api.add_namespace(on_demand_ns)

# tags.py
api.add_namespace(user_tags_ns)
api.add_namespace(user_rules_tags_ns)

# user_rules.py
api.add_namespace(user_rule_ns)


@services.route("/doc/", endpoint="api_doc")
def swagger_ui():
    return apidoc.ui_for(api)
