from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

from flask import Blueprint
from flask_restx import Api

from mozart.services.api_v01.builder import job_spec_ns, container_ns, hysds_io_ns
from mozart.services.api_v01.events import event_ns
from mozart.services.api_v01.jobs import job_ns, queue_ns, on_demand_ns
from mozart.services.api_v01.tags import user_tags_ns, user_rules_tags_ns
from mozart.services.api_v01.user_rules import user_rule_ns


services = Blueprint('api_v0-1', __name__, url_prefix='/api/v0.1')
api = Api(services, ui=False, version="0.1", title="Mozart API",
          description="Rest API for all job related functionality")

# builder.py
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
