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


services = Blueprint('api_v0-1', __name__, url_prefix='/api/v0.1')
api = Api(services, ui=False, version="0.1", title="Mozart API",
          description="Rest API for all job related functionality")


JOB_SPEC_NS = "job_spec"
job_spec_ns = api.namespace(JOB_SPEC_NS, description="Mozart job-specification operations")

CONTAINER_NS = "container"
container_ns = api.namespace(CONTAINER_NS, description="Mozart container operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")

JOB_NS = "job"
job_ns = api.namespace(JOB_NS, description="Mozart job operations")

QUEUE_NS = "queue"
queue_ns = api.namespace(QUEUE_NS, description="Mozart queue operations")

ON_DEMAND_NS = "on-demand"
on_demand_ns = api.namespace(ON_DEMAND_NS, description="For retrieving and submitting on-demand jobs for mozart")

EVENT_NS = "event"
event_ns = api.namespace(EVENT_NS, description="HySDS event stream operations")

USER_TAGS_NS = "user-tags"
user_tags_ns = api.namespace(USER_TAGS_NS, description="user tags for Mozart jobs")

USER_RULES_TAGS = "user-rules-tags"
user_rules_tags_ns = api.namespace(USER_RULES_TAGS, description="user tags for Mozart jobs")

USER_RULE_NS = "user-rules"
user_rule_ns = api.namespace(USER_RULE_NS, description="C.R.U.D. for Mozart user rules")
