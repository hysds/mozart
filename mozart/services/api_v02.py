from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
import traceback
from datetime import datetime

from flask import Blueprint, request

from hysds.celery import app as celery_app
from hysds.task_worker import do_submit_task
from flask_restx import Api, apidoc, Resource, fields

import hysds_commons.job_utils
from hysds_commons.action_utils import check_passthrough_query

from mozart import app, mozart_es

# Library backend imports
import mozart.lib.job_utils
import mozart.lib.queue_utils


services = Blueprint('api_v0-2', __name__, url_prefix='/api/v0.2')
api = Api(services, ui=False, version="0.2", title="Mozart API", description="API for HySDS job submission and query.")

# Namespace declarations
# QUEUE_NS = "queue"
# queue_ns = api.namespace(QUEUE_NS, description="Mozart queue operations")

# JOB_SPEC_NS = "job_spec"
# job_spec_ns = api.namespace(JOB_SPEC_NS, description="Mozart job-specification operations")

# JOB_NS = "job"
# job_ns = api.namespace(JOB_NS, description="Mozart job operations")

# CONTAINER_NS = "container"
# container_ns = api.namespace(CONTAINER_NS, description="Mozart container operations")

# HYSDS_IO_NS = "hysds_io"
# hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")

# EVENT_NS = "event"
# event_ns = api.namespace(EVENT_NS, description="HySDS event stream operations")

# ON_DEMAND_NS = "on-demand"
# on_demand_ns = api.namespace(ON_DEMAND_NS, description="For retrieving and submitting on-demand jobs for mozart")

# USER_RULE_NS = "user-rules"
# user_rule_ns = api.namespace(USER_RULE_NS, description="C.R.U.D. for Mozart user rules")

# USER_TAGS_NS = "user-tags"
# user_tags_ns = api.namespace(USER_TAGS_NS, description="user tags for Mozart jobs")

# USER_RULES_TAGS = "user-rules-tags"
# user_rules_tags_ns = api.namespace(USER_RULES_TAGS, description="user tags for Mozart jobs")

HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
JOB_SPECS_INDEX = app.config['JOB_SPECS_INDEX']
JOB_STATUS_INDEX = app.config['JOB_STATUS_INDEX']
CONTAINERS_INDEX = app.config['CONTAINERS_INDEX']


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)
