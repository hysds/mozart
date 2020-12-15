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
