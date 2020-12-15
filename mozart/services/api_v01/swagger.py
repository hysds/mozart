from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

from flask_restx import apidoc
from mozart.services.api_v01 import api, services


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)
