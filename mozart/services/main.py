from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from flask import jsonify, Blueprint

mod = Blueprint('services/main', __name__)


@mod.route('/services')
def index():
    return jsonify({
        'success': True,
        'content': "HySDS job orchestration/worker library"
    })
