from flask import jsonify, Blueprint

from mozart import app

mod = Blueprint('services/main', __name__)

@mod.route('/services')
def index():
    return jsonify({'success': True,
                    'content': "HySDS job orchestration/worker library"})
