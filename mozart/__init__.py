from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import os
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_cors import CORS  # TODO: will remove this once we figure out the proper host for the UI

from hysds_commons.elasticsearch_utils import ElasticsearchUtility
from jenkins import Jenkins


class ReverseProxied(object):
    """
    Wrap the application in this middleware and configure the 
    front-end server to add these headers, to let you quietly bind 
    this to a URL other than / and to an HTTP scheme that is 
    different than what is used locally.

    In nginx:
        location /myprefix {
            proxy_pass http://127.0.0.1:8888;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Scheme $scheme;
            proxy_set_header X-Script-Name /myprefix;
        }

    In apache:
        RewriteEngine on
        RewriteRule "^/mozart$" "/mozart/" [R]
        SSLProxyEngine on
        ProxyRequests Off
        ProxyPreserveHost Off
        ProxyPass /mozart/static !
        ProxyPass /mozart/ http://localhost:8888/
        ProxyPassReverse /mozart/ http://localhost:8888/
        <Location /mozart>
            Header add "X-Script-Name" "/mozart"
            RequestHeader set "X-Script-Name" "/mozart"
            Header add "X-Scheme" "https"
            RequestHeader set "X-Scheme" "https"
        </Location>
        Alias /mozart/static/ /home/ops/mozart/ops/mozart/mozart/static/
        <Directory /home/ops/mozart/ops/mozart/mozart/static>
            Options Indexes FollowSymLinks
            AllowOverride All
            Require all granted
        </Directory>

    :param app: the WSGI application
    """

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        x_forwarded_host = environ.get('HTTP_X_FORWARDED_HOST', '')
        if x_forwarded_host:
            environ['HTTP_HOST'] = x_forwarded_host
        return self.app(environ, start_response)


def resource_not_found(e):
    return jsonify({
        'status_code': 404,
        'message': str(e)
    }), 404


app = Flask(__name__)
app.wsgi_app = ReverseProxied(app.wsgi_app)
app.config.from_pyfile('../settings.cfg')

# TODO: will remove this when ready for actual release, need to figure out the right host
CORS(app)

# TODO: may remove this (and any code related to User models and authentication) once SSO is integrated
# set database config
dbdir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(dbdir, 'app.db')
db = SQLAlchemy(app)

# set user auth config
lm = LoginManager()
lm.init_app(app)
lm.login_view = 'views/main.login'

# handle global errors
app.register_error_handler(404, resource_not_found)

# Mozart's connection to Elasticsearch
mozart_es = ElasticsearchUtility(app.config['ES_URL'], app.logger)

# add jenkins connection
if app.config['JENKINS_HOST']:
    jenkins_wrapper = Jenkins(app.config['JENKINS_HOST'], username=app.config['JENKINS_USER'],
                              password=app.config['JENKINS_API_KEY'])
else:
    jenkins_wrapper = None

# views blueprints
from mozart.views.main import mod as views_module
app.register_blueprint(views_module)

# services blueprints
from mozart.services.main import mod as main_module
app.register_blueprint(main_module)

from mozart.services.jobs import mod as jobs_module
app.register_blueprint(jobs_module)

from mozart.services.admin import mod as admin_module
app.register_blueprint(admin_module)

from mozart.services.es import mod as es_module
app.register_blueprint(es_module)

from mozart.services.stats import mod as stats_module
app.register_blueprint(stats_module)

# rest API blueprints
from mozart.services.api_v01.service import services as api_v01_services
app.register_blueprint(api_v01_services)

from mozart.services.api_v02.service import services as api_v02_services
app.register_blueprint(api_v02_services)

from mozart.services.ci import services as ci_services
app.register_blueprint(ci_services)
