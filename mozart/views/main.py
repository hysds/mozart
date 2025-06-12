from future import standard_library

standard_library.install_aliases()

# from datetime import datetime
# import hashlib
# import simpleldap
from flask import Blueprint, redirect, url_for

from mozart import lm
from mozart.models.user import User

# from mozart.lib.job_utils import get_execute_nodes
# from mozart.lib.forms import LoginForm
# from mozart.lib.ldap import ldap_user_verified


mod = Blueprint("views/main", __name__)


@lm.user_loader
def load_user(username):
    return User.query.get(username)


@mod.route("/")
def index():
    return redirect(url_for("api_v0-1.api_doc"))
