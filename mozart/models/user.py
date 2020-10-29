from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()
from mozart import db


ROLE_USER = 0
ROLE_ADMIN = 1


# TODO: may remove this because authentication/authorization will be handled by SSO/keycloak
class User(db.Model):
    id = db.Column(db.String(64), primary_key=True)
    ldap_info = db.Column(db.PickleType)
    role = db.Column(db.SmallInteger, default=ROLE_USER)

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return str(self.id)

    def __repr__(self):
        return '<User %r>' % (self.id)
