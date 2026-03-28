#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()

from mozart import app, db

with app.app_context():
    db.create_all()
