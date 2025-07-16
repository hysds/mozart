from future import standard_library

standard_library.install_aliases()

import os
import time
import subprocess
from flask import jsonify, Blueprint


from mozart import app

mod = Blueprint("services/admin", __name__)


@mod.route("/services/admin/clean", methods=["GET"])
def clean():
    """Clean out mozart's mongodb database and rabbitmq queues."""

    ret = {"success": True}

    # clean mozart
    clean_script = os.path.normpath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "..",
            "scripts",
            "demo",
            "clean_mozart.sh",
        )
    )
    app.logger.debug("clean mozart: %s" % clean_script)
    subprocess.call([clean_script], shell=True)

    # sleep while rabbitmq restarts
    time.sleep(5)

    # queue cleaning puccini
    clean_script = os.path.normpath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "..",
            "scripts",
            "demo",
            "queue_clean_puccini.py",
        )
    )
    app.logger.debug("clean puccini: %s" % clean_script)
    subprocess.call([clean_script], shell=True)

    return jsonify(ret)
