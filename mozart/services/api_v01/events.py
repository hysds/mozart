from future import standard_library

standard_library.install_aliases()

import json
import traceback

from flask import request
from flask_restx import Namespace, Resource, fields

from hysds.log_utils import log_custom_event

from mozart import app


EVENT_NS = "event"
event_ns = Namespace(EVENT_NS, description="HySDS event stream operations")


@event_ns.route("/add", endpoint="event-add", methods=["POST"])
@event_ns.doc(
    responses={200: "Success", 500: "Event log failed"},
    description="Logs a HySDS custom event",
)
class AddLogEvent(Resource):
    """Add log event."""

    resp_model = event_ns.model(
        "HySDS Event Log Response(JSON)",
        {
            "success": fields.Boolean(
                required=True,
                description="if 'false', encountered exception;"
                "otherwise no errors occurred",
            ),
            "message": fields.String(
                required=True, description="message describing success or failure"
            ),
            "result": fields.String(
                required=True, description="HySDS custom event log ID"
            ),
        },
    )
    parser = event_ns.parser()
    parser.add_argument(
        "type",
        required=True,
        type=str,
        help="Event type, e.g. aws_autoscaling, verdi_anomalies",
    )
    parser.add_argument(
        "status",
        required=True,
        type=str,
        help="Event status, e.g. spot_termination, docker_daemon_failed",
    )
    parser.add_argument(
        "event",
        required=True,
        type=str,
        help="""Arbitrary JSON event payload, e.g. {} or {
                            "ec2_instance_id": "i-07b8989f41ce23880",
                            "private_ip": "100.64.134.145",
                            "az": "us-west-2a",
                            "reservation": "r-02fd006170749a0a8",
                            "termination_date": "2015-01-02T15:49:05.571384"
                        }""",
    )
    parser.add_argument(
        "tags",
        required=False,
        type=str,
        help='JSON list of tags, e.g. ["dumby", "test_job"]',
    )
    parser.add_argument(
        "hostname",
        required=False,
        type=str,
        help='Event-related hostname, e.g. "job.hysds.net", "192.168.0.1"',
    )

    @event_ns.expect(parser)
    @event_ns.marshal_with(resp_model)
    def post(self):
        """Log HySDS custom event."""

        try:
            if len(request.data) > 0:
                try:
                    form = json.loads(request.data)
                except Exception as e:
                    app.logger.error(e)
                    raise Exception(
                        f"Failed to parse request data. '{request.data}' is malformed JSON"
                    )
            else:
                form = request.form

            event_type = form.get("type", request.args.get("type", None))
            event_status = form.get("status", request.args.get("status", None))
            event = form.get("event", request.args.get("event", "{}"))
            try:
                if event is not None and not isinstance(event, dict):
                    event = json.loads(event)
            except Exception:
                raise Exception(
                    f"Failed to parse input event. '{event}' is malformed JSON"
                )

            tags = form.get("tags", request.args.get("tags", None))
            try:
                if tags is not None and not isinstance(tags, list):
                    tags = json.loads(tags)
            except Exception:
                raise Exception(
                    f"Failed to parse input tags. '{tags}' is malformed JSON"
                )

            hostname = form.get("hostname", request.args.get("hostname", None))
            app.logger.info("type: %s" % event_type)
            app.logger.info("status: %s" % event_status)
            app.logger.info("event: %s" % event)
            app.logger.info("tags: %s" % tags)
            app.logger.info("hostname: %s" % hostname)
            uuid = log_custom_event(event_type, event_status, event, tags, hostname)

        except Exception as e:
            message = f"Failed to log custom event. {type(e)}:{str(e)}"
            app.logger.warning(message)
            app.logger.warning("".join(traceback.format_exception(e)))
            return {"success": False, "message": message}, 500

        return {"success": True, "message": "", "result": uuid}
