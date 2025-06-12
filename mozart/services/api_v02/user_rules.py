from future import standard_library

standard_library.install_aliases()

import json
from datetime import datetime

from flask import request
from flask_restx import Namespace, Resource, inputs

from hysds_commons.action_utils import check_passthrough_query

from mozart import app, mozart_es


user_rule_ns = Namespace("user-rules", description="C.R.U.D. for Mozart user rules")

HYSDS_IOS_INDEX = app.config["HYSDS_IOS_INDEX"]


@user_rule_ns.route("", endpoint="user-rules")
@user_rule_ns.doc(
    responses={200: "Success", 500: "Execution failed"},
    description="user rule return status",
)
class UserRules(Resource):
    """User Rules API"""

    parser = user_rule_ns.parser()
    parser.add_argument("id", type=str, help="rule id")
    parser.add_argument(
        "rule_name", type=str, help="rule name (fallback if id is not provided)"
    )

    post_parser = user_rule_ns.parser()
    post_parser.add_argument(
        "rule_name", type=str, required=True, location="form", help="rule name"
    )
    post_parser.add_argument(
        "hysds_io", type=str, required=True, location="form", help="hysds io"
    )
    post_parser.add_argument(
        "job_spec", type=str, required=True, location="form", help="queue"
    )
    post_parser.add_argument(
        "priority",
        type=int,
        required=True,
        location="form",
        help="RabbitMQ job priority (0-9)",
    )
    post_parser.add_argument(
        "query_string",
        type=str,
        required=True,
        location="form",
        help="elasticsearch query",
    )
    post_parser.add_argument(
        "kwargs",
        type=str,
        required=True,
        location="form",
        help="keyword arguments for PGE",
    )
    post_parser.add_argument(
        "queue", type=str, required=True, location="form", help="RabbitMQ job queue"
    )
    post_parser.add_argument(
        "tags", type=list, location="form", help="user defined tags for trigger rule"
    )
    post_parser.add_argument(
        "time_limit", type=int, location="form", help="time limit for PGE job"
    )
    post_parser.add_argument(
        "soft_time_limit", type=int, location="form", help="soft time limit for PGE job"
    )
    post_parser.add_argument(
        "disk_usage",
        type=str,
        location="form",
        help="memory usage required for jon (KB, MB, GB)",
    )
    post_parser.add_argument(
        "enable_dedup",
        type=inputs.boolean,
        location="form",
        help="enable job de-duplication",
    )

    put_parser = user_rule_ns.parser()
    put_parser.add_argument("id", type=str, help="rule id")
    put_parser.add_argument(
        "rule_name", type=str, help="rule name (fallback if id is not provided)"
    )
    put_parser.add_argument("hysds_io", type=str, location="form", help="hysds io")
    put_parser.add_argument("job_spec", type=str, location="form", help="queue")
    put_parser.add_argument(
        "priority", type=int, location="form", help="RabbitMQ job priority (0-9)"
    )
    put_parser.add_argument(
        "query_string", type=str, location="form", help="elasticsearch query"
    )
    put_parser.add_argument(
        "kwargs", type=str, location="form", help="keyword arguments for PGE"
    )
    put_parser.add_argument(
        "queue", type=str, location="form", help="RabbitMQ job queue"
    )
    put_parser.add_argument(
        "tags", type=list, location="form", help="user defined tags for trigger rule"
    )
    put_parser.add_argument(
        "time_limit", type=int, location="form", help="time limit for PGE job"
    )
    put_parser.add_argument(
        "soft_time_limit", type=int, location="form", help="soft time limit for PGE job"
    )
    put_parser.add_argument(
        "disk_usage",
        type=str,
        location="form",
        help="memory usage required for jon (KB, MB, GB)",
    )
    put_parser.add_argument(
        "enable_dedup",
        type=inputs.boolean,
        location="form",
        help="enable job de-duplication",
    )

    @user_rule_ns.expect(parser)
    def get(self):
        """Retrieve user rule(s)"""
        _id = request.args.get("id", None)
        _rule_name = request.args.get("rule_name", None)
        user_rules_index = app.config["USER_RULES_INDEX"]

        if _id:
            rule = mozart_es.get_by_id(index=user_rules_index, id=_id, ignore=404)
            if rule.get("found", False) is False:
                return {"success": False, "message": rule["message"]}, 404
            else:
                rule = {**rule, **rule["_source"]}
                rule.pop("_source", None)
                return {"success": True, "rule": rule}
        elif _rule_name:
            result = mozart_es.search(
                index=user_rules_index, q=f"rule_name:{_rule_name}", ignore=404
            )
            if result.get("hits", {}).get("total", {}).get("value", 0) == 0:
                return {
                    "success": False,
                    "message": f"rule {_rule_name} not found",
                }, 404
            rule = result.get("hits").get("hits")[0]
            rule = {**rule, **rule["_source"]}
            rule.pop("_source", None)
            return {"success": True, "rule": rule}

        user_rules = mozart_es.query(index=user_rules_index)

        parsed_user_rules = []
        for rule in user_rules:
            rule_copy = rule.copy()
            rule_temp = {**rule_copy, **rule["_source"]}
            rule_temp.pop("_source")
            parsed_user_rules.append(rule_temp)

        return {"success": True, "rules": parsed_user_rules}

    @user_rule_ns.expect(post_parser)
    def post(self):
        """create new user rule"""
        user_rules_index = app.config["USER_RULES_INDEX"]

        request_data = request.json or request.form
        rule_name = request_data.get("rule_name")
        hysds_io = request_data.get("workflow")
        job_spec = request_data.get("job_spec")
        priority = int(request_data.get("priority", 0))
        query_string = request_data.get("query_string")
        kwargs = request_data.get("kwargs", "{}")
        queue = request_data.get("queue")
        tags = request_data.get("tags", [])
        time_limit = request_data.get("time_limit", None)
        soft_time_limit = request_data.get("soft_time_limit", None)
        disk_usage = request_data.get("disk_usage", None)
        enable_dedup = request_data.get("enable_dedup")
        if enable_dedup is not None:
            try:
                enable_dedup = inputs.boolean(enable_dedup)
            except ValueError as e:
                return {"success": False, "message": str(e)}, 400

        username = (
            "ops"  # TODO: add user role and permissions, hard coded to "ops" for now
        )

        if (
            not rule_name
            or not hysds_io
            or not job_spec
            or not query_string
            or not queue
        ):
            missing_params = []
            if not rule_name:
                missing_params.append("rule_name")
            if not hysds_io:
                missing_params.append("workflow")
            if not job_spec:
                missing_params.append("job_spec")
            if not query_string:
                missing_params.append("query_string")
            if not queue:
                missing_params.append("queue")
            return {
                "success": False,
                "message": "Params not specified: %s" % ", ".join(missing_params),
                "result": None,
            }, 400

        if len(rule_name) > 64:
            return {
                "success": False,
                "message": "rule_name needs to be less than 64 characters",
                "result": None,
            }, 400

        try:
            json.loads(query_string)
        except (ValueError, TypeError) as e:
            app.logger.error(e)
            return {
                "success": False,
                "message": "invalid elasticsearch query JSON",
            }, 400

        try:
            json.loads(kwargs)
        except (ValueError, TypeError) as e:
            app.logger.error(e)
            return {"success": False, "message": "invalid JSON: kwargs"}, 400

        # check if rule name already exists
        rule_exists_query = {"query": {"term": {"rule_name": rule_name}}}
        existing_rules_count = mozart_es.get_count(
            index=user_rules_index, body=rule_exists_query
        )
        if existing_rules_count > 0:
            return {
                "success": False,
                "message": "user rule already exists: %s" % rule_name,
            }, 409

        # check if job_type (hysds_io) exists in elasticsearch
        job_type = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
        if job_type["found"] is False:
            return {"success": False, "message": "%s not found" % hysds_io}, 400

        params = job_type["_source"]["params"]
        is_passthrough_query = check_passthrough_query(params)

        if type(tags) == str:
            tags = [tags]

        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        new_doc = {
            "workflow": hysds_io,
            "job_spec": job_spec,
            "priority": priority,
            "rule_name": rule_name,
            "username": username,
            "query_string": query_string,
            "kwargs": kwargs,
            "job_type": hysds_io,
            "enabled": True,
            "passthru_query": is_passthrough_query,
            "query_all": False,
            "queue": queue,
            "modified_time": now,
            "creation_time": now,
            "tags": tags,
        }

        if time_limit and isinstance(time_limit, int):
            if time_limit <= 0 or time_limit > 86400 * 7:
                return {
                    "success": False,
                    "message": "time_limit must be between 0 and 604800 (sec)",
                }, 400
            else:
                new_doc["time_limit"] = time_limit

        if soft_time_limit and isinstance(soft_time_limit, int):
            if soft_time_limit <= 0 or soft_time_limit > 86400 * 7:
                return {
                    "success": False,
                    "message": "soft_time_limit must be between 0 and 604800 (sec)",
                }, 400
            else:
                new_doc["soft_time_limit"] = soft_time_limit

        if disk_usage:
            new_doc["disk_usage"] = disk_usage
        if enable_dedup is not None:
            new_doc["enable_dedup"] = enable_dedup

        result = mozart_es.index_document(
            index=user_rules_index, body=new_doc, refresh=True
        )
        return {"success": True, "message": "rule created", "result": result}

    @user_rule_ns.expect(put_parser)
    def put(self):
        """edit existing user rule"""
        request_data = request.json or request.form
        _id = request_data.get("id", None)
        _rule_name = request_data.get("rule_name", None)

        if not _id and not _rule_name:
            return {
                "success": False,
                "message": "Must specify id or rule_name in the request",
            }, 400

        user_rules_index = app.config["USER_RULES_INDEX"]

        rule_name = request_data.get("rule_name")
        hysds_io = request_data.get("workflow")
        job_spec = request_data.get("job_spec")
        priority = request_data.get("priority")
        query_string = request_data.get("query_string")
        kwargs = request_data.get("kwargs")
        queue = request_data.get("queue")
        enabled = request_data.get("enabled")
        tags = request_data.get("tags")
        time_limit = request_data.get("time_limit", None)
        soft_time_limit = request_data.get("soft_time_limit", None)
        disk_usage = request_data.get("disk_usage", None)
        enable_dedup = request_data.get("enable_dedup")
        if enable_dedup is not None:
            try:
                enable_dedup = inputs.boolean(enable_dedup)
            except ValueError as e:
                return {"success": False, "message": str(e)}, 400

        # check if job_type (hysds_io) exists in elasticsearch (only if we're updating job_type)
        if hysds_io:
            job_type = mozart_es.get_by_id(
                index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404
            )
            if job_type.get("found", False) is False:
                return {
                    "success": False,
                    "message": "job_type not found: %s" % hysds_io,
                }, 404

        if _id:
            app.logger.info("finding existing user rule: %s" % _id)
            existing_rule = mozart_es.get_by_id(
                index=user_rules_index, id=_id, ignore=404
            )
            if existing_rule.get("found", False) is False:
                app.logger.info("rule not found %s" % _id)
                return {
                    "result": False,
                    "message": "user rule not found: %s" % _id,
                }, 404
        elif _rule_name:
            app.logger.info("finding existing user rule: %s" % _rule_name)
            result = mozart_es.search(
                index=user_rules_index, q=f"rule_name:{_rule_name}", ignore=404
            )
            if result.get("hits", {}).get("total", {}).get("value", 0) == 0:
                return {
                    "success": False,
                    "message": "rule %s not found" % _rule_name,
                }, 404
            else:
                _id = result.get("hits").get("hits")[0].get("_id")

        update_doc = {}
        if rule_name:
            if len(rule_name) > 64:
                return {
                    "success": False,
                    "message": "rule_name needs to be less than 64 characters",
                    "result": None,
                }, 400
            update_doc["rule_name"] = rule_name
        if hysds_io:
            update_doc["workflow"] = hysds_io
            update_doc["job_type"] = hysds_io
        if job_spec:
            update_doc["job_spec"] = job_spec
        if priority:
            update_doc["priority"] = int(priority)
        if query_string:
            update_doc["query_string"] = query_string
            try:
                json.loads(query_string)
            except (ValueError, TypeError) as e:
                app.logger.error(e)
                return {
                    "success": False,
                    "message": "invalid elasticsearch query JSON",
                }, 400
        if kwargs:
            update_doc["kwargs"] = kwargs
            try:
                json.loads(kwargs)
            except (ValueError, TypeError) as e:
                app.logger.error(e)
                return {"success": False, "message": "invalid JSON: kwargs"}, 400
        if queue:
            update_doc["queue"] = queue
        if enabled is not None:
            if isinstance(enabled, str):
                if enabled.lower() == "false":
                    value = False
                else:
                    value = True
                update_doc["enabled"] = value
            else:
                update_doc["enabled"] = enabled
        if tags is not None:
            if type(tags) == str:
                tags = [tags]
            update_doc["tags"] = tags
        update_doc["modified_time"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        if "time_limit" in request_data:  # if submitted in editor
            if time_limit is None:
                update_doc["time_limit"] = None
            else:
                if isinstance(time_limit, int) and 0 < time_limit <= 86400 * 7:
                    update_doc["time_limit"] = time_limit
                else:
                    return {
                        "success": False,
                        "message": "time_limit must be between 0 and 604800 (sec)",
                    }, 400

        if "soft_time_limit" in request_data:  # if submitted in editor
            if soft_time_limit is None:
                update_doc["soft_time_limit"] = None
            else:
                if (
                    isinstance(soft_time_limit, int)
                    and 0 < soft_time_limit <= 86400 * 7
                ):
                    update_doc["soft_time_limit"] = soft_time_limit
                else:
                    return {
                        "success": False,
                        "message": "time_limit must be between 0 and 604800 (sec)",
                    }, 400

        if "disk_usage" in request_data:
            update_doc["disk_usage"] = disk_usage
        if "enable_dedup" in request_data:
            update_doc["enable_dedup"] = enable_dedup

        app.logger.info("editing document id %s in user_rule index" % _id)

        doc = {"doc_as_upsert": True, "doc": update_doc}

        result = mozart_es.update_document(
            index=user_rules_index, id=_id, body=doc, refresh=True
        )
        app.logger.info(result)
        app.logger.info("document updated: %s" % _id)
        return {"success": True, "id": _id, "updated": update_doc}

    @user_rule_ns.expect(parser)
    def delete(self):
        """remove user rule"""
        user_rules_index = app.config["USER_RULES_INDEX"]
        _id = request.args.get("id", None)
        _rule_name = request.args.get("rule_name", None)

        if not _id and not _rule_name:
            return {
                "success": False,
                "message": "Must specify id or rule_name in the request",
            }, 400

        if _id:
            mozart_es.delete_by_id(index=user_rules_index, id=_id, ignore=404)
            app.logger.info("user rule %s deleted" % _id)
            return {"success": True, "message": "user rule deleted", "id": _id}
        elif _rule_name:
            query = {"query": {"match": {"rule_name": _rule_name}}}
            mozart_es.es.delete_by_query(index=user_rules_index, body=query, ignore=404)
            app.logger.info("user rule %s deleted" % _rule_name)
            return {
                "success": True,
                "message": "user rule deleted",
                "rule_name": _rule_name,
            }
