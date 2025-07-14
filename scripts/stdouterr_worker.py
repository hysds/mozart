#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import json
from pymongo import MongoClient

from leopold.consumer import Consumer, logger


class StdouterrConsumer(Consumer):
    def __init__(
        self,
        amqp_url,
        exchange,
        exchange_type,
        queue,
        routing_key,
        mongodb_url,
        mongodb_name,
    ):
        super().__init__(amqp_url, exchange, exchange_type, queue, routing_key)
        self._mongodb_url = mongodb_url
        self._mongodb_name = mongodb_name
        self._client = MongoClient(self._mongodb_url)
        self._db = self._client[self._mongodb_name]
        self._col = self._db["stdouterr"]
        self._col.ensure_index([("job_id", 1), ("datetime", 1)])

    def execute_callback(self, ch, method, properties, body):
        logger.info("body: %s" % body)

        # save results in mongodb
        self._col.insert(json.loads(body))

    def __del__(self):
        self._client.close()


if __name__ == "__main__":
    amqp_url = "amqp://guest:guest@localhost:5672/%2F"
    exchange = ""
    exchange_type = "direct"
    queue = "stdouterr"
    routing_key = "stdouterr"
    mongodb_url = "mongodb://localhost/"
    mongodb_name = "mozart"
    status_worker = StdouterrConsumer(
        amqp_url, exchange, exchange_type, queue, routing_key, mongodb_url, mongodb_name
    )
    try:
        status_worker.run()
    except KeyboardInterrupt:
        status_worker.stop()
