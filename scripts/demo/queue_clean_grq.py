#!/usr/bin/env python
import json, logging
from pika import BasicProperties
from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters


log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger('queue_puccini_restart')
logger.setLevel(logging.INFO)


def main():
    queue_name = "grq_clean"
    host = 'localhost'
    conn = BlockingConnection(ConnectionParameters('localhost'))
    channel = conn.channel()
    channel.queue_declare(queue_name, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=json.dumps({'clean': True}),
                          properties=BasicProperties(delivery_mode=2))
    channel.close()
    conn.close()


if __name__ == "__main__": main()
