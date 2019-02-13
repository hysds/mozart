#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from builtins import str
from builtins import input
from future import standard_library
standard_library.install_aliases()
import sys
import pprint
import os
import json
from subprocess import Popen, PIPE

from pika import BasicProperties
from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters

CONNECTION = None
COUNT = 0


def handle_delivery(channel, method_frame, body):
    global COUNT
    j = json.loads(body)
    # pprint.pprint(j)
    if 'queue_name' in j:
        queue_name = str(j["queue_name"])
    elif 'job' in j and 'job_info' in j['job'] and 'job_queue' in j['job']['job_info']:
        queue_name = str(j['job']['job_info']['job_queue'])
    else:
        queue_name = "<unknown>"
    print(("#" * 80))
    print(("queue: %s" % queue_name))
    print(("delivery-tag: %i" % method_frame.delivery_tag))
    print(("-" * 80))
    print(("error: %s" % j["error"]))
    print(("traceback: %s" % j["traceback"]))

    # ask for an action
    while True:
        print("Please select an action:")
        print("1. Print body of message")
        print(("2. Move message back on %s queue" % queue_name))
        print("3. Remove message from error queue")
        print("4. Do nothing")
        print("\nPress CTRL-C to quit.")
        option = eval(input("Select [1,2,3,4] "))
        if option == '1':
            print("body:")
            try:
                pprint.pprint(json.loads(j['body']))
            except:
                pprint.pprint(j)
        elif option == '2':
            channel2 = CONNECTION.channel()
            channel2.queue_declare(queue=queue_name, durable=True)
            channel2.basic_publish(exchange='',
                                   routing_key=queue_name,
                                   body=j['body'],
                                   properties=BasicProperties(
                                       delivery_mode=2,  # make message persistent
                                   ))

            # Acknowledge the message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            break
        elif option == '3':
            # Acknowledge the message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            break
        elif option == '4':
            break

    # quit if no more
    COUNT -= 1
    if COUNT == 0:
        return True
    return False


if __name__ == '__main__':

    # get message count on error queue
    pop = Popen(["sudo", "rabbitmqctl", "list_queues"],
                stdin=PIPE, stdout=PIPE, stderr=PIPE, env=os.environ)
    try:
        sts = pop.wait()  # wait for child to terminate and get status
    except Exception as e:
        print((str(e)))
    status = pop.returncode
    # print "returncode is:",status
    stdOut = pop.stdout.read()
    stdErr = pop.stderr.read()
    for line in stdOut.split('\n'):
        if line.startswith("error_queue"):
            COUNT = int(line.split()[1])
            break
    print(("Total number of messages in error_queue:", COUNT))
    if COUNT == 0:
        sys.exit()

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    CONNECTION = BlockingConnection(ConnectionParameters(host))

    # Open the channel
    channel = CONNECTION.channel()

    # loop
    for method_frame, properties, body in channel.consume('error_queue'):
        if handle_delivery(channel, method_frame, body):
            break

    channel.stop_consuming()
    channel.close()
    CONNECTION.close()
