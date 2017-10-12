#!/usr/bin/env python
import sys, pprint, os, json
from subprocess import Popen, PIPE

from pika import BasicProperties
from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters

CONNECTION = None
COUNT = 0
QUEUE_NAME = None


def usage():
    """Print usage."""

    print "Usage: %s <host> <queue_name>" % sys.argv[0]
    print "e.g. %s localhost product_queue" % sys.argv[0]
    sys.exit(1)

def handle_delivery(channel, method_frame, header_frame, body):
    global COUNT, QUEUE_NAME
    queue_name = QUEUE_NAME

    try:
        j = json.loads(body)
        body = json.dumps(j, indent=2)
    except: pass
 
    print "#" * 80
    print "queue: %s" % queue_name
    print "delivery-tag: %i" % method_frame.delivery_tag
    print "-" * 80
    print "body: %s" % body

    #ask for an action
    while COUNT > 0:
        print "Please select an action:"
        print "1. Print body of message"
        print "2. Remove message from %s queue" % queue_name
        print "3. Leave message on %s queue" % queue_name
        print "\nPress CTRL-C to quit."
        option = raw_input("Select [1,2,3] ")
        if option == '1': print "body: %s" % body
        elif option == '2':
            # Acknowledge the message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            COUNT -= 1
            break
        elif option == '3':
            COUNT -= 1
            break

    #quit if no more
    if COUNT == 0:
        channel.stop_consuming()
        CONNECTION.close()
        sys.exit()

if __name__ == '__main__':

    # gst host and queue name
    try:
        host = sys.argv[1]
        QUEUE_NAME = sys.argv[2]
    except: usage()

    # get message count on queue
    pop = Popen(["sudo", "rabbitmqctl", "list_queues"], 
                stdin=PIPE, stdout=PIPE, stderr=PIPE, env=os.environ)
    try: sts = pop.wait()  #wait for child to terminate and get status
    except Exception, e: print str(e)
    status = pop.returncode
    #print "returncode is:",status
    stdOut = pop.stdout.read()
    stdErr = pop.stderr.read()
    for line in stdOut.split('\n'):
        if line.startswith(QUEUE_NAME):
            COUNT = int(line.split()[1])
            break
    print "Total number of messages in %s: %d" % (QUEUE_NAME, COUNT)
    if COUNT == 0: sys.exit()

    # Connect to RabbitMQ
    CONNECTION = BlockingConnection(ConnectionParameters(host))

    # Open the channel
    channel = CONNECTION.channel()

    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME,
                          durable=True,
                          exclusive=False,
                          auto_delete=False)

    # Add a queue to consume
    channel.basic_consume(handle_delivery, queue=QUEUE_NAME)

    # Start consuming, block until keyboard interrupt
    try:
        channel.start_consuming()
        print "Press CTRL-C to quit."
    except KeyboardInterrupt:

        # Someone pressed CTRL-C, stop consuming and close
        channel.stop_consuming()
        CONNECTION.close()
