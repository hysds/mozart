#!/usr/bin/env python
import sys, pprint, os, json
from subprocess import Popen, PIPE

from pika import BasicProperties
from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters

CONNECTION = None
COUNT = 0

def handle_delivery(channel, method_frame, body):
    global COUNT
    j = json.loads(body)
    #pprint.pprint(j)
    if 'queue_name' in j:
        queue_name = str(j["queue_name"])
    elif 'job' in j and 'job_info' in j['job'] and 'job_queue' in j['job']['job_info']:
        queue_name = str(j['job']['job_info']['job_queue'])
    else: queue_name = "<unknown>"

    #ask for an action
    if queue_name == 'product_processed':
        channel2 = CONNECTION.channel()
        channel2.queue_declare(queue=queue_name, durable=True)
        channel2.basic_publish(exchange='',
                               routing_key=queue_name,
                               body=j['body'],
                               properties=BasicProperties(
                                  delivery_mode = 2, # make message persistent
                               ))

        # Acknowledge the message
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    #quit if no more
    COUNT -= 1
    if COUNT == 0: return True
    return False


if __name__ == '__main__':

    #get message count on error queue
    pop = Popen(["sudo", "rabbitmqctl", "list_queues"], 
                stdin=PIPE, stdout=PIPE, stderr=PIPE, env=os.environ)
    try: sts = pop.wait()  #wait for child to terminate and get status
    except Exception, e: print str(e)
    status = pop.returncode
    #print "returncode is:",status
    stdOut = pop.stdout.read()
    stdErr = pop.stderr.read()
    for line in stdOut.split('\n'):
        if line.startswith("error_queue"):
            COUNT = int(line.split()[1])
            break
    print "Total number of messages in error_queue:", COUNT
    if COUNT == 0: sys.exit()

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    CONNECTION = BlockingConnection(ConnectionParameters(host))

    # Open the channel
    channel = CONNECTION.channel()

    # loop
    for method_frame, properties, body in channel.consume('error_queue'):
        if handle_delivery(channel, method_frame, body): break

    channel.stop_consuming()
    channel.close()
    CONNECTION.close()
