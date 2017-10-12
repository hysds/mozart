import pika, traceback, json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('pika')
logger.setLevel(logging.INFO)


def processError(queue_name, body, error, traceback):
    """Add to error queue."""

    body = json.dumps({
        "queue_name": queue_name,
        "body": body,
        "error": error,
        "traceback": traceback
    })
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='error_queue', durable=True)
    channel.basic_publish(exchange='',
                  routing_key='error_queue',
                  body=body,
                  properties=pika.BasicProperties(
                     delivery_mode = 2, # make message persistent
                  ))
    connection.close()

def pika_callback(queue_name):
    def wrapped(fn):
        def wrapped_fn(ch, method, properties, body):
            logger.info(" [x] Received message from %s" % queue_name)
            try: fn(ch, method, properties, body)
            except Exception, e:
                processError(queue_name, body, str(e), traceback.format_exc())
            logger.info(" [x] Done")
            ch.basic_ack(method.delivery_tag)
        return wrapped_fn
    return wrapped
