#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import os
import sys
import re
import json
import logging
import time
import pprint
from datetime import datetime, UTC
import pika

from mozart import app
from pikaUtils import pika_callback


log_format = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger("orchestrator")
logger.setLevel(logging.INFO)


def getTimestamp(fraction=True):
    """Return the current date and time formatted for a message header."""

    (year, month, day, hh, mm, ss, wd, y, z) = time.gmtime()
    d = datetime.now(UTC)
    if fraction:
        s = "%04d%02d%02dT%02d%02d%02d.%dZ" % (
            d.year,
            d.month,
            d.day,
            d.hour,
            d.minute,
            d.second,
            d.microsecond,
        )
    else:
        s = "%04d%02d%02dT%02d%02d%02dZ" % (
            d.year,
            d.month,
            d.day,
            d.hour,
            d.minute,
            d.second,
        )
    return s


def getFunction(funcStr, addToSysPath=None):
    """Automatically parse a function call string to import any libraries
    and return a pointer to the function.  Define addToSysPath to prepend a
    path to the modules path."""

    # check if we have to import a module
    libmatch = re.match(r"^((?:\w|\.)+)\.\w+\(?.*$", funcStr)
    if libmatch:
        importLib = libmatch.group(1)
        if addToSysPath:
            exec("import sys; sys.path.insert(1,'%s')" % addToSysPath)
        exec("import %s" % importLib)
        exec("reload(%s)" % importLib)

    # check there are args
    argsMatch = re.search(r"\((\w+)\..+\)$", funcStr)
    if argsMatch:
        importLib2 = argsMatch.group(1)
        if addToSysPath:
            exec("import sys; sys.path.insert(1,'%s')" % addToSysPath)
        exec("import %s" % importLib2)
        exec("reload(%s)" % importLib2)

    # return function
    return eval(funcStr)


def getJobId(job_name):
    """Return a mozart job id."""

    return "{}-{}".format(job_name, getTimestamp())


class OrchestratorClient:
    def __init__(self, connection, config, job_name, job_json, queue_name):
        self._connection = connection
        self._channel = None
        self._config = config
        self._job_json = job_json
        self._queue_name = queue_name
        self._response = {"status": None, "job_id": None}
        self._callback_queue = None
        self._corr_id = None

        # get descriptive job name
        if "name" in self._job_json:
            self._job_name = self._job_json["name"]
        else:
            self._job_name = job_name

        logger.info("config: %s" % self._config)

    def on_response(self, ch, method, props, body):
        """Handle response from worker."""

        logger.info("on_response was called with body: %s" % body)
        logger.info("on_response was called with props: %s" % props)

        if self._corr_id == props.correlation_id:
            self._response = json.loads(body)
            logger.info("set self._response to: %s" % pprint.pformat(self._response))
            if self._response["status"] in ("job-completed", "job-failed"):
                # move to completed or error queue
                if self._response["status"] == "job-completed":
                    routing_key = self._config["job_completed_queue"]
                else:
                    routing_key = self._config["job_error_queue"]
                self._channel.basic_publish(
                    exchange="",
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2  # make message persistent
                    ),
                )
                self._channel.close()
                logger.info("closed channel to temp queue: %s" % self._callback_queue)

    def on_queue_declared(self, frame):
        """Set response handler then submit the job."""

        self._callback_queue = frame.method.queue
        self._channel.basic_consume(
            self.on_response, no_ack=True, queue=self._callback_queue
        )

        # set job id
        self._job_json["job_id"] = getJobId(self._job_name)

        # set job_info
        self._job_json["job_info"] = {
            "id": self._job_json["job_id"],
            "job_queue": self._queue_name,
            "completed_queue": self._config["job_completed_queue"],
            "error_queue": self._config["job_error_queue"],
            "job_status_exchange": self._config["job_status_exchange"],
            "time_queued": datetime.now(UTC).replace(tzinfo=None).isoformat() + "Z",
        }

        body = json.dumps(self._job_json)

        self._response["job_id"] = self._job_json["job_id"]
        self._corr_id = self._job_json["job_id"]
        self._channel.basic_publish(
            exchange="",
            routing_key=self._queue_name,
            body=body,
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                correlation_id=self._corr_id,
                delivery_mode=2,  # make message persistent
            ),
        )

        # set status
        self._channel.basic_publish(
            exchange=self._config["job_status_exchange"],
            routing_key="",
            body=json.dumps(
                {
                    "job_id": self._job_json["job_id"],
                    "status": "job-queued",
                    "timestamp": datetime.now(UTC).replace(tzinfo=None).isoformat() + "Z",
                    "job": self._job_json,
                }
            ),
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                correlation_id=self._corr_id,
                delivery_mode=2,  # make message persistent
            ),
        )

    def on_channel_open(self, channel):
        """Declare temporary anonymous queue for worker response."""

        self._channel = channel
        self._channel.queue_declare(
            self.on_jobqueue_declared, self._queue_name, durable=True
        )

    def on_jobqueue_declared(self, frame):
        """Set response handler then submit the job."""

        self._channel.queue_declare(
            exclusive=True, auto_delete=True, callback=self.on_queue_declared
        )

    def queue(self):
        """Open new channel."""
        self._connection.channel(on_open_callback=self.on_channel_open)


class Orchestrator:
    """
    Based on the ansynchronous consumer example from the pika documentation:

        https://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html
    """

    def __init__(self, amqp_url, config_file):
        """Create a new instance of the orchestrator class, passing in the AMQP
        URL used to connect to RabbitMQ and the json config file.

        :param str amqp_url: The AMQP url to connect with
        :param str config_file: The JSON config file

        """
        self._url = amqp_url
        self._config_file = config_file
        self._config = json.loads(open(self._config_file).read())
        self._exchange = self._config["job_status_exchange"]
        self._exchange_type = "fanout"
        self._job_status_queues = ["job_status_response", "job_status_log"]
        # self._routing_key = None
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tags = {}
        self._added_cancel_callback = False

        # parse config file for job configurations
        self._job_config_dict = {}
        for config in self._config["configs"]:
            self._job_config_dict[config["job_type"]] = config["job_creators"]
        logger.info("Starting up orchestrator using %s." % self._config_file)

        # append job_creators dir
        self._job_creators_dir = os.path.normpath(
            os.path.join(app.root_path, "..", "scripts", "job_creators")
        )
        logger.info("Job creators directory: %s." % self._job_creators_dir)

    def create_job_callback(self, channel, method, properties, body):
        """Callback to handle job creation."""

        # self.acknowledge_message(method.delivery_tag)

        logger.info(
            "Received message # %s from %s: %s",
            method.delivery_tag,
            properties.app_id,
            body,
        )

        # check that we have info to create jobs
        j = json.loads(body)
        if "job_type" not in j:
            raise RuntimeError("Invalid job spec. No 'job_type' specified.")
        job_type = j["job_type"]
        if "payload" not in j:
            raise RuntimeError("Invalid job spec. No 'payload' specified.")
        payload = j["payload"]
        logger.info("got job_type: %s" % job_type)
        logger.info("payload: %s" % payload)

        # check that we know handle to handle this job type
        if job_type not in self._job_config_dict:
            logger.info("No job configuration info for '%s'." % job_type)
            raise RuntimeError("No job configuration info for '%s'." % job_type)

        # get job json and add to queues
        for jc in self._job_config_dict[job_type]:
            func = getFunction(jc["function"], addToSysPath=self._job_creators_dir)
            job = func(payload)
            logger.info("job_json: %s" % job)
            for queue in jc["job_queues"]:
                self.queue_job(jc["job_name"], job, queue)

    def queue_job(self, job_name, job, queue):
        """Queue job."""

        orc_client = OrchestratorClient(
            self._connection, self._config, job_name, job, queue
        )
        orc_client.queue()
        logger.info("added job_json to %s" % queue)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        logger.info("Connecting to %s", self._url)
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            self.on_connection_open,
            stop_ioloop_on_close=False,
        )

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        logger.info("Closing connection")
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        logger.info("Adding connection close callback")
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.frame.Method frame: The method frame from RabbitMQ

        """
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning(
                "Server closed connection, reopening: (%s) %s", reply_code, reply_text
            )
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        logger.info("Connection opened")
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.
        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()
        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as redeclare an exchange or queue with
        different paramters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.frame.Method frame: The Channel.Close method frame

        """
        logger.warning("Channel was closed: (%s) %s", reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.info("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange, self._exchange_type)

    def setup_exchange(self, exchange_name, exchange_type):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        logger.info("Declaring exchange %s of type %s", exchange_name, exchange_type)
        self._channel.exchange_declare(
            self.on_exchange_declareok, exchange_name, exchange_type, durable=True
        )

    def on_exchange_declareok(self, frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method frame: Exchange.DeclareOk response frame

        """
        logger.info("Exchange declared")

        # setup completed and error queues
        self.setup_queue_no_cb(self._config["job_completed_queue"])
        self.setup_queue_no_cb(self._config["job_error_queue"])

        # setup job status queues
        for queue in self._job_status_queues:
            self.setup_status_queue(queue)

        # setup job worker queues
        for queue in self._config["queues"]:
            self.setup_queue(queue)

    def setup_queue_no_cb(self, queue_name):
        logger.info("Declaring queue %s", queue_name)
        self._channel.queue_declare(None, queue_name, durable=True)

    def setup_status_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info("Declaring status queue %s", queue_name)
        self._channel.queue_declare(
            self.on_statusqueue_declareok, queue_name, durable=True
        )

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info("Declaring queue %s", queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method frame: The Queue.DeclareOk frame

        """
        queue = frame.method.queue
        self.start_consuming(queue)

    def on_statusqueue_declareok(self, frame):
        queue = frame.method.queue
        logger.info("Binding {} to {}".format(self._exchange, queue))
        self._channel.queue_bind(self.on_bindok, queue, self._exchange)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        if not self._added_cancel_callback:
            logger.info("Adding consumer cancellation callback")
            self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
            self._added_cancel_callback = True

    def on_consumer_cancelled(self, frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self._channel.basic_ack(delivery_tag)
        logger.info("Acknowledged message %s", delivery_tag)

    def on_cancelok(self, frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the connection
        which will automatically close the channel if it's open.

        :param pika.frame.Method frame: The Basic.CancelOk frame

        """
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self.close_connection()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            for queue in self._consumer_tags:
                self._channel.basic_cancel(self.on_cancelok, self._consumer_tags[queue])

    def start_consuming(self, queue):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. A wrapped method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self.add_on_cancel_callback()
        callback = pika_callback(queue)(self.create_job_callback)
        logger.info("Got callback for queue {}: {}".format(queue, callback))
        self._consumer_tags[queue] = self._channel.basic_consume(callback, queue=queue)

    def on_bindok(self, frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method frame: The Queue.BindOk response frame

        """
        logger.info("Queue bound")

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info("Closing the channel")
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        logger.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        logger.info("Stopping")
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        logger.info("Stopped")


def main():
    amqp_url = "amqp://guest:guest@localhost:5672/%2F"
    config_file = sys.argv[1]

    # handle rabbitMQ server being down
    while True:
        try:
            orch = Orchestrator(amqp_url, config_file)
            break
        except OSError as e:
            logger.error("Failed to connect: %s" % str(e))
            time.sleep(3)

    # start event loop
    try:
        orch.run()
    except KeyboardInterrupt:
        orch.stop()


if __name__ == "__main__":
    main()
