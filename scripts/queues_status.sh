#!/bin/sh

sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged | grep -v ^amq.gen | grep -v ^job_status_log:sciflowuid
