#!/bin/sh
clear
status=`sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged | grep -v ^amq. | grep -v ^job_status_log:sciflowuid`
while [ 1 ]; do
  echo "$status"
  sleep 1
  status=`sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged | grep -v ^amq. | grep -v ^job_status_log:sciflowuid`
  clear
done
