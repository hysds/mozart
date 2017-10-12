#!/bin/bash

queue=$1
if [[ -z "$queue" ]]; then
  echo "Specify a queue to clean, e.g."
  echo "$0 queue_to_delete"
  exit 1
fi
curl -i -u guest:guest -H "content-type:application/json" -XDELETE "http://localhost:15672/api/queues/%2f/$queue/contents"
