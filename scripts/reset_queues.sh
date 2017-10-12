#!/bin/bash

sudo service rabbitmq-server stop
sudo rm -rf /var/lib/rabbitmq/mnesia
sudo cp -rp /var/lib/rabbitmq/mnesia.lastgood /var/lib/rabbitmq/mnesia
sudo service rabbitmq-server start
