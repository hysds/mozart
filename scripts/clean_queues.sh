#!/bin/bash

sudo service rabbitmq-server stop
sudo rm -rf /var/lib/rabbitmq/mnesia/*
sudo service rabbitmq-server start
