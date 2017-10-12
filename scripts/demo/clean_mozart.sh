#!/bin/bash

source $HOME/mozart/bin/activate

#rm -rf $HOME/mozart/log/*

# clean out mongo
mongo mozart --eval "db.dropDatabase();"

# clean out rabbitmq
sudo rabbitmqctl stop_app
sudo rabbitmqctl reset
sudo rabbitmqctl start_app

exit 0
