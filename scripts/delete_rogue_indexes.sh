#!/bin/bash
ES_URL=$1

ROGUE_INDEXES=(
  admin
  bin
  blazeds
  cgi-bin
  docs
  flex2gateway
  gw
  *.php  
  *.html  
  *.cgi
  lcds
  formmail*
  messagebroker
  perl
  phppath
  samba
  scripts
  servlet
  *.asp
  *.pl
  spipe
  static
  topic
  webui
)

for i in ${ROGUE_INDEXES[@]}; do
  curl -XDELETE "${ES_URL}/$i/"
  echo "Deleted $i"
done
