#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
PACKAGE_DIR=$(cd "${BASE_PATH}/.."; pwd)
CONF_FILE_SETTINGS="${PACKAGE_DIR}/settings.cfg"

ES_URL=`grep '^ES_URL' ${CONF_FILE_SETTINGS} | cut -d'"' -f 2`


curl -XDELETE "${ES_URL}/job_status/job/_query?q=status:job-failed"
