#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
export PYTHONPATH=$BASE_PATH/..:$PYTHONPATH

# install templates for containers, job_specs, and hysds_ios
$BASE_PATH/install_es_template.py $*
