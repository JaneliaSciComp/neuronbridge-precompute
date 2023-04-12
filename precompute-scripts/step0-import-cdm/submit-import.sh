#!/bin/bash

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
source "${SCRIPT_DIR}/import-params.sh"

mkdir -p $JOB_LOGPREFIX

sh ${SCRIPT_DIR}/import-job.sh
