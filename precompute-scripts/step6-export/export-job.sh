#!/bin/bash

# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

source "${SCRIPT_DIR}/run-export-function.sh"

OUTPUT_LOG=${JOB_LOGPREFIX}/${AREA}-${JOB_TYPE}.log
echo "$HOSTNAME $(date) :> Export $AREA $EXPORT_TYPE (Output log: ${OUTPUT_LOG})"

run_export_job > ${OUTPUT_LOG} 2>&1
