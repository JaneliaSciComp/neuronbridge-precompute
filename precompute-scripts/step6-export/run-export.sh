#!/bin/bash

# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

source "${SCRIPT_DIR}/run-export-function.sh"

function prepare_and_run_export {
    # job index is 1-based index that should be passed either as the first arg
    # or in the LSB_JOBINDEX env var
    declare -i ONE_BASED_JOB_INDEX=$1
    # convert the 1-based index to 0-based
    declare -i JOB_INDEX=$((ONE_BASED_JOB_INDEX - 1))

    declare EXPORT_OFFSET=$((JOB_INDEX * EXPORT_SIZE + START_EXPORT))

    echo "
    Job index: ${JOB_INDEX};
    Exports offset: ${EXPORT_OFFSET};
    "

#    export EXPORT_OFFSET
    run_export_job
}

JOB_INDEX=$((${LSB_JOBINDEX:-$1}))
OUTPUT_LOG=${JOB_LOGPREFIX}/${JOB_TYPE}_${JOB_INDEX}.log
echo "$(date) Run Job ${JOB_INDEX} (Output log: ${OUTPUT_LOG})"
prepare_and_run_export ${JOB_INDEX} > ${OUTPUT_LOG} 2>&1
