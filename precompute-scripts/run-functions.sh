#!/bin/bash

RUN_SCRIPT=$1
JOB_NAME=$2

function localRun {
    if [[ $# -lt 2 ]] ; then
        echo "localRun <from> <to>"
        exit 1
    fi
    declare -i from=$1
    declare -i to=$2
    for ((currindex=${from}; currindex<=${to}; currindex++)) ; do
        ${RUN_SCRIPT} ${currindex}
    done
}

function gridRun {
    if [[ $# -lt 2 ]] ; then
        echo "gridRun <from> <to>"
        exit 1
    fi
    declare -i from=$1
    declare -i to=$2
    bsub -n ${CORES_RESOURCE} \
        -J ${JOB_NAME}[${from}-${to}] \
        -P ${CLUSTER_PROJECT_CODE} \
        ${OTHER_BSUB_OPTIONS} \
        ${RUN_SCRIPT}
}

# to run locally use localRun <from> <to>
# to run on the grid use gridRun <from> <to>
RUN_JOBS_CMD="${RUN_CMD} ${FIRST_JOB} ${LAST_JOB}"
