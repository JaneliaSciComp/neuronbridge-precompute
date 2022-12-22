anatomical_area=$1

# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

source "${SCRIPT_DIR}/../global-params.sh" ${anatomical_area}
source "${SCRIPT_DIR}/cds-params.sh"

function localRun {
    if [[ $# -lt 2 ]] ; then
      echo "localRun <from> <to>"
            exit 1
    fi
    declare -i from=$1
    declare -i to=$2
    for ((currindex=${from}; currindex<=${to}; currindex++)) ; do
        ${SCRIPT_DIR}/run-cds-job.sh ${currindex}
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
        -J CDS[${from}-${to}] \
        -P ${CLUSTER_PROJECT_CODE} \
        ${OTHER_BSUB_OPTIONS} \
        ${SCRIPT_DIR}/run-cds-job.sh
}

echo "Total jobs: $TOTAL_JOBS"

mkdir -p $JOB_LOGPREFIX

# to run locally use localRun <from> <to>
# to run on the grid use gridRun <from> <to>
startcmd="${RUN_CMD} ${FIRST_JOB} ${LAST_JOB}"
echo $startcmd
($startcmd)
