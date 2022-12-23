anatomical_area=$1

# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

source "${SCRIPT_DIR}/../global-params.sh" ${anatomical_area}
source "${SCRIPT_DIR}/cds-params.sh"
source "${SCRIPT_DIR}/../run-functions.sh" "${SCRIPT_DIR}/run-cds-job.sh" cds

echo "Total jobs: $TOTAL_JOBS"

mkdir -p $JOB_LOGPREFIX

echo ${RUN_JOBS_CMD}
${RUN_JOBS_CMD}
