# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

anatomical_area=$1

source "${SCRIPT_DIR}/../global-params.sh" ${anatomical_area}
source "${SCRIPT_DIR}/normalize-ga-params.sh"
source "${SCRIPT_DIR}/../run-functions.sh" "${SCRIPT_DIR}/run-normalize-ga-job.sh" normalize_gradscore

echo "Total jobs: $TOTAL_JOBS"

mkdir -p $JOB_LOGPREFIX

echo ${RUN_JOBS_CMD}
${RUN_JOBS_CMD}
