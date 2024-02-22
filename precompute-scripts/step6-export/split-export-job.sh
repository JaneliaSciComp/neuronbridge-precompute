# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

# AREA: brain, vnc, brain+vnc, vnc+brain
if [ "$#" -ge 1 ]; then
    AREA=$1
    shift
else
    echo "Anatomical area must be specified: submit_export.sh <anatomical_area> <export_type>"
    echo "Valid values: {brain | vnc | brain+vnc}"
    exit 1
fi

# EXPORT_TYPE: EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
if [ "$#" -ge 1 ]; then
    EXPORT_TYPE=$1
    shift
else
    echo "Export type must be specified: submit_export.sh <anatomical_area> <export_type>"
    echo "Valid values: {EM_MIPS | LM_MIPS | EM_CD_MATCHES | LM_CD_MATCHES | EM_PPP_MATCHES}"
    exit 1
fi

case $EXPORT_TYPE in
    EM_CD_MATCHES)
        JOB_TYPE=em-cds
        ;;
    LM_CD_MATCHES)
        JOB_TYPE=lm-cds
        ;;
    EM_PPP_MATCHES)
        JOB_TYPE=em-pppm
        ;;
    EM_MIPS)
        JOB_TYPE=em-mips
        ;;
    LM_MIPS)
        JOB_TYPE=lm-mips
        ;;
    *)
        echo "Invalid export type: ${EXPORT_TYPE}"
        exit 1
        ;;
esac

export JOB_TYPE
export EXPORT_TYPE

echo "Source global_params from ${SCRIPT_DIR}/../global-params.sh"
source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
echo "Source export_params from ${SCRIPT_DIR}/export-params.sh"
source "${SCRIPT_DIR}/export-params.sh"

export TOTAL_EXPORTS=$((${TOTAL_EXPORTS:=0}))
export START_EXPORT=$((${START_EXPORT:=0}))

export EXPORT_SIZE=$((${EXPORT_SIZE:=1}))
export TOTAL_JOBS=$(((TOTAL_EXPORTS - START_EXPORT) / EXPORT_SIZE + 1))

echo "TOTAL_EXPORTS: ${TOTAL_EXPORTS}"

export FIRST_JOB=${FIRST_JOB:-1}
export LAST_JOB=${LAST_JOB:-${TOTAL_JOBS}}
export RUN_CMD=${RUN_CMD:=localRun}

source "${SCRIPT_DIR}/../run-functions.sh" "${SCRIPT_DIR}/run-export.sh" export-${JOB_TYPE}

echo "Total jobs: $TOTAL_JOBS"

echo ${RUN_JOBS_CMD}
${RUN_JOBS_CMD}
