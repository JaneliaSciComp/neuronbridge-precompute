#!/bin/bash

function run_cds_job {
    # job index is 1-based index that should be passed either as the first arg
    # or in the LSB_JOBINDEX env var
    declare -i ONE_BASED_JOB_INDEX=$1
    # convert the 1-based index to 0-based
    declare -i JOB_INDEX=$((ONE_BASED_JOB_INDEX - 1))

    declare -i MASKS_PARTITION_INDEX=$((JOB_INDEX % MASKS_PARTITIONS))
    declare -i TARGETS_PARTITION_INDEX=$((JOB_INDEX / MASKS_PARTITIONS))

    declare -i MASKS_OFFSET=$((MASKS_PARTITION_INDEX * MASKS_PER_JOB))
    declare -i TARGETS_OFFSET=$((TARGETS_PARTITION_INDEX * TARGETS_PER_JOB))

    echo "
    Job index: ${JOB_INDEX};
    Masks partition index: ${MASKS_PARTITION_INDEX};
    Targets partition index: ${TARGETS_PARTITION_INDEX};
    Masks offset: ${MASKS_OFFSET};
    Targets offset: ${TARGETS_OFFSET};
    "

    declare MASKS_ARG="-m ${MASKS_LIBRARY}:${MASKS_OFFSET}:${MASKS_PER_JOB}"
    declare TARGETS_ARG="-i ${TARGETS_LIBRARY}:${TARGETS_OFFSET}:${TARGETS_PER_JOB}"

    if [[ -n ${CDS_TAG} ]]; then
        PROCESS_TAG_ARG="--processing-tag ${CDS_TAG}"
    else
        PROCESS_TAG_ARG=""
    fi

    case ${ALIGNMENT_SPACE} in
        JRC2018_Unisex_20x_HR|JRC2018_VNC_Unisex_40x_DS)
            AS_ARG="-as ${ALIGNMENT_SPACE}"
            ;;
        *)
            echo "Invalid alignment space: ${ALIGNMENT_SPACE} - no alignment space argument will be used"
            AS_ARG=""
            ;;
    esac

    if [[ -n ${DB_CONFIG} && -f ${DB_CONFIG} ]]; then
        CONFIG_ARG="--config ${DB_CONFIG}"
    else
        echo "No database configuration set or found! Will use a default local database"
        CONFIG_ARG=""
    fi

    if [[ ${AVAILABLE_THREADS} -gt 0 ]] ; then
        CONCURRENCY_ARG="--task-concurrency ${AVAILABLE_THREADS}"
    else
        CONCURRENCY_ARG=
    fi

    cds_cmd="${JAVA_EXEC} \
        ${JAVA_OPTS} ${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} \
        -jar ${NEURONSEARCH_TOOLS_JAR} \
        colorDepthSearch \
        ${CONCURRENCY_ARG} \
        ${CONFIG_ARG} \
        ${PROCESS_TAG_ARG} \
        ${AS_ARG} \
        ${MASKS_ARG} \
        ${TARGETS_ARG}
        --mirrorMask \
        --dataThreshold ${MASK_THRESHOLD} \
        --maskThreshold ${DATA_THRESHOLD} \
        --pixColorFluctuation ${PIX_FLUCTUATION} \
        --xyShift ${XY_SHIFT} \
        --pctPositivePixels ${PIX_PCT_MATCH} \
        -ps ${PROCESSING_PARTITION_SIZE} \
        "
    echo "$HOSTNAME $(date):> ${cds_cmd}"
    ($cds_cmd)
}

JOB_INDEX=$((${LSB_JOBINDEX:-$1}))
OUTPUT_LOG=${JOB_LOGPREFIX}/cds_${JOB_INDEX}.log
echo "$(date) Run Job ${JOB_INDEX} (Output log: ${OUTPUT_LOG})"
run_cds_job ${JOB_INDEX} > ${OUTPUT_LOG} 2>&1
