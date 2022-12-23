#!/bin/bash

function run_ga_job {
    # job index is 1-based index that should be passed either as the first arg
    # or in the LSB_JOBINDEX env var
    declare -i one_based_job_index=$1
    # convert the 1-based index to 0-based
    declare -i job_index=$((one_based_job_index - 1))

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

    CACHE_SIZE_ARG="--cacheSize ${CACHE_SIZE}"

    if [[ ${AVAILABLE_THREADS} -gt 0 ]] ; then
        CONCURRENCY_ARG="--task-concurrency ${AVAILABLE_THREADS}"
    else
        CONCURRENCY_ARG=
    fi

    MASKS_ARG="-m ${MASKS_LIBRARY}:${masks_offset}:${MASKS_PER_JOB}"
    TARGETS_ARG="-i ${TARGETS_LIBRARY}:${targets_offset}:${TARGETS_PER_JOB}"

    cds_cmd="${JAVA_EXEC} \
        ${JAVA_OPTS} ${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} \
        -jar ${NEURONSEARCH_TOOLS_JAR} \
        ${CACHE_SIZE_ARG} \
        gradientScores \
        ${CONFIG_ARG} \
        ${CONCURRENCY_ARG} \
        ${AS_ARG} \
        ${MASKS_ARG} \
        ${TARGETS_ARG}
        --nBestLines ${TOP_RESULTS} \
        -ps ${PROCESSING_PARTITION_SIZE} \
        "
    echo "$HOSTNAME $(date):> ${cds_cmd}"
    ($cds_cmd)
}

job_index=$((${LSB_JOBINDEX:-$1}))
output_log=${JOB_LOGPREFIX}/ga_${job_index}.log
echo "$(date) Run Job ${job_index} (Output log: ${output_log})"
run_ga_job ${job_index} > ${output_log} 2>&1
