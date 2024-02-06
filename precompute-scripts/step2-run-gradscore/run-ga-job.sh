#!/bin/bash

function run_ga_job {
    # job index is 1-based index that should be passed either as the first arg
    # or in the LSB_JOBINDEX env var
    declare -i ONE_BASED_JOB_INDEX=$1
    # convert the 1-based index to 0-based
    declare -i JOB_INDEX=$((ONE_BASED_JOB_INDEX - 1))

    declare MASK_NEURONS_OFFSET=$((JOB_INDEX * NEURONS_PER_JOB + START_MASK_NEURON_INDEX))

    echo "
    Job index: ${JOB_INDEX};
    Masks neurons offset: ${MASK_NEURONS_OFFSET};
    "

    MASKS_ARG="--masks-libraries ${MASKS_LIBRARY}:${MASK_NEURONS_OFFSET}:${NEURONS_PER_JOB}"
    TARGETS_ARG="--targets-libraries ${TARGETS_LIBRARY}"

    if [[ -n ${MASK_NEURONS_FILTER} ]]; then
        MASK_NEURONS_FILTER_ARG="--masks-published-names ${MASK_NEURONS_FILTER}"
    else
        MASK_NEURONS_FILTER_ARG=""
    fi
    if [[ -n ${MASKS_TAGS} ]]; then
        MASKS_TAGS_ARG="--mask-tags ${MASKS_TAGS}"
    else
        MASKS_TAGS_ARG=""
    fi
    if [[ -n ${TARGET_NEURONS_FILTER} ]]; then
        TARGET_NEURONS_FILTER_ARG="--targets-published-names ${TARGET_NEURONS_FILTER}"
    else
        TARGET_NEURONS_FILTER_ARG=""
    fi
    if [[ -n ${TARGETS_DATASETS} ]]; then
        TARGETS_DATASETS_ARG="--targets-datasets ${TARGETS_DATASETS}"
    else
        TARGETS_DATASETS_ARG=""
    fi
    if [[ -n ${TARGETS_TAGS} ]]; then
        TARGETS_TAGS_ARG="--target-tags ${TARGETS_TAGS}"
    else
        TARGETS_TAGS_ARG=""
    fi
    if [[ -n ${MATCHES_TAGS} ]]; then
        MATCHES_TAGS_ARG="--match-tags ${MATCHES_TAGS}"
    elif [[ -n ${CDS_TAG} ]]; then
        MATCHES_TAGS_ARG="--match-tags ${CDS_TAG}"
    else
        MATCHES_TAGS_ARG=""
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

    if (( ${MIPS_CACHE_SIZE} > 0 )); then
        echo "Mips cache size: ${MIPS_CACHE_SIZE}"
        CACHE_SIZE_ARG="--cacheSize ${MIPS_CACHE_SIZE}"
    else
        CACHE_SIZE_ARG=
        echo "No MIPS cache"
    fi

    gradscore_cmd="${JAVA_EXEC} \
        ${JAVA_OPTS} ${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} \
        -jar ${NEURONSEARCH_TOOLS_JAR} \
        ${CACHE_SIZE_ARG} \
        gradientScores \
        ${CONFIG_ARG} \
        ${CONCURRENCY_ARG} \
        ${AS_ARG} \
        ${MASKS_ARG} \
        ${MASK_NEURONS_FILTER_ARG} \
        ${MASKS_TAGS_ARG} \
        ${TARGETS_ARG} \
        ${TARGET_NEURONS_FILTER_ARG} \
        ${TARGETS_DATASETS_ARG} \
        ${TARGETS_TAGS_ARG} \
        ${MATCHES_TAGS_ARG} \
        --nBestLines ${TOP_RESULTS} \
        --nBestSamplesPerLine ${SAMPLES_PER_LINE} \
        --nBestMatchesPerSample ${BEST_MATCHES_PER_SAMPLE} \
        -ps ${PROCESSING_PARTITION_SIZE} \
        "
    echo "$HOSTNAME $(date):> ${gradscore_cmd}"
    ($gradscore_cmd)
}

JOB_INDEX=$((${LSB_JOBINDEX:-$1}))
OUTPUT_LOG=${JOB_LOGPREFIX}/ga_${JOB_INDEX}.log
echo "$(date) Run Job ${JOB_INDEX} (Output log: ${OUTPUT_LOG})"
run_ga_job ${JOB_INDEX} > ${OUTPUT_LOG} 2>&1
