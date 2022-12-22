#!/bin/bash

function run_cds_job {
    # job index is 1-based index that should be passed either as the first arg
    # or in the LSB_JOBINDEX env var
    echo "ARG - $1"

    one_based_job_index=$((${LSB_JOBINDEX:-$1}))
    # convert the 1-based index to 0-based
    job_index=$((one_based_job_index - 1))
    
    masks_partition_index=$((job_index / MASKS_PER_JOB))
    targets_partition_index=$((job_index % MASKS_PER_JOB))

    masks_offset=$((masks_partition_index * MASKS_PER_JOB))
    targets_offset=$((targets_partition_index * TARGETS_PER_JOB))

    case ${ALIGNMENT_SPACE} in
        JRC2018_Unisex_20x_HR|JRC2018_VNC_Unisex_40x_DS)
            AS_ARG="-as ${ALIGNMENT_SPACE}"
            ;;
        *)
            echo "Invalid alignment space: ${ALIGNMENT_SPACE} - no alignment space argument will be used"
            AS_ARG=""
            ;;
    esac

    MASKS_ARG="-m ${MASKS_LIBRARY}:${masks_offset}:${MASKS_PER_JOB}"
    TARGETS_ARG="-i ${TARGETS_LIBRARY}:${targets_offset}:${TARGETS_PER_JOB}"

    cds_cmd="${JAVA_EXEC} \
        ${JAVA_OPTS} ${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} \
        -jar ${NEURONSEARCH_TOOLS_JAR} \
        colorDepthSearch \
        ${CONFIG_ARG} \
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
        $*"
    echo "$HOSTNAME $(date):> ${cds_cmd}"
    ($cds_cmd)
}

job_index=$1
echo "$(date) Run Job ${job_index} (Logs are located at ${JOB_LOGPREFIX})"
run_cds_job ${job_index} > ${JOB_LOGPREFIX}/cds_${job_index}.log 2>&1
