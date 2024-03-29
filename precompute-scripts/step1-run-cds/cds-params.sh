#!/bin/bash

JOB_TYPE=${JOB_TYPE:=cds-em-vs-lm}

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/${JOB_TYPE}}

export MASKS_PER_JOB=$((${MASKS_PER_JOB:=10000}))
export TARGETS_PER_JOB=$((${TARGETS_PER_JOB:=10000}))

export MASKS_PARTITIONS=$((MASKS_COUNT / MASKS_PER_JOB + 1))
export TARGETS_PARTITIONS=$((TARGETS_COUNT / TARGETS_PER_JOB + 1)) 
export TOTAL_JOBS=$((MASKS_PARTITIONS * TARGETS_PARTITIONS))

export MASK_NEURONS_FILTER=${MASK_NEURONS_FILTER:=""}
export MASKS_DATASETS=${MASKS_DATASETS:=""}
export MASKS_TAGS=${MASK_TAGS:=""}
export TARGET_NEURONS_FILTER=${TARGET_NEURONS_FILTER:=""}
export TARGETS_DATASETS=${TARGETS_DATASETS:=""}
export TARGETS_TAGS=${TARGETS_TAGS:=""}

export UPDATE_RESULTS=${UPDATE_RESULTS:="false"}

# Color depth search params
if [[ -z ${CDS_TAG} ]] ; then
    echo "CDS_TAG must be set in your .env file"
    exit 1
fi

export CDS_TAG=${CDS_TAG}
export PROCESSING_PARTITION_SIZE=${PROCESSING_PARTITION_SIZE:=1000}
export MASK_THRESHOLD=${MASK_THRESHOLD:=20}
export DATA_THRESHOLD=${MASK_THRESHOLD:=20}
export XY_SHIFT=${XY_SHIFT:=2}
export PIX_FLUCTUATION=${PIX_FLUCTUATION:=1.0}
export PIX_PCT_MATCH=${PIX_PCT_MATCH:=1.0}

export FIRST_JOB=${FIRST_JOB:-1}
export LAST_JOB=${LAST_JOB:-${TOTAL_JOBS}}
export RUN_CMD=${RUN_CMD:=localRun}
