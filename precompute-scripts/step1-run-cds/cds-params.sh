#!/bin/bash

JOB_TYPE=${JOB_TYPE:=cds-em-vs-lm}

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/${JOB_TYPE}}

export MASKS_PER_JOB=$((${MASKS_PER_JOB:=50000}))
export TARGETS_PER_JOB=$((${TARGETS_PER_JOB:=5000}))

export MASKS_PARTITIONS=$((MASKS_COUNT / MASKS_PER_JOB + 1))
export TARGETS_PARTITIONS=$((TARGETS_COUNT / TARGETS_PER_JOB + 1)) 
export TOTAL_JOBS=$((MASKS_PARTITIONS * TARGETS_PARTITIONS))

# Color depth search params
export PROCESSING_PARTITION_SIZE=${PROCESSING_PARTITION_SIZE:=1000}
export MASK_THRESHOLD=${MASK_THRESHOLD:=20}
export DATA_THRESHOLD=${MASK_THRESHOLD:=20}
export XY_SHIFT=${XY_SHIFT:=2}
export PIX_FLUCTUATION=${PIX_FLUCTUATION:=1.0}
export PIX_PCT_MATCH=${PIX_PCT_MATCH:=1.0}

export FIRST_JOB=${FIRST_JOB:-1}
export LAST_JOB=${LAST_JOB:-${TOTAL_JOBS}}
export RUN_CMD=${RUN_CMD:=localRun}
