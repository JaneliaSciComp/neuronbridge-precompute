#!/bin/bash

JOB_TYPE=${JOB_TYPE:=ga-em-vs-lm}

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/${JOB_TYPE}}

export TOTAL_MASK_NEURONS=$((${TOTAL_MASK_NEURONS:=${MASKS_COUNT}}))
export START_MASK_NEURON_INDEX=$((${START_MASK_NEURON_INDEX:=0}))
export NEURONS_PER_JOB=$((${NEURONS_PER_JOB:=${MASKS_PER_JOB}}))
export TOTAL_JOBS=$(((TOTAL_MASK_NEURONS - START_MASK_NEURON_INDEX) / NEURONS_PER_JOB + 1))

export MASK_NEURONS_FILTER=${MASK_NEURONS_FILTER:=""}
export MASKS_TAGS=${MASKS_TAGS:=""}
export TARGETS_TAGS=${TARGETS_TAGS:=""}
export CDS_TAG=${CDS_TAG:=""}
export TARGET_NEURONS_FILTER=${TARGET_NEURONS_FILTER:=""}
export MATCHES_TAGS=${MATCHES_TAGS:=""}

export PROCESSING_PARTITION_SIZE=${PROCESSING_PARTITION_SIZE:=100}

export TOP_RESULTS=${TOP_RESULTS:=300}
export SAMPLES_PER_LINE=${SAMPLES_PER_LINE:=0}
export BEST_MATCHES_PER_SAMPLE=${BEST_MATCHES_PER_SAMPLE:=0}

export FIRST_JOB=${FIRST_JOB:-1}
export LAST_JOB=${LAST_JOB:-${TOTAL_JOBS}}
export RUN_CMD=${RUN_CMD:=localRun}
