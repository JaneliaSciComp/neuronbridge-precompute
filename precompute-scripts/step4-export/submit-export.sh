#!/bin/bash

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

# AREA: brain, vnc, brain+vnc, vnc+brain
AREA=${AREA:=$1}

if [ "$#" -ge 1 ]; then
    shift
fi

EXPORT_TYPE=${EXPORT_TYPE:=$1}

sh ${SCRIPT_DIR}/export-job.sh brain+vnc EM_MIPS
sh ${SCRIPT_DIR}/export-job.sh brain+vnc LM_MIPS

sh ${SCRIPT_DIR}/export-job.sh brain EM_CD_MATCHES
sh ${SCRIPT_DIR}/export-job.sh brain LM_CD_MATCHES
sh ${SCRIPT_DIR}/export-job.sh brain EM_PPP_MATCHES

sh ${SCRIPT_DIR}/export-job.sh vnc EM_CD_MATCHES
sh ${SCRIPT_DIR}/export-job.sh vnc LM_CD_MATCHES
sh ${SCRIPT_DIR}/export-job.sh vnc EM_PPP_MATCHES
