#!/bin/bash

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

if [ "$#" -ge 1 ]; then
    AREA=$1
    shift
else
    echo "Anatomical area must be specified: submit_tag.sh <anatomical_area> [<tag>]"
    echo "Valid values: {brain | vnc}"
    exit 1
fi

source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
source "${SCRIPT_DIR}/tag-params.sh"

if [ "$#" -ge 1 ]; then
    TAG=$1
    shift
else
    TAG=${DATA_VERSION}
fi

export AREA
export TAG

mkdir -p $JOB_LOGPREFIX

sh ${SCRIPT_DIR}/tag-job.sh
