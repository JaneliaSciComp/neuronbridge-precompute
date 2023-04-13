#!/bin/bash

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

if [ "$#" -ge 1 ]; then
    AREA=$1
    shift
else
    echo "Anatomical area must be specified: submit_import.sh <anatomical_area> <libname>"
    echo "Valid values: {brain | vnc}"
    exit 1
fi

if [ "$#" -ge 1 ]; then
    LIBNAME=$1
    shift
else
    echo "Library must be specified: submit_import.sh <anatomical_area> <libname>"
    exit 1
fi

source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
source "${SCRIPT_DIR}/import-params.sh" ${LIBNAME}

export AREA
export LIBNAME

mkdir -p $JOB_LOGPREFIX

sh ${SCRIPT_DIR}/import-job.sh
