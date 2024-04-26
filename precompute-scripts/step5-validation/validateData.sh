#!/bin/bash

JAR_VERSION=3.1.0

ALIGNMENT_SPACE=JRC2018_VNC_Unisex_40x_DS
LIBNAME=flylight_split_gal4_published

AS_ARG=
LIB_ARG="-l flywire_fafb_783"
VALIDATED_SAMPLES_ARG=
APPLY_ERROR_TO_LMCDS="--apply-error-tag-to-lm-cdmatches"
APPLY_ERROR_TO_EMCDS="--apply-error-tag-to-em-cdmatches"
ERROR_TAG_ARG="--error-tag validationError ${APPLY_ERROR_TO_LMCDS} ${APPLY_ERROR_TO_EMCDS}"

PROD_CONFIG="--config mylocal/proddb-config.properties"
DEV_CONFIG="--config mylocal/devdb-config.properties"
RUNNER=

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"

$RUNNER java \
    -Xmx120G -Xms120G \
    ${DEBUG_OPTS} \
    -jar ../neuron-search-tools/target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
    validateDBData \
    ${PROD_CONFIG} \
    --jacs-url http://e03u04.int.janelia.org:8800/api/rest-v2 \
    --authorization "APIKEY MyKey" \
    ${AS_ARG} \
    ${LIB_ARG} \
    ${EXCLUDED_LIBRARIES_ARG} \
    ${VALIDATED_SAMPLES_ARG} \
    ${ERROR_TAG_ARG} \
    --read-batch-size 2000 \
    -ps 50 \
    --offset 0 --size 0 \
    $*
