#!/bin/bash

JAR_VERSION=3.1.0

ALIGNMENT_SPACE=JRC2018_VNC_Unisex_40x_DS
LIBNAME=flylight_split_gal4_published
VALIDATED_NAMES_ARG=
APPLY_ERROR_TO_LMCDS="--apply-error-tag-to-lm-cdmatches"
APPLY_ERROR_TO_EMCDS="--apply-error-tag-to-em-cdmatches"
ERROR_TAG_ARG="--error-tag validationError ${APPLY_ERROR_TO_LMCDS} ${APPLY_ERROR_TO_EMCDS}"

PROD_CONFIG="--config local/proddb-config.properties"
DEV_CONFIG="--config local/devdb-config.properties"
RUNNER=

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"

$RUNNER java \
    -Xmx270G -Xms270G \
    ${DEBUG_OPTS} \
    -jar target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
    validateDBData \
    ${PROD_CONFIG} \
    -as ${ALIGNMENT_SPACE} \
    --jacs-url http://e03u04.int.janelia.org:8800/api/rest-v2 \
    --authorization "APIKEY MyKey" \
    -l ${LIBNAME} \
    ${EXCLUDED_LIBRARIES_ARG} \
    ${VALIDATED_NAMES_ARG} \
    ${ERROR_TAG_ARG} \
    --read-batch-size 2000 \
    -ps 50 \
    --offset 0 --size 0 \
    $*
