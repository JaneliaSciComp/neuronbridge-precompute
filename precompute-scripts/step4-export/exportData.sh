#!/bin/bash

JAR_VERSION=3.0.0

AREA=$1
shift
# EXPORT_TYPE can be one of [EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES]
EXPORT_TYPE=$1
shift

OUTPUT_DIR=/nrs/neuronbridge/v3.0.0/${AREA}
PROD_CONFIG="--config local/proddb-config.properties"
DEV_CONFIG="--config local/devdb-config.properties"
RUNNER=

# Typically no change is required below this point
EM_HEMIBRAIN_LIB=flyem_hemibrain_1_2_1
EM_VNC_0_5_LIB=flyem_vnc_0_5
EM_VNC_0_6_LIB=flyem_vnc_0_6

SGAL4_LIB=flylight_split_gal4_published
MCFO_LIB=flylight_gen1_mcfo_published
ANNOTATOR_MCFO_LIB=flylight_annotator_gen1_mcfo_published

LM_LIBS="${SGAL4_LIB} ${MCFO_LIB} ${ANNOTATOR_MCFO_LIB}"
EM_LIBS="${EM_HEMIBRAIN_LIB} ${EM_VNC_0_5_LIB} ${EM_VNC_0_6_LIB}"

case $EXPORT_TYPE in
  EM_CD_MATCHES)
    LIBNAME="${EM_LIBS}"
    SUBDIR=cdmatches/em-vs-lm
    ;;
  LM_CD_MATCHES)
    LIBNAME="${LM_LIBS}"
    SUBDIR=cdmatches/lm-vs-em
    ;;
  EM_PPP_MATCHES)
    LIBNAME="${EM_LIBS}"
    SUBDIR=pppmatches/em-vs-lm
    ;;
  EM_MIPS)
    LIBNAME="${EM_LIBS}"
    SUBDIR=mips/embodies
    ;;
  LM_MIPS)
    LIBNAME="${LM_LIBS}"
    SUBDIR=mips/lmlines
    ;;
  *)
    echo "Invalid export type: ${EXPORT_TYPE}"
    exit 1
    ;;
esac

# store name based on alignment space
DATA_STORE_ARG="\
--default-image-store fl:open_data:brain \
--image-stores-per-neuron-meta JRC2018_VNC_Unisex_40x_DS:fl:pre_release:vnc \
"

URL_TRANSFORM_PARAMS=

case ${AREA} in
  brain)
    AS_ARG="-as JRC2018_Unisex_20x_HR"
    ;;
  vnc)
    AS_ARG="-as JRC2018_VNC_Unisex_40x_DS"
    ;;
  brain+vnc|vnc+brain)
    AS_ARG=""
    ;;
  *)
    echo "Invalid area: ${AREA}"
    exit 1
    ;;
esac

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
$RUNNER java \
    -Xmx270G -Xms270G \
    -jar target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
    exportData \
    ${PROD_CONFIG} \
    ${AS_ARG} \
    --exported-result-type ${EXPORT_TYPE} \
    ${DATA_STORE_ARG} \
    --jacs-url http://e03u04.int.janelia.org:8800/api/rest-v2 \
    --authorization "APIKEY MyKey" \
    -l ${LIBNAME} \
    --read-batch-size 2000 \
    -ps 50 \
    --default-relative-url-index 1 \
    ${URL_TRANSFORM_PARAMS} \
    -od ${OUTPUT_DIR} \
    --subdir ${SUBDIR} \
    --offset 0 --size 0 \
    $*