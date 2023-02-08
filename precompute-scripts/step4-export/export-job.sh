# Precompute tools location
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

AREA=$1
shift

# EXPORT_TYPE can be one of [EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES]
EXPORT_TYPE=$1
shift

source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
source "${SCRIPT_DIR}/export-params.sh"

# Supported LM Libraries

LM_LIBS="\
${SGAL4_LIB} \
${MCFO_LIB} \
${ANNOTATOR_MCFO_LIB} \
"

EM_LIBS="\
${EM_HEMIBRAIN_LIB} \
${EM_VNC_0_5_LIB} \
${EM_VNC_0_6_LIB} \
${EM_VNC_0_9_LIB} \
"

case $EXPORT_TYPE in
  EM_CD_MATCHES)
    LIBNAME="${EM_LIBS}"
    SUBDIR=cdmatches/em-vs-lm
    JOB_TYPE=em-cds
    ;;
  LM_CD_MATCHES)
    LIBNAME="${LM_LIBS}"
    SUBDIR=cdmatches/lm-vs-em
    JOB_TYPE=lm-cds
    ;;
  EM_PPP_MATCHES)
    LIBNAME="${EM_LIBS}"
    SUBDIR=pppmatches/em-vs-lm
    JOB_TYPE=em-pppm
    ;;
  EM_MIPS)
    LIBNAME="${EM_LIBS}"
    SUBDIR=mips/embodies
    JOB_TYPE=em-mips
    ;;
  LM_MIPS)
    LIBNAME="${LM_LIBS}"
    SUBDIR=mips/lmlines
    JOB_TYPE=lm-mips
    ;;
  *)
    echo "Invalid export type: ${EXPORT_TYPE}"
    exit 1
    ;;
esac

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/exports/${AREA}}

case ${ALIGNMENT_SPACE} in
    JRC2018_Unisex_20x_HR|JRC2018_VNC_Unisex_40x_DS)
        AS_ARG="-as ${ALIGNMENT_SPACE}"
        ;;
    *)
        AS_ARG=""
        ;;
esac

if [[ -n ${DB_CONFIG} && -f ${DB_CONFIG} ]]; then
    CONFIG_ARG="--config ${DB_CONFIG}"
else
    echo "No database configuration set or found! Will use a default local database"
    CONFIG_ARG=""
fi

# store name based on alignment space
DATA_STORE_ARG="\
--default-image-store ${BRAIN_STORE} \
--image-stores-per-neuron-meta JRC2018_VNC_Unisex_40x_DS:${VNC_STORE} \
"

export_cmd="${JAVA_EXEC} \
    ${JAVA_OPTS} ${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} \
    -jar ${NEURONSEARCH_TOOLS_JAR} \
    exportData \
    ${CONFIG_ARG} \
    ${AS_ARG} \
    --exported-result-type ${EXPORT_TYPE} \
    ${DATA_STORE_ARG} \
    --jacs-url "${JACS_URL}" \
    --authorization "${JACS_AUTH}" \
    -l ${LIBNAME} \
    --read-batch-size ${READ_BATCH_SIZE} \
    -ps ${PROCESSING_PARTITION_SIZE} \
    --default-relative-url-index 1 \
    -od ${OUTPUT_DIR} \
    --subdir ${SUBDIR} \
    --offset 0 --size 0 \
    "

mkdir -p $JOB_LOGPREFIX

OUTPUT_LOG=${JOB_LOGPREFIX}/export-${AREA}-${JOB_TYPE}.log

$export_cmd 2>&1 | tee ${OUTPUT_LOG}
