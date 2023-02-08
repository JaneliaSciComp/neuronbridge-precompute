#!/bin/bash

function run_export_job {
    # Supported LM Libraries
    declare LM_LIBS="\
        ${LM_SGAL4_LIB} \
        ${LM_MCFO_LIB} \
        ${LM_ANNOTATOR_MCFO_LIB} \
        "
    # Supported EM Libraries
    declare EM_LIBS="\
        ${EM_HEMIBRAIN_1_2_1_LIB} \
        ${EM_VNC_0_5_LIB} \
        ${EM_VNC_0_6_LIB} \
        ${EM_VNC_0_9_LIB} \
        "

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


    case ${ALIGNMENT_SPACE} in
        JRC2018_Unisex_20x_HR|JRC2018_VNC_Unisex_40x_DS)
            declare AS_ARG="-as ${ALIGNMENT_SPACE}"
            ;;
        *)
            declare AS_ARG=""
            ;;
    esac

    if [[ -n ${DB_CONFIG} && -f ${DB_CONFIG} ]]; then
        declare CONFIG_ARG="--config ${DB_CONFIG}"
    else
        echo "No database configuration set or found! Will use a default local database"
        declare CONFIG_ARG=""
    fi

    # store name based on alignment space
    declare DATA_STORE_ARG="\
    --default-image-store \"${BRAIN_STORE}\" \
    --image-stores-per-neuron-meta \"JRC2018_VNC_Unisex_40x_DS:${VNC_STORE}\" \
    "

    export_cmd="${JAVA_EXEC} \
        ${JAVA_OPTS} ${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} \
        -jar ${NEURONSEARCH_TOOLS_JAR} \
        exportData \
        ${CONFIG_ARG} \
        ${AS_ARG} \
        --exported-result-type ${EXPORT_TYPE} \
        ${DATA_STORE_ARG} \
        --jacs-url \"${JACS_URL}\" \
        --authorization \"${JACS_AUTH}\" \
        -l ${LIBNAME} \
        --read-batch-size ${READ_BATCH_SIZE} \
        -ps ${PROCESSING_PARTITION_SIZE} \
        --default-relative-url-index 1 \
        -od ${OUTPUT_DIR} \
        --subdir ${SUBDIR} \
        --offset 0 --size 0 \
        "

    echo "$HOSTNAME $(date):> ${export_cmd}"
    $export_cmd
}

OUTPUT_LOG=${JOB_LOGPREFIX}/export-${AREA}-${JOB_TYPE}.log
echo "$HOSTNAME $(date) :> Export $AREA $EXPORT_TYPE (Output log: ${OUTPUT_LOG})"

run_export_job > ${OUTPUT_LOG} 2>&1
