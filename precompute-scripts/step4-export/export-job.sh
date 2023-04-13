#!/bin/bash

function run_export_job() {
    # Supported LM Libraries
    declare LM_LIBS=(
        ${LM_SGAL4_LIB}
        ${LM_MCFO_LIB}
        ${LM_ANNOTATOR_MCFO_LIB}
    )
    # Supported EM Libraries
    declare EM_LIBS=(
        ${EM_HEMIBRAIN_1_2_1_LIB}
        ${EM_VNC_0_5_LIB}
        ${EM_VNC_0_6_LIB}
        ${EM_VNC_0_9_LIB}
    )

    case $EXPORT_TYPE in
        EM_CD_MATCHES)
            LIBNAMES=${EM_LIBS[@]}
            SUBDIR=cdmatches/em-vs-lm
            ;;
        LM_CD_MATCHES)
            LIBNAMES=${LM_LIBS[@]}
            SUBDIR=cdmatches/lm-vs-em
            ;;
        EM_PPP_MATCHES)
            LIBNAMES=${EM_LIBS[@]}
            SUBDIR=pppmatches/em-vs-lm
            ;;
        EM_MIPS)
            LIBNAMES=${EM_LIBS[@]}
            SUBDIR=mips/embodies
            ;;
        LM_MIPS)
            LIBNAMES=${LM_LIBS[@]}
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
            declare AS_ARG=
            ;;
    esac

    echo "DB_CONFIG: ${DB_CONFIG}"
    if [[ -n ${DB_CONFIG} && -f ${DB_CONFIG} ]]; then
        declare CONFIG_ARG="--config ${DB_CONFIG}"
    else
        echo "No database configuration set or found! Will use a default local database"
        declare CONFIG_ARG=
    fi

    declare OUTPUT_DIR=${EXPORT_DIR}/${AREA}
    declare JACS_AUTH="${JACS_AUTH_TYPE} ${JACS_AUTH_TOKEN}"
    export_cmd_args=(
        "${JAVA_OPTS}"
        -jar "${NEURONSEARCH_TOOLS_JAR}"
        exportData
        --jacs-url "${JACS_URL}"
        ${CONFIG_ARG}
        ${AS_ARG}
        --exported-result-type ${EXPORT_TYPE}
        --default-image-store "${BRAIN_STORE}"
        --image-stores-per-neuron-meta "JRC2018_VNC_Unisex_40x_DS:${VNC_STORE}"
        -l ${LIBNAMES[@]}
        --read-batch-size ${READ_BATCH_SIZE}
        -ps ${PROCESSING_PARTITION_SIZE}
        --default-relative-url-index 1
        -od "${OUTPUT_DIR}"
        --subdir ${SUBDIR}
        --offset 0 --size 0
    )

    echo "$HOSTNAME $(date):> ${JAVA_EXEC} ${export_cmd_args[@]} --authorization \"${JACS_AUTH}\""
    # when I tried to put authorization parameter directly in the export_cmd - it was not read correctly
    # so I had to pass it explicitly here
    ${JAVA_EXEC} ${export_cmd_args[@]} --authorization "${JACS_AUTH}"
}

OUTPUT_LOG=${JOB_LOGPREFIX}/export-${AREA}-${JOB_TYPE}.log
echo "$HOSTNAME $(date) :> Export $AREA $EXPORT_TYPE (Output log: ${OUTPUT_LOG})"

run_export_job > ${OUTPUT_LOG} 2>&1
