#!/bin/bash

function run_import_job {
    echo "DB_CONFIG: ${DB_CONFIG}"
    if [[ -n ${DB_CONFIG} && -f ${DB_CONFIG} ]]; then
        declare CONFIG_ARG="--config ${DB_CONFIG}"
    else
        echo "No database configuration set or found! Will use a default local database"
        declare CONFIG_ARG=""
    fi

    if [[ -n ${IMPORT_TAG} ]]; then
        echo "Tag imported ${LIBNAME} with ${IMPORT_TAG}"
        declare TAG_ARG="--tag ${IMPORT_TAG}"
    else
        declare TAG_ARG=""
    fi

    if [[ -n ${EXCLUDED_LIBS} ]]; then
        declare EXCLUDED_LIBS_ARG="--excluded-libraries ${EXCLUDED_LIBS}"
    else
        declare EXCLUDED_LIBS_ARG=""
    fi

    if [[ -n ${PUBLISHED_NAMES_FILTER} ]]; then
        declare PUBLISHED_NAMES_FILTER_ARG="--included-published-names ${PUBLISHED_NAMES_FILTER}"
    else
        declare PUBLISHED_NAMES_FILTER_ARG=""
    fi

    import_cmd="${JAVA_EXEC} \
        ${JAVA_OPTS} \
        -jar ${NEURONSEARCH_TOOLS_JAR} \
        createColorDepthSearchDataInput \
        ${CONFIG_ARG} \
        --jacs-url ${JACS_URL} \
        --authorization \"${JACS_AUTH_TYPE} ${JACS_AUTH_TOKEN}\" \
        -as ${ALIGNMENT_SPACE} \
        -l ${LIBNAME} \
        --librariesVariants ${SEARCHABLE_MIPS} ${GRAD_MIPS} ${ZGAP_MIPS} \
        ${EXCLUDED_LIBS_ARG} \
        ${PUBLISHED_NAMES_FILTER_ARG} \
        ${TAG_ARG} \
        --for-update \
        "

    echo "$HOSTNAME $(date):> ${import_cmd}"
    $import_cmd
}

OUTPUT_LOG=${JOB_LOGPREFIX}/import-${AREA}-${JOB_TYPE}.log
echo "$HOSTNAME $(date) :> Import $ALIGNMENT_SPACE $LIBNAME (Output log: ${OUTPUT_LOG})"

run_import_job > ${OUTPUT_LOG} 2>&1
