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

    if [[ "${RELEASE_NAMES}" == "" ]]; then
	declare DATA_RELEASES_ARG=
    else
	declare DATA_RELEASES_ARG="-r ${RELEASE_NAMES}"
    fi

    if [[ -n ${PUBLISHED_NAMES_FILTER} ]]; then
        declare PUBLISHED_NAMES_FILTER_ARG="--included-published-names ${PUBLISHED_NAMES_FILTER}"
    else
        declare PUBLISHED_NAMES_FILTER_ARG=
    fi

    declare JACS_AUTH="${JACS_AUTH_TYPE} ${JACS_AUTH_TOKEN}"

    import_cmd_args=(
        "${JAVA_OPTS}"
        -jar "${NEURONSEARCH_TOOLS_JAR}"
        createColorDepthSearchDataInput
        ${CONFIG_ARG}
        --jacs-url ${JACS_URL}
        -as ${ALIGNMENT_SPACE}
        -l ${LIBNAME}
        --librariesVariants ${SEARCHABLE_MIPS} ${GRAD_MIPS} ${ZGAP_MIPS}
        ${DATA_RELEASES_ARG}
        ${EXCLUDED_LIBS_ARG}
        ${PUBLISHED_NAMES_FILTER_ARG}
        ${TAG_ARG}
        --for-update
    )

    echo "$HOSTNAME $(date):> ${JAVA_EXEC} ${import_cmd_args[@]} --authorization \"${JACS_AUTH}\""
    ${JAVA_EXEC} ${import_cmd_args[@]} --authorization "${JACS_AUTH}"
}

OUTPUT_LOG=${JOB_LOGPREFIX}/import-${AREA}-${LIBNAME}.log
echo "$HOSTNAME $(date) :> Import $ALIGNMENT_SPACE $LIBNAME (Output log: ${OUTPUT_LOG})"

run_import_job > ${OUTPUT_LOG} 2>&1
