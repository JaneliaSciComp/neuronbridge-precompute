#!/bin/bash

function run_tag_job {
    echo "DB_CONFIG: ${DB_CONFIG}"
    if [[ -n ${DB_CONFIG} && -f ${DB_CONFIG} ]]; then
        declare CONFIG_ARG="--config ${DB_CONFIG}"
    else
        echo "No database configuration set or found! Will use a default local database"
        declare CONFIG_ARG=""
    fi

    if [[ -n ${MIP_IDS} ]]; then
        declare MIP_IDS_ARG="--mip-ids ${MIP_IDS}"
    else
        declare MIP_IDS_ARG=""
    fi

    if [[ "${RELEASE_NAMES}" == "" ]]; then
	declare DATA_RELEASES_ARG=
    else
	declare DATA_RELEASES_ARG="--data-labels ${RELEASE_NAMES}"
    fi

    if [[ "${TAG_VALUES}" == "" ]]; then
	declare TAG_VALUES_ARG=
    else
	declare TAG_VALUES_ARG="--data-tags ${TAG_VALUES}"
    fi

    if [[ -n ${PUBLISHED_NAMES} ]]; then
        declare PUBLISHED_NAMES_ARG="--published-names ${PUBLISHED_NAMES}"
    else
        declare PUBLISHED_NAMES_ARG=
    fi

    tag_cmd_args=(
        "${JAVA_OPTS}"
        -jar "${NEURONSEARCH_TOOLS_JAR}"
        tag
        ${CONFIG_ARG}
        -as ${ALIGNMENT_SPACE}
        ${DATA_RELEASES_ARG}
        ${TAG_VALUES_ARG}
        ${PUBLISHED_NAMES_ARG}
        --tag ${TAG}
    )

    echo "$HOSTNAME $(date):> ${JAVA_EXEC} ${tag_cmd_args[@]}"
    ${JAVA_EXEC} ${tag_cmd_args[@]}
}

OUTPUT_LOG=${JOB_LOGPREFIX}/tag-${AREA}-${TAG}.log
echo "$HOSTNAME $(date) :> Tag $ALIGNMENT_SPACE (Output log: ${OUTPUT_LOG})"

run_tag_job > ${OUTPUT_LOG} 2>&1
