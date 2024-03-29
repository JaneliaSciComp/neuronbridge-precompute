LIBNAME=$1

JACS_LIB_DIR=/nrs/jacs/jacsData/filestore/system/ColorDepthMIPs

export LIBDIR=${LIBDIR:=${JACS_LIB_DIR}/${ALIGNMENT_SPACE}/${LIBNAME}}

export JACS_URL=${JACS_URL:="http://<host>/api/rest-v2"}
export JACS_AUTH_TYPE=${JACS_AUTH_TYPE:="APIKEY"}
export JACS_AUTH_TOKEN=${JACS_AUTH_TOKEN:="tokenvalue"}

if [[ -z ${IMPORT_TAG} ]] ; then
    echo "IMPORT_TAG must be set in your .env file"
    exit 1
fi

export IMPORT_TAG=${IMPORT_TAG}

export SEARCHABLE_SUBFOLDER=${SEARCHABLE_SUBFOLDER:=}
export GRAD_SUBFOLDER=${GRAD_SUBFOLDER:=grad}
export ZGAP_SUBFOLDER=${ZGAP_SUBFOLDER:=zgap}

export SEARCHABLE_MIPS=${SEARCHABLE_MIPS:=${LIBNAME}:searchable_neurons:${LIBDIR}/${SEARCHABLE_SUBFOLDER}}
export GRAD_MIPS=${GRAD_MIPS:=${LIBNAME}:gradient:${LIBDIR}/${GRAD_SUBFOLDER}}
export ZGAP_MIPS=${ZGAP_MIPS:=${LIBNAME}:zgap:${LIBDIR}/${ZGAP_SUBFOLDER}}

export EXCLUDED_LIBS=${EXCLUDED_LIBS:=}
export PUBLISHED_NAMES_FILTER=${PUBLISHED_NAMES_FILTER:=}
export NEURON_NAMES=${NEURON_NAMES:=}

export RELEASE_NAMES=${RELEASE_NAMES:=}

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/imports}
