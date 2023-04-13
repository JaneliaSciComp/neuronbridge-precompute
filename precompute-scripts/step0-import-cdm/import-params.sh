LIBNAME=$1

JACS_LIB_DIR=/nrs/jacs/jacsData/filestore/system/ColorDepthMIPs

export LIBDIR=${LIBDIR:=${JACS_LIB_DIR}/${ALIGNMENT_SPACE}/${LIBNAME}}

export JACS_URL=${JACS_URL:="http://<host>/api/rest-v2"}
export JACS_AUTH_TYPE=${JACS_AUTH_TYPE:="APIKEY"}
export JACS_AUTH_TOKEN=${JACS_AUTH_TOKEN:="tokenvalue"}

export IMPORT_TAG=${IMPORT_TAG:=}

export SEARCHABLE_MIPS=${SEARCHABLE_MIPS:=${LIBNAME}:searchable_neurons:${LIBDIR}/segmentation}
export GRAD_MIPS=${GRAD_MIPS:=${LIBNAME}:gradient:${LIBDIR}/grad}
export ZGAP_MIPS=${ZGAP_MIPS:=${LIBNAME}:zgap:${LIBDIR}/zgap}

export EXCLUDED_LIBS=${EXCLUDED_LIBS:=}
export PUBLISHED_NAMES_FILTER=${PUBLISHED_NAMES_FILTER:=}

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/imports}
