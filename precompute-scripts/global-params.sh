#!/bin/bash

PRECOMPUTE_SCRIPTS_BASEDIR=${PRECOMPUTE_SCRIPTS_BASEDIR:=${SCRIPT_DIR}}

if [[ -f "${PRECOMPUTE_SCRIPTS_BASEDIR}/../.env" ]]; then
    # read the session specific environment variables
    echo "Source ${PRECOMPUTE_SCRIPTS_BASEDIR}/../.env"
    source "${PRECOMPUTE_SCRIPTS_BASEDIR}/../.env"
fi

if [[ $# -ge 1 ]] ; then
    AREA=$1
    shift
fi

MASKS_LIBRARY=${MASKS_LIBRARY:=""}
TARGETS_LIBRARY=${TARGETS_LIBRARY:=""}
MASKS_COUNT=$((${MASKS_COUNT:=0}))
TARGETS_COUNT=$((${TARGETS_COUNT:=0}))

LOGS_DIR=${LOGS_DIR:="./logs"}

# Config
DB_CONFIG=${DB_CONFIG=""}

case ${AREA} in
    brain)
        ALIGNMENT_SPACE="JRC2018_Unisex_20x_HR"
        ;;
    vnc)
        ALIGNMENT_SPACE="JRC2018_VNC_Unisex_40x_DS"
        ;;
    *)
        ALIGNMENT_SPACE=
        ;;
esac

# Computation Resources
CPU_CORES=$((${CPU_CORES:=20}))
CPU_RESERVE=$((${CPU_RESERVE:=1}))
MEM_RESOURCE=$((${MEM_RESOURCE:=460}))
# For available threads assume 2 threads/core
AVAILABLE_THREADS=$((2 * CPU_CORES - CPU_RESERVE))
if (( ${AVAILABLE_THREADS} < 0 )) ; then
    AVAILABLE_THREADS=0
fi

# Supported EM Libraries
EM_HEMIBRAIN_LIB=flyem_hemibrain_1_2_1
EM_VNC_0_5_LIB=flyem_vnc_0_5
EM_VNC_0_6_LIB=flyem_vnc_0_6
EM_VNC_0_9_LIB=flyem_vnc_0_9

# Supported LM Libraries
SGAL4_LIB=flylight_split_gal4_published
MCFO_LIB=flylight_gen1_mcfo_published
ANNOTATOR_MCFO_LIB=flylight_annotator_gen1_mcfo_published

# Runtime
JAVA_EXEC=${JAVA_EXEC:-java}
JAVA_MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
JAVA_GC_OPTS=""
JAVA_OPTS="${JAVA_MEM_OPTS} ${JAVA_GC_OPTS}"

CLUSTER_PROJECT_CODE=${CLUSTER_PROJECT_CODE:=scicompsoft}
OTHER_BSUB_OPTIONS=

NEURONSEARCH_TOOLS_DIR="${PRECOMPUTE_SCRIPTS_BASEDIR}/../../neuron-search-tools"
JAR_VERSION=3.0.0
NEURONSEARCH_TOOLS_JAR="${NEURONSEARCH_TOOLS_DIR}/target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar"

if [[ -z ${AREA} ]]; then
    echo "Invalid Anatomical area"
    exit 1
fi

# export variables used in the called scripts
export MASKS_LIBRARY
export TARGETS_LIBRARY
export DB_CONFIG
export MEM_RESOURCE
export AVAILABLE_THREADS
export JAVA_EXEC
export JAVA_OPTS
export NEURONSEARCH_TOOLS_JAR
export ALIGNMENT_SPACE
