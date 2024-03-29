#!/bin/bash

PRECOMPUTE_SCRIPTS_BASEDIR=${PRECOMPUTE_SCRIPTS_BASEDIR:=${SCRIPT_DIR}}

# This allows running this in parallel on different machines
# and use different environment
ENV_FILENAME=${ENV_FILENAME:="${PRECOMPUTE_SCRIPTS_BASEDIR}/../.env"}

echo "ENV file: ${ENV_FILENAME}"
if [[ -f "${ENV_FILENAME}" ]]; then
    # read the session specific environment variables
    echo "Source ${ENV_FILENAME}"
    source "${ENV_FILENAME}"
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
    brain|Brain)
        ALIGNMENT_SPACE="JRC2018_Unisex_20x_HR"
        ;;
    vnc|VNC)
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
MIPS_CACHE_SIZE=$((${MIPS_CACHE_SIZE:=100000}))

# Supported EM Libraries
EM_HEMIBRAIN_1_2_1_LIB=flyem_hemibrain_1_2_1
EM_VNC_0_5_LIB=flyem_vnc_0_5
EM_MANC_1_0_LIB=flyem_manc_1_0

# Supported LM Libraries
LM_SGAL4_LIB=flylight_split_gal4_published
LM_MCFO_LIB=flylight_gen1_mcfo_published
LM_ANNOTATOR_MCFO_LIB=flylight_annotator_gen1_mcfo_published

# Runtime
JAVA_EXEC=${JAVA_EXEC:-java}
JAVA_MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
JAVA_GC_OPTS=""
JAVA_DEBUG_OPTS=${JAVA_DEBUG_OPTS-}
JAVA_OPTS="${JAVA_MEM_OPTS} ${JAVA_GC_OPTS} ${JAVA_DEBUG_OPTS}"

CLUSTER_PROJECT_CODE=${CLUSTER_PROJECT_CODE:=scicompsoft}
OTHER_BSUB_OPTIONS=

NEURONSEARCH_TOOLS_DIR="${PRECOMPUTE_SCRIPTS_BASEDIR}/../../neuron-search-tools"
JAR_VERSION=3.1.0
NEURONSEARCH_TOOLS_JAR="${NEURONSEARCH_TOOLS_DIR}/target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar"

if [[ -z ${AREA} ]]; then
    echo "Invalid Anatomical area"
    exit 1
fi

# export variables used in the called scripts
export EM_HEMIBRAIN_1_2_1_LIB
export EM_VNC_0_5_LIB
export EM_MANC_1_0_LIB
export LM_SGAL4_LIB
export LM_MCFO_LIB
export LM_ANNOTATOR_MCFO_LIB
export MASKS_LIBRARY
export TARGETS_LIBRARY
export DB_CONFIG
export MEM_RESOURCE
export AVAILABLE_THREADS
export MIPS_CACHE_SIZE
export JAVA_EXEC
export JAVA_OPTS
export NEURONSEARCH_TOOLS_JAR
export ALIGNMENT_SPACE
