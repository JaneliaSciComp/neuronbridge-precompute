
LOG_OPTS="-Dlog4j2.configuration=file://$PWD/local/log4j.properties"
JAR_VERSION=3.0.0

SOURCE_PPP_RESULTS=/nrs/saalfeld/maisl/flymatch/all_vnc_1.0/setup22_nblast_20/results
PPP_RES_SUBDIR=lm_cable_length_20_v4_resmp_1_5

NEURON_DIR=${SOURCE_PPP_RESULTS}
RES_SUBDIR=${PPP_RES_SUBDIR}
EM_LIBRARY=flyem_vnc_0_5
LM_LIBRARY=flylight_gen1_mcfo_published

NEURON_SUBDIRS="\
00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 \
20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 \
40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 \
60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 \
80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99"

DEV_CONFIG="--config local/devdb-config.properties"
PROD_CONFIG="--config local/proddb-config.properties"
RUNNER=

for nd in ${NEURON_SUBDIRS} ; do
    echo "$(date) Process dir ${NEURON_DIR}/${nd}"
    ${RUNNER} java ${LOG_OPTS} \
        -jar target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
        importPPPResults \
        ${PROD_CONFIG} \
        --data-url http://e03u04.int.janelia.org:8800/api/rest-v2 \
        --authorization "APIKEY MyKey" \
        --anatomical-area VNC \
        -as JRC2018_VNC_Unisex_40x_DS \
        --em-library ${EM_LIBRARY} \
        --lm-library ${LM_LIBRARY} \
        -rd ${NEURON_DIR}/${nd} \
        --neuron-matches-sub-dir ${RES_SUBDIR} \
        -ps 20 \
        --only-best-skeleton-matches \
        --processing-tag 2.3.0-pre \
        $*
    echo "$(date) Completed dir ${NEURON_DIR}/${nd}"
done
