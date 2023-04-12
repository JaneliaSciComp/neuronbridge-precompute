#!/bin/bash

echo "Upload data"

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

echo "Source global_params from ${SCRIPT_DIR}/../global-params.sh"
source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
echo "Source upload_params from ${SCRIPT_DIR}/upload-params.sh"
source "${SCRIPT_DIR}/upload-params.sh"

uploadMIPS() {
    local region="$1" # brain, vnc, brain+vnc, or vnc+brain
    local mips_type="$2" # lmlines or embodies
    local d=${LOCAL_DATA_DIR}/${region}/${MIPS_DIR}/${mips_type}
    local mips_dest

    case ${mips_type} in
        lmlines|lm_lines|by_line)
            mips_dest=by_line
            ;;
        embodies|by_body|em_bodies)
            mips_dest=by_body
            ;;
        *)
            echo "Unsupported mips type: ${mips_type}"
            exit 1
    esac

    # upload
    $AWSCP $d s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/${mips_dest} --recursive
}

uploadMatches() {
    local region="$1" # brain or vnc
    local matches_type="$2" # cdmatches or pppmatches
    local direction="$3" # em-vs-lm or lm-vs-em

    local src_subdir
    local dest_subdir
    case ${matches_type} in
        cdm|cdmatches|cds|cdsresults)
            src_subdir=${CDS_RESULTS_DIR}
            dest_subdir=cdsresults
            ;;
        ppp|pppm|pppmatches|pppmresults)
            src_subdir=${PPPM_RESULTS_DIR}
            dest_subdir=pppmresults
            ;;
        *)
            echo "Unsupported matches type: ${matches_type}"
            exit 1
      esac
      local d=${LOCAL_DATA_DIR}/${region}/${src_subdir}/${direction}
      $AWSCP ${d} s3://${S3_BUCKET}/${S3_DATA_VERSION}/metadata/${dest_subdir} --recursive
}

mkdir -p $JOB_LOGPREFIX

#uploadMIPS brain ${LM_MIPS}
#uploadMIPS brain ${EM_MIPS}

#uploadMatches brain cds ${PER_EM_DIR}
#uploadMatches brain cds ${PER_LM_DIR}
#uploadMatches brain pppm ${PER_EM_DIR}

#uploadMatches vnc cds ${PER_EM_DIR}
#uploadMatches vnc cds ${PER_LM_DIR}
#uploadMatches vnc pppm ${PER_EM_DIR}
