#!/bin/bash

echo "Upload data"

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

# AREA: brain, vnc, brain+vnc, vnc+brain
if [ "$#" -ge 1 ]; then
    AREA=$1
    shift
else
    echo "Anatomical area must be specified: submit_export.sh <anatomical_area> <export_type>"
    echo "Valid values: {brain | vnc | brain+vnc}"
    exit 1
fi

# UPLOAD_TYPE: EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
if [ "$#" -ge 1 ]; then
    UPLOAD_TYPE=$1
    shift
else
    echo "Upload type must be specified: submit_upload.sh <anatomical_area> <export_type>"
    echo "Valid values: {EM_MIPS | LM_MIPS | EM_CD_MATCHES | LM_CD_MATCHES | EM_PPP_MATCHES}"
    exit 1
fi

echo "Source global_params from ${SCRIPT_DIR}/../global-params.sh"
source "${SCRIPT_DIR}/../global-params.sh" ${AREA}
echo "Source upload_params from ${SCRIPT_DIR}/upload-params.sh"
source "${SCRIPT_DIR}/upload-params.sh"

case $UPLOAD_TYPE in
    EM_CD_MATCHES)
        JOB_TYPE=em-cds
        ;;
    LM_CD_MATCHES)
        JOB_TYPE=lm-cds
        ;;
    EM_PPP_MATCHES)
        JOB_TYPE=em-pppm
        ;;
    EM_MIPS)
        JOB_TYPE=em-mips
        ;;
    LM_MIPS)
        JOB_TYPE=lm-mips
        ;;
    *)
        echo "Invalid upload type: ${UPLOAD_TYPE}"
        exit 1
        ;;
esac


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

case $UPLOAD_TYPE in
    EM_CD_MATCHES)
        upload_cmd="uploadMatches ${AREA} cds ${PER_EM_DIR}"
        ;;
    LM_CD_MATCHES)
        upload_cmd="uploadMatches ${AREA} cds ${PER_LM_DIR}"
        ;;
    EM_PPP_MATCHES)
        upload_cmd="uploadMatches ${AREA} pppm ${PER_EM_DIR}"
        ;;
    EM_MIPS)
	upload_cmd="uploadMIPS ${AREA} ${EM_MIPS}"
        ;;
    LM_MIPS)
	upload_cmd="uploadMIPS ${AREA} ${LM_MIPS}"
        ;;
    *)
        echo "Invalid export type: ${UPLOAD_TYPE}"
        exit 1
        ;;
esac

`echo ${upload_cmd}`
