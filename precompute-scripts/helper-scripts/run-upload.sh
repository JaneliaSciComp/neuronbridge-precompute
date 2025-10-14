#!/bin/bash

dry_run=false
LOCAL_DATA_DIR=v3.8.0-fixed-cns
AWS_DATA_DIR=3.8.0
bucket=janelia-neuronbridge-data-dev

nextflow run workflows/step9-upload.nf \
         --dry_run ${dry_run} \
         --upload_anatomical_areas brain+vnc \
         --upload_type "EM_MIPS,LM_MIPS" \
         --release_dirname ${LOCAL_DATA_DIR} \
         --data_version ${AWS_DATA_DIR} \
         --upload_bucket ${bucket}

MATCHES_TO_UPLOAD="EM_CD_MATCHES,LM_CD_MATCHES,EM_PPP_MATCHES"
MATCHES_FOR_ANATOMICAL_AREA="brain,vnc"

nextflow run workflows/step9-upload.nf \
         --dry_run ${dry_run} \
         --upload_anatomical_areas ${MATCHES_FOR_ANATOMICAL_AREA} \
         --upload_type ${MATCHES_TO_UPLOAD} \
         --release_dirname ${LOCAL_DATA_DIR} \
         --data_version ${AWS_DATA_DIR} \
         --upload_bucket ${bucket}
