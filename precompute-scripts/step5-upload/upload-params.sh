export S3_BUCKET=${S3_BUCKET:="janelia-neuronbridge-data-devpre"}

export S3_DATA_VERSION=${S3_DATA_VERSION:="${DATA_VERSION//\./_}"}

export LOCAL_DATA_VERSION=${LOCAL_DATA_VERSION:="${DATA_VERSION}"}
export LOCAL_DATA_DIR=${LOCAL_DATA_DIR:="/nrs/neuronbridge/${LOCAL_DATA_VERSION}"}

export AWSRUNNER=${AWSRUNNER:="echo"}
export AWS=${AWS:="${AWSRUNNER} aws"}
export AWSCP="$AWS s3 cp"
export MIPS_DIR=${MIPS_DIR:="mips"}
export CDS_RESULTS_DIR=${CDS_RESULTS_DIR:="cdmatches"}
export PPPM_RESULTS_DIR=${PPPM_RESULTS_DIR:="pppmatches"}
export PER_EM_DIR=${PER_EM_DIR:="em-vs-lm"}
export PER_LM_DIR=${PER_LM_DIR:="lm-vs-em"}
export LM_MIPS=${LM_MIPS:="lmlines"}
export EM_MIPS=${EM_MIPS:="embodies"}

export JOB_LOGPREFIX=${JOB_LOGPREFIX:="${LOGS_DIR}/uploads"}
