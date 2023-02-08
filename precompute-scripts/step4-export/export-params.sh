BASE_EXPORT_FOLDER=${BASE_EXPORT_FOLDER:=/nrs/neuronbridge}
DATA_VERSION=${DATA_VERSION:=v3.0.0}

export OUTPUT_DIR=${EXPORT_DIR:=${BASE_EXPORT_FOLDER}/${DATA_VERSION}}
export JACS_URL=${JACS_URL:="http://<host>/api/rest-v2"}
export JACS_AUTH=${JACS_AUTH:="APIKEY thekeyvalue"}

export BRAIN_STORE=fl:open_data:brain
export VNC_STORE=fl:pre_release:vnc

export JOB_LOGPREFIX=${JOB_LOGPREFIX:=${LOGS_DIR}/exports}
export READ_BATCH_SIZE=2000
