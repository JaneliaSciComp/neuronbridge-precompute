#!/bin/bash
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

sh ${SCRIPT_DIR}/submit-export.sh brain+vnc EM_MIPS
sh ${SCRIPT_DIR}/submit-export.sh brain+vnc LM_MIPS
TOTAL_EXPORTS=30790 EXPORT_SIZE=2500 sh ${SCRIPT_DIR}/split-export-job.sh brain EM_CD_MATCHES
sh ${SCRIPT_DIR}/submit-export.sh brain LM_CD_MATCHES
sh ${SCRIPT_DIR}/submit-export.sh brain EM_PPP_MATCHES
TOTAL_EXPORTS=92154 EXPORT_SIZE=5000 sh ${SCRIPT_DIR}/split-export-job.sh vnc EM_CD_MATCHES
sh ${SCRIPT_DIR}/submit-export.sh vnc EM_CD_MATCHES
sh ${SCRIPT_DIR}/submit-export.sh vnc LM_CD_MATCHES
sh ${SCRIPT_DIR}/submit-export.sh vnc EM_PPP_MATCHES
