#!/bin/bash

sh local/scripts/exportData.sh brain+vnc EM_MIPS 2>&1 | tee logs/export-em-mips.log
sh local/scripts/exportData.sh brain+vnc LM_MIPS 2>&1 | tee logs/export-lm-mips.log

sh local/scripts/exportData.sh brain EM_CD_MATCHES 2>&1 | tee logs/export-brain-em-cds.log
sh local/scripts/exportData.sh brain LM_CD_MATCHES 2>&1 | tee logs/export-brain-lm-cds.log
sh local/scripts/exportData.sh brain EM_PPP_MATCHES 2>&1 | tee logs/export-brain-em-pppm.log

sh local/scripts/exportData.sh vnc EM_CD_MATCHES 2>&1 | tee logs/export-vnc-em-cds.log
sh local/scripts/exportData.sh vnc LM_CD_MATCHES 2>&1 | tee logs/export-vnc-lm-cds.log
sh local/scripts/exportData.sh vnc EM_PPP_MATCHES 2>&1 | tee logs/export-vnc-em-pppm.log