## Export Results

To run one export at time run each command from `step7-export/submit-all-exports.sh`:
```
sh step7-export/submit-export.sh brain+vnc EM_MIPS
sh step7-export/submit-export.sh brain+vnc LM_MIPS
sh step7-export/submit-export.sh brain EM_CD_MATCHES
sh step7-export/submit-export.sh brain LM_CD_MATCHES
sh step7-export/submit-export.sh brain EM_PPP_MATCHES
sh step7-export/submit-export.sh vnc EM_CD_MATCHES
sh step7-export/submit-export.sh vnc LM_CD_MATCHES
sh step7-export/submit-export.sh vnc EM_PPP_MATCHES
```

or run the entire script
