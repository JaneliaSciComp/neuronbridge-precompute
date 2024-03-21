nextflow run workflows/step1-run-cds.nf \
         --tool_runner "echo" \
         --anatomical_area brain \
         --targets_library flylight_split_gal4_published \
         $*
