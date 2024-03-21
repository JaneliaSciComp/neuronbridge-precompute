if [[ $# -eq 0 ]] ; then
    echo "Missing anatomical area argument: use $0 <anatomical_area> ..."
    exit 1
fi

anatomical_area=$1
shift

nextflow run workflows/step2-run-gradscore.nf \
         --anatomical_area ${anatomical_area} \
         $*
