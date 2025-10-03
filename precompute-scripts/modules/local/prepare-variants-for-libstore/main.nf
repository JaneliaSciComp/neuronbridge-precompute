include { 
    area_to_alignment_space;
    get_values_as_collection;
} from '../../../nfutils/utils'

// This module will create the CDM JSON file
// which later can be used to copy the mips to the JACS libstore
// It never writes anything to the NB database
process PREPARE_VARIANTS_FOR_MIPSTORE {
    container { task.ext.container }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(anatomical_area),
          val(library_name),
          path(output_dir),
          path(variants_location),
          val(display_cdm_location),
          val(searchable_cdm_location),
          val(grad_location),
          val(zgap_location),
          val(vol_segmentation_location),
          val(junk_location),
          val(output_name)
    tuple path(app_jar),
          path(log_config),
          val(app_runner),
          val(readlink_cmd)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(jacs_url),
          val(jacs_authorization)
    path(data_paths) // this argument is only sent to ensure all needed volumes are available
    
    output:
    tuple val(anatomical_area),
          val(library_name),
          env(full_output_name)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(${readlink_cmd} -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def alignment_space = area_to_alignment_space(anatomical_area)
    def output_file_name = output_name ?: library_name
    def library_variants_arg = create_library_variants_arg(
        library_name,
        variants_location, 
        display_cdm_location,
        searchable_cdm_location,
        grad_location,
        zgap_location,
        vol_segmentation_location,
        junk_location,
    )
    def jacs_url_arg = jacs_url ? "--jacs-url ${jacs_url}" : ''
    def jacs_auth_arg = jacs_authorization ? "--authorization \"${jacs_authorization}\"" : ''
    def output_dirname = output_dir ?: './'

    """
    echo "\$(date) Run ${library_name} variants import on \$(hostname -s)"

    full_output_dir=\$(${readlink_cmd} -m ${output_dirname})
    if [[ ! -e \${full_output_dir} ]]; then
        mkdir -p \${full_output_dir}
    fi

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG="${log_config_arg}"
    else
        LOG_CONFIG_ARG=
    fi

    if [[ -e "\${full_output_dir}/${output_file_name}.json" ]]; then
        echo "Remove file \${full_output_dir}/${output_file_name}.json because it already exists"
        rm -f "\${full_output_dir}/${output_file_name}.json"
    fi
    CMD=(
        ${app_runner} java
        ${java_opts} ${java_mem_opts}
        \${LOG_CONFIG_ARG}
        -jar ${java_app}
        createColorDepthSearchDataInput
        ${jacs_url_arg}
        ${jacs_auth_arg}
        -as ${alignment_space}
        -l ${library_name}
        ${library_variants_arg}
        --results-storage FS
        -od ${output_dirname}
        --output-filename ${output_file_name}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    full_output_name="\${full_output_dir}/${output_file_name}.json"
    echo "\$(date) Completed ${library_name} variants import on \$(hostname -s)"
    """
}

def create_library_variants_arg(library,
                                variants_location,
                                display_or_source_cdm_locations, // this is the same as source_cdm
                                searchable_cdm_locations,
                                grad_locations,
                                zgap_locations,
                                vol_segmentation_locations,
                                junk_locations) {
    def source_cdm_variants = create_variant_arg(library, variants_location, 'source_cdm', display_or_source_cdm_locations)
    def searchable_cdm_variants = create_variant_arg(library, variants_location, 'searchable_neurons', searchable_cdm_locations)
    def grad_variants = create_variant_arg(library, variants_location, 'gradient', grad_locations)
    def zgap_variants = create_variant_arg(library, variants_location, 'zgap', zgap_locations)
    def junk_variants = create_variant_arg(library, variants_location, 'junk', junk_locations)
    def _3d_seg_variants = create_variant_arg(library, variants_location, '3d-segmentation', vol_segmentation_locations)

    def variants_arg = "${source_cdm_variants} ${searchable_cdm_variants} ${grad_variants} ${zgap_variants} ${junk_variants} ${_3d_seg_variants}".trim()
    variants_arg ? "--librariesVariants ${variants_arg}" : ''
}

def create_variant_arg(library, variants_location, variant_type, locations) {
    if (locations) {
        def locations_list = get_values_as_collection(locations)
            .collect { location_arg ->
                def location
                if (location_arg.startsWith('/')) {
                    location = location_arg
                } else {
                    location = "${variants_location}/${location_arg}"
                }
                location
            }
            .join('^')

        "${library}:${variant_type}:${locations_list}"
    } else {
        ""
    }
}