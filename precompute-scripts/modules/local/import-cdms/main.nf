include { 
    area_to_alignment_space;
    get_values_as_collection;
} from '../../../nfutils/utils'

process IMPORT_CDMS {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(anatomical_area),
          val(library_name),
          path(library_base_dir),
          val(source_cdm_locations),
          val(searchable_cdm_locations),
          val(grad_locations),
          val(zgap_locations),
          val(vol_segmentation_locations)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(jacs_url),
          val(jacs_authorization),
          val(import_tag)
    path(data_paths) // this argument is only sent to ensure all needed volumes are available
    
    output:
    tuple val(anatomical_area),
          val(library_name),
          val(import_tag)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def alignment_space = area_to_alignment_space(anatomical_area)
    def library_dir = "${library_base_dir}/${alignment_space}/${library_name}"
    def library_variants_arg = create_library_variants_arg(
        library_name,
        library_dir,
        source_cdm_locations,
        searchable_cdm_locations,
        grad_locations,
        zgap_locations,
        vol_segmentation_locations,
    )
    def jacs_url_arg = jacs_url ? "--jacs-url ${jacs_url}" : ''
    def jacs_auth_arg = jacs_authorization ? "--authorization \"${jacs_authorization}\"" : ''
    def import_tag_arg = import_tag ? "--tag ${import_tag}" : ''

    """
    echo "\$(date) Run ${library_name} CDMs import on \$(hostname -s)"

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG="${log_config_arg}"
    else
        LOG_CONFIG_ARG=
    fi

    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        \${LOG_CONFIG_ARG} \
        -jar ${java_app} \
        createColorDepthSearchDataInput \
        --config ${db_config_file} \
        ${jacs_url_arg} \
        ${jacs_auth_arg} \
        -as ${alignment_space} \
        -l ${library_name} \
        ${library_variants_arg} \
        ${import_tag_arg} \
        --results-storage DB \
        --for-update

    echo "\$(date) Completed ${library_name} CDMs import on \$(hostname -s)"
    """
}

def create_library_variants_arg(library,
                                variants_location,
                                source_cdm_locations,
                                searchable_cdm_locations,
                                grad_locations,
                                zgap_locations,
                                vol_segmentation_locations) {
    def source_cdm_variants = create_variant_arg(library, variants_location, 'source_cdm', source_cdm_locations)
    def searchable_cdm_variants = create_variant_arg(library, variants_location, 'searchable_neurons', searchable_cdm_locations)
    def grad_variants = create_variant_arg(library, variants_location, 'gradient', grad_locations)
    def zgap_variants = create_variant_arg(library, variants_location, 'zgap', zgap_locations)
    def _3d_seg_variants = create_variant_arg(library, variants_location, '3d-segmentation', vol_segmentation_locations)

    def variants_arg = "${source_cdm_variants} ${searchable_cdm_variants} ${grad_variants} ${zgap_variants} ${_3d_seg_variants}".trim()
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