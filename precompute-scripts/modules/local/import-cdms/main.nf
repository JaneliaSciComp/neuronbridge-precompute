include { 
    area_to_alignment_space;
    get_values_as_collection;
} from '../../../nfutils/utils'

process IMPORT_CDMS {
    container { task.ext.container }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(anatomical_area),
          val(library_name),
          val(library_base_dir),
          val(source_cdm_locations),
          val(searchable_cdm_locations),
          val(grad_locations),
          val(zgap_locations),
          val(vol_segmentation_locations),
          val(junk_locations)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(jacs_url),
          val(jacs_authorization),
          val(import_tag),
          val(junk_import_tag),
          val(import_mips),
          val(import_published_names),
          val(import_releases),
          val(excluded_libraries),
          val(included_neurons),
          val(excluded_neurons)
    path(data_paths) // this argument is only sent to ensure all needed volumes are available
    
    output:
    tuple val(anatomical_area),
          val(library_name),
          val(import_tag)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
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
        junk_locations,
    )
    def jacs_url_arg = jacs_url ? "--jacs-url ${jacs_url}" : ''
    def jacs_auth_arg = jacs_authorization ? "--authorization \"${jacs_authorization}\"" : ''
    def import_tag_arg = import_tag ? "--tag ${import_tag}" : ''
    def junk_import_tag_arg = junk_import_tag ? "--junk-tag ${junk_import_tag}" : ''
    def import_mips_arg = import_mips ? "--mips ${import_mips}" : ''
    def import_published_names_arg = import_published_names ? "--included-published-names ${import_published_names}" : ''
    def import_releases_arg = import_releases ? "--releases \"${import_releases}\"" : ''
    def excluded_libraries_arg = excluded_libraries ? "--excluded-libraries ${excluded_libraries}" : ''
    def included_neurons_arg = included_neurons ? "--included-neurons ${included_neurons}" : ''
    def excluded_neurons_arg = excluded_neurons ? "--excluded-neurons ${excluded_neurons}" : ''

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
        ${junk_import_tag_arg} \
        ${import_mips_arg} \
        ${import_published_names_arg} \
        ${import_releases_arg} \
        ${excluded_libraries_arg} \
        ${included_neurons_arg} \
        ${excluded_neurons_arg} \
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
                                vol_segmentation_locations,
                                junk_locations) {
    def source_cdm_variants = create_variant_arg(library, variants_location, 'source_cdm', source_cdm_locations)
    def searchable_cdm_variants = create_variant_arg(library, variants_location, 'searchable_neurons', searchable_cdm_locations)
    def grad_variants = create_variant_arg(library, variants_location, 'gradient', grad_locations)
    def zgap_variants = create_variant_arg(library, variants_location, 'zgap', zgap_locations)
    def _3d_seg_variants = create_variant_arg(library, variants_location, '3d-segmentation', vol_segmentation_locations)
    def junk_variants = create_variant_arg(library, variants_location, 'junk', junk_locations)

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