include { 
    area_to_alignment_space;
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
          val(source_cdm_location),
          val(searchable_cdm_location),
          val(grad_location),
          val(zgap_location),
          val(vol_segmentation_location)
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
        source_cdm_location,
        searchable_cdm_location,
        grad_location,
        zgap_location,
        vol_segmentation_location,
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
                                source_cdm_location,
                                searchable_cdm_location,
                                grad_location,
                                zgap_location,
                                vol_segmentation_location) {
    def variants_arg = ""
    if (source_cdm_location) {
        def source_cdm
        if (source_cdm_location.startsWith('/')) {
            source_cdm = source_cdm_location
        } else {
            source_cdm = "${variants_location}/${source_cdm_location}"
        }
        variants_arg = "${variants_arg} ${library}:source_cdm:${source_cdm}"
    }
    if (searchable_cdm_location) {
        def searchable_cdm
        if (searchable_cdm_location.startsWith('/')) {
            searchable_cdm = searchable_cdm_location
        } else {
            searchable_cdm = "${variants_location}/${searchable_cdm_location}"
        }
        variants_arg = "${variants_arg} ${library}:searchable_neurons:${searchable_cdm}"
    }
    if (grad_location) {
        def grad
        if (grad_location.startsWith('/')) {
            grad = grad_location
        } else {
            grad = "${variants_location}/${grad_location}"
        }
        variants_arg = "${variants_arg} ${library}:gradient:${grad}"
    }
    if (zgap_location) {
        def zgap
        if (zgap_location.startsWith('/')) {
            zgap = zgap_location
        } else {
            zgap = "${variants_location}/${zgap_location}"
        }
        variants_arg = "${variants_arg} ${library}:zgap:${zgap}"
    }
    if (vol_segmentation_location) {
        def vol_segmentation
        if (vol_segmentation_location.startsWith('/')) {
            vol_segmentation = vol_segmentation_location
        } else {
            vol_segmentation = "${variants_location}/${vol_segmentation_location}"
        }
        variants_arg = "${variants_arg} ${library}:3d-segmentation:${vol_segmentation}"

    }
    variants_arg ? "--librariesVariants ${variants_arg}" : ''
}
