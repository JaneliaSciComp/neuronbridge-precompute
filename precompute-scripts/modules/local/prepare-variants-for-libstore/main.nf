include { 
    area_to_alignment_space;
} from '../../../nfutils/utils'

// This module will create the CDM JSON file
// which later can be used to copy the mips to the JACS libstore
// It never writes anything to the NB database
process PREPARE_VARIANTS_FOR_MIPSTORE {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/colormipsearch-tools:3.1.0' }
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
          val(output_name)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
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
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(readlink -e ${log_config})" : ''
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
    )
    def jacs_url_arg = jacs_url ? "--jacs-url ${jacs_url}" : ''
    def jacs_auth_arg = jacs_authorization ? "--authorization \"${jacs_authorization}\"" : ''
    def output_dirname = output_dir ?: './'

    """
    echo "\$(date) Run ${library_name} variants import on \$(hostname -s)"

    full_output_dir=\$(readlink -m ${output_dirname})
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
    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        \${LOG_CONFIG_ARG} \
        -jar ${java_app} \
        createColorDepthSearchDataInput \
        ${jacs_url_arg} \
        ${jacs_auth_arg} \
        -as ${alignment_space} \
        -l ${library_name} \
        ${library_variants_arg} \
        --results-storage FS \
        -od ${output_dirname} \
        --output-filename ${output_file_name}

    full_output_name="\${full_output_dir}/${output_file_name}.json"
    echo "\$(date) Completed ${library_name} variants import on \$(hostname -s)"
    """
}

def create_library_variants_arg(library,
                                variants_location,
                                display_cdm_location,
                                searchable_cdm_location,
                                grad_location,
                                zgap_location) {
    def variants_arg = ""
    if (display_cdm_location) {
        def display_cdm
        if (display_cdm_location.startsWith('/')) {
            display_cdm = display_cdm_location
        } else {
            display_cdm = "${variants_location}/${display_cdm_location}"
        }
        variants_arg = "${variants_arg} ${library}:source_cdm:${display_cdm}"
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
    variants_arg ? "--librariesVariants ${variants_arg}" : ''
}
