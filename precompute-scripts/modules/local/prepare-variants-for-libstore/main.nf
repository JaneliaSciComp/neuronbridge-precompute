include { 
    area_to_alignment_space;
} from '../../../nfutils/utils'

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
          val(cdm_relative_location),
          val(grad_relative_location),
          val(zgap_relative_location),
          val(output_name)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(jacs_url),
          val(jacs_authorization)
    
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
        cdm_relative_location,
        grad_relative_location,
        zgap_relative_location,
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

    if [[ -e "\${full_output_dir}/${output_file_name}.json" ]]; then
        echo "Remove file \${full_output_dir}/${output_file_name}.json because it already exists"
        rm -f "\${full_output_dir}/${output_file_name}.json"
    fi
    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        ${log_config_arg} \
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
                                cdm_relative_location,
                                grad_relative_location,
                                zgap_relative_location) {
    def variants_arg = ""
    if (cdm_relative_location) {
        def cdm = "${variants_location}/${cdm_relative_location}"
        variants_arg = "${variants_arg} ${library}:searchable_neurons:${cdm}"
    }
    if (grad_relative_location) {
        def grad = "${variants_location}/${grad_relative_location}"
        variants_arg = "${variants_arg} ${library}:gradient:${grad}"
    }
    if (zgap_relative_location) {
        def zgap = "${variants_location}/${zgap_relative_location}"
        variants_arg = "${variants_arg} ${library}:zgap:${zgap}"
    }
    variants_arg ? "--librariesVariants ${variants_arg}" : ''
}
