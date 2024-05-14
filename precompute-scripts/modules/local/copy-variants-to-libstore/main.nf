include { 
    area_to_alignment_space;
} from '../../../nfutils/utils'

process COPY_VARIANTS_TO_LIBSTORE {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(anatomical_area),
          val(library_name),
          path(variants_json_file),
          path(libstore_base_dir),
          val(dry_run)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    
    output:

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
    def dry_run_arg = dry_run ? '-n' : ''

    """
    echo "\$(date) Run ${library_name} variants import on \$(hostname -s)"

    full_output_dir=\$(readlink -m \${output_dir})
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
        copyToMipsStore \
        -i ${variants_json_file} \
        -od ${libstore_dir} \
        --surjective-variants-mappingcdm= \
        --surjective-variants-mappinggrad=grad \
        --surjective-variants-mappingsegmentation=segmentation \
        --surjective-variants-mappingzgap=zgap \
        ${dry_run_arg}

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
