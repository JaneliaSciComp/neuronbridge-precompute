include {
    area_to_alignment_space;
} from '../../../nfutils/utils'

process TAG {
    container { task.ext.container }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(new_tag),
          val(anatomical_area)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(libraries),
          val(data_tags),
          val(data_labels),
          val(processing_tags),
          val(excluded_tags),
          val(published_names),
          val(mip_ids),
          val(neuron_terms),
          val(excluded_neuron_terms)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def libraries_arg = libraries ? "-l ${libraries}" : ''
    def data_tags_arg = data_tags ? "--data-tags ${data_tags}" : ''
    def data_labels_arg = data_labels ? "--data-labels ${data_labels}" : ''
    def excluded_tags_arg = excluded_tags ? "--excluded-data-tags ${excluded_tags}" : ''
    def processing_tags_arg = processing_tags ? "--processing-tags ${processing_tags}" : ''
    def published_names_arg = published_names ? "--published-names ${published_names}" : ''
    def mip_ids_arg = mip_ids ? "--mip-ids ${mip_ids}" : ''
    def neuron_terms_arg = neuron_terms ? "--neuron-terms ${neuron_terms}" : ''
    def excluded_neuron_terms_arg = excluded_neuron_terms ? "--excluded-neuron-terms ${excluded_neuron_terms}" : ''

    """
    echo "\$(date) Tag mips with: ${new_tag} on \$(hostname -s)"
    CMD=(
        ${app_runner} java
        ${java_opts} ${java_mem_opts}
        ${log_config_arg}
        -jar ${java_app}
        tag
        --config ${db_config_file}
        --tag ${new_tag}
        ${alignment_space_arg}
        ${libraries_arg}
        ${data_tags_arg}
        ${data_labels_arg}
        ${excluded_tags_arg}
        ${processing_tags_arg}
        ${published_names_arg}
        ${mip_ids_arg}
        ${neuron_terms_arg}
        ${excluded_neuron_terms_arg}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}
