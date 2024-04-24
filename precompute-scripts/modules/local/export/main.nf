include { 
      area_to_alignment_space;
      get_list_arg;
} from '../../../nfutils/utils'

process EXPORT {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(job_id),
          val(data_version),
          val(anatomical_area),
          path(base_export_dir),
          val(relative_output_dir),
          val(mask_libraries),
          val(target_libraries),
          val(job_offset), 
          val(job_size)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(export_type),  // EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
          val(exported_tags),
          val(excluded_mask_tags),
          val(excluded_target_tags),
          val(jacs_url),
          val(jacs_authorization),
          val(default_image_store),
          val(image_stores_map),
          val(jacs_read_batch_size),
          val(processing_size)

    output:
    env(full_result_dir)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configurationFile=file:${log_config}" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def concurrency_arg = cpus ? "--task-concurrency ${2 * cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def job_offset_arg = job_offset ? "--offset ${job_offset}" : ''
    def job_size_arg = job_size ? "--size ${job_size}" : ''
    def mask_libraries_list = get_list_arg(mask_libraries)
    def target_libraries_list = get_list_arg(target_libraries)
    def mask_libraries_arg = mask_libraries_list ? "-l ${mask_libraries_list}" : ''
    def target_libraries_arg = target_libraries_list ? "--target-library ${target_libraries_list}" : ''
    def excluded_mask_tags_list = get_list_arg(excluded_mask_tags)
    def excluded_target_tags_list = get_list_arg(excluded_target_tags)
    def excluded_mask_tags_arg = excluded_mask_tags_list ? "--excluded-neuron-tags ${excluded_mask_tags_list}" : ''
    def excluded_target_tags_arg = excluded_target_tags_list ? "--excluded-target-tags ${excluded_target_tags_list}" : ''
    def default_image_store_arg = default_image_store ? "--default-image-store ${default_image_store}" : ''
    def image_stores_map_as_str = get_image_store_map_as_string(image_stores_map)
    def image_stores_map_arg = image_stores_map_as_str ? "--image-stores-per-neuron-meta ${image_stores_map_as_str}" : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''

    """
    echo "\$(date) Run ${export_type} export job: ${job_id} on \$(hostname)"
    release_export_dir="${base_export_dir}/v${data_version}"
    mkdir -p \${release_export_dir}
    result_export_dir="\${release_export_dir}/${anatomical_area}/${relative_output_dir}"
    full_result_dir=\$(readlink -m \${result_export_dir})
    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        ${log_config_arg} \
        -jar ${java_app} \
        exportData \
        --config ${db_config_file} \
        ${concurrency_arg} \
        --exported-result-type ${export_type} \
        --jacs-url "${jacs_url}" \
        --authorization "${jacs_authorization}" \
        --read-batch-size ${jacs_read_batch_size} \
        ${processing_size_arg} \
        ${alignment_space_arg} \
        ${mask_libraries_arg} \
        ${target_libraries_arg} \
        ${excluded_mask_tags_arg} \
        ${excluded_target_tags_arg} \
        ${default_image_store_arg} \
        ${image_stores_map_arg} \
        -od "\${result_export_dir}" \
        ${job_offset_arg} ${job_size_arg}

    """
}

def get_image_store_map_as_string(m) {
      if (m) {
            m.inject('') { s, k, v ->
                  "$s $k:$v"
            }
      } else {
            ''
      }
}