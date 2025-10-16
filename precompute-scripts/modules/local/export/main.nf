include { 
      area_to_alignment_space;
      get_concurrency_arg;
      get_java_mem_opts;
      get_list_arg;
} from '../../../nfutils/utils'

process EXPORT {
    container { task.ext.container }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(job_id),
          val(data_version),
          val(anatomical_area),
          path(base_export_dir, stageAs: 'nb-export'),
          val(release_dirname),
          val(relative_output_dir),
          val(mask_libraries),
          val(target_libraries),
          val(job_offset), 
          val(job_size)
    tuple path(app_jar, stageAs: 'app/app.jar'),
          path(log_config, stageAs: 'config/logconfig.properties'),
          val(app_runner),
          val(readlink_cmd)
    path(db_config_file, stageAs: 'config/db.properties')
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(export_type),  // EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
          val(concurrency),
          val(mask_names),
          val(mask_mip_ids),
          val(mask_tags),
          val(excluded_mask_tags),
          val(target_tags),
          val(excluded_target_tags),
          val(mask_terms),
          val(excluded_mask_terms),
          val(target_terms),
          val(excluded_target_terms),
          val(jacs_url),
          val(jacs_authorization),
          val(default_image_store),
          val(image_stores_map),
          val(jacs_read_batch_size),
          val(db_read_page_size),
          val(processing_size),
          val(max_matches_with_same_name_per_mip),
          val(max_matches_per_mip)

    output:
    env(full_result_dir)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(${readlink_cmd} -e ${log_config})" : ''
    def java_mem_opts = get_java_mem_opts(mem_gb)
    def concurrency_arg = get_concurrency_arg(concurrency, cpus)
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def job_offset_arg = job_offset ? "--offset ${job_offset}" : ''
    def job_size_arg = job_size ? "--size ${job_size}" : ''
    def mask_libraries_list = get_list_arg(mask_libraries)
    def target_libraries_list = get_list_arg(target_libraries)
    def mask_libraries_arg = mask_libraries_list ? "-l ${mask_libraries_list}" : ''
    def target_libraries_arg = target_libraries_list ? "--target-library ${target_libraries_list}" : ''

    def release_dir = release_dirname ?: "v${data_version}"
    def mask_names_arg = get_arg_from_input_list('--exported-names', mask_names)
    def mask_mip_ids_arg = get_arg_from_input_list('--exported-mips', mask_mip_ids)

    def mask_tags_arg = get_arg_from_input_list('--neuron-tags', mask_tags)
    def excluded_mask_tags_arg = get_arg_from_input_list('--excluded-neuron-tags', excluded_mask_tags)

    def target_tags_arg = get_arg_from_input_list('--target-tags', target_tags)
    def excluded_target_tags_arg = get_arg_from_input_list('--excluded-target-tags', excluded_target_tags)

    def mask_terms_arg = get_arg_from_input_list('--neuron-terms', mask_terms)
    def excluded_mask_terms_arg = get_arg_from_input_list('--excluded-neuron-terms', excluded_mask_terms)

    def target_terms_arg = get_arg_from_input_list('--target-terms', target_terms)
    def excluded_target_terms_arg = get_arg_from_input_list('--excluded-target-terms', excluded_target_terms)

    def default_image_store_arg = default_image_store ? "--default-image-store ${default_image_store}" : ''
    def image_stores_map_as_str = get_image_store_map_as_string(image_stores_map)
    def image_stores_map_arg = image_stores_map_as_str ? "--image-stores-per-neuron-meta ${image_stores_map_as_str}" : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''
    def max_matches_with_same_name_per_mip_arg = max_matches_with_same_name_per_mip ? "--max-matches-with-same-name-per-mip ${max_matches_with_same_name_per_mip}" : ''
    def max_matches_per_mip_arg = max_matches_per_mip ? "--max-matches-per-mip ${max_matches_per_mip}" : ''
    def db_read_page_size_arg = (db_read_page_size as int) > 0 ? "--db-page-size ${db_read_page_size}" : ''

    """
    echo "\$(date) Run ${anatomical_area} ${export_type} export job: ${job_id} on \$(hostname -s)"
    release_export_dir="${base_export_dir}/${release_dir}"
    mkdir -p \${release_export_dir}
    result_export_dir="\${release_export_dir}/${anatomical_area}/${relative_output_dir}"
    full_result_dir=\$(${readlink_cmd} -m \${result_export_dir})

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG="${log_config_arg}"
    else
        LOG_CONFIG_ARG=
    fi

    CMD=(
      ${app_runner} java
        ${java_opts} ${java_mem_opts}
        \${LOG_CONFIG_ARG}
        -jar ${java_app}
        exportData
        --config ${db_config_file}
        ${concurrency_arg}
        --exported-result-type ${export_type}
        --jacs-url "${jacs_url}"
        --authorization "${jacs_authorization}"
        --read-batch-size ${jacs_read_batch_size}
        ${db_read_page_size_arg}
        ${processing_size_arg}
        ${alignment_space_arg}
        ${mask_names_arg}
        ${mask_mip_ids_arg}
        ${mask_libraries_arg}
        ${target_libraries_arg}
        ${mask_tags_arg}
        ${excluded_mask_tags_arg}
        ${target_tags_arg}
        ${excluded_target_tags_arg}
        ${mask_terms_arg}
        ${excluded_mask_terms_arg}
        ${target_terms_arg}
        ${excluded_target_terms_arg}
        ${default_image_store_arg}
        ${image_stores_map_arg}
        ${max_matches_with_same_name_per_mip_arg}
        ${max_matches_per_mip_arg}
        --default-relative-url-index 1
        -od "\${result_export_dir}"
        ${job_offset_arg} ${job_size_arg}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    echo "\$(date) Completed ${anatomical_area} ${export_type} export job: ${job_id} on \$(hostname -s)"
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

def get_arg_from_input_list(flag, lvalue) {
    def l = get_list_arg(lvalue)
    l ? "${flag} ${l}" : ''
}
