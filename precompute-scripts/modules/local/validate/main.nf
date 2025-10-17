include { 
      area_to_alignment_space;
      get_list_arg;
} from '../../../nfutils/utils'

process VALIDATE {
    container { task.ext.container }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(job_id),
          val(anatomical_area),
          val(mip_libraries),
          val(job_offset), 
          val(job_size)
    tuple path(app_jar),
          path(log_config),
          val(app_runner),
          val(readlink_cmd)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(release_names),
          val(published_names),
          val(sample_refs),
          val(mip_ids),
          val(tags),
          val(excluded_tags),
          val(terms),
          val(excluded_terms),
          val(jacs_url),
          val(jacs_authorization),
          val(jacs_read_batch_size),
          val(processing_size)
    path(mips_base_dir)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(${readlink_cmd} -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def concurrency_arg = cpus ? "--task-concurrency ${2 * cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def job_offset_arg = job_offset ? "--offset ${job_offset}" : ''
    def job_size_arg = job_size ? "--size ${job_size}" : ''
    def mip_libraries_list = get_list_arg(mip_libraries)
    def mip_libraries_arg = mip_libraries_list ? "-l ${mip_libraries_list}" : ''
    def validated_releases_arg = release_names ? "--validated-releases ${release_names}" : ''
    def validated_names_arg = published_names ? "--validated-names ${published_names}" : ''
    def validated_samples_arg = sample_refs ? "--validated-samples ${sample_refs}" : ''
    def validated_mips_arg = mip_ids ? "--validated-mips ${mip_ids}" : ''
    def validated_tags_arg = tags ? "--validated-tags ${tags}" : ''

    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''

    """
    echo "\$(date) Run validation job: ${job_id} on \$(hostname -s)"

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
        validateDBData
        --config ${db_config_file}
        ${concurrency_arg}
        --jacs-url "${jacs_url}"
        --authorization "${jacs_authorization}"
        --read-batch-size ${jacs_read_batch_size}
        ${processing_size_arg}
        ${alignment_space_arg}
        ${mip_libraries_arg}
        ${validated_releases_arg}
        ${validated_names_arg}
        ${validated_samples_arg}
        ${validated_mips_arg}
        ${validated_tags_arg}
        ${job_offset_arg} ${job_size_arg}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    echo "\$(date) Completed validation job: ${job_id} on \$(hostname -s)"
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