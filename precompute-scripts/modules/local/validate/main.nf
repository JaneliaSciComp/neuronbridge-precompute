include { 
      area_to_alignment_space;
      get_list_arg;
} from '../../../nfutils/utils'

process VALIDATE {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(job_id),
          val(data_version),
          val(anatomical_area),
          val(mip_libraries),
          val(job_offset), 
          val(job_size)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(samples),
          val(data_releases),
          val(data_tags),
          val(data_names),
          val(excluded_mip_libraries),
          val(jacs_url),
          val(jacs_authorization),
          val(jacs_read_batch_size),
          val(processing_size)
    path(mips_base_dir)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j2.configurationFile=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def concurrency_arg = cpus ? "--task-concurrency ${2 * cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def job_offset_arg = job_offset ? "--offset ${job_offset}" : ''
    def job_size_arg = job_size ? "--size ${job_size}" : ''
    def mip_libraries_list = get_list_arg(mip_libraries)
    def mip_libraries_arg = mip_libraries_list ? "-l ${mip_libraries_list}" : ''
    def excluded_mip_libraries_list = get_list_arg(excluded_mip_libraries)
    def excluded_mip_libraries_arg = excluded_mip_libraries_list ? "--excluded-libraries ${excluded_mip_libraries_list}" : ''

    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''

    """
    echo "\$(date) Run validation job: ${job_id} on \$(hostname -s)"

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG=${log_config_arg}
    else
        LOG_CONFIG_ARG=
    fi

    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        \${LOG_CONFIG_ARG} \
        -jar ${java_app} \
        validateDBData \
        --config ${db_config_file} \
        ${concurrency_arg} \
        --jacs-url "${jacs_url}" \
        --authorization "${jacs_authorization}" \
        --read-batch-size ${jacs_read_batch_size} \
        ${processing_size_arg} \
        ${alignment_space_arg} \
        ${mip_libraries_arg} \
        ${excluded_mip_libraries_arg} \
        ${job_offset_arg} ${job_size_arg}

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