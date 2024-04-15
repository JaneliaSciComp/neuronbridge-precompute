process EXPORT {
    container { task.ext.container ?: 'janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { export_cpus }
    memory "${export_mem_gb} GB"

    input:
    tuple val(job_id),
          val(anatomical_area),
          val(libraries),
          val(job_offset), 
          val(job_size)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(export_cpus)
    val(export_mem_gb)
    val(java_opts)
    tuple val(export_type),  // EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
          val(exported_tags),
          val(excluded_tags),
          val(jacs_url),
          val(jacs_authorization),
          val(jacs_read_batch_size)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configurationFile=file:${log_config}" : ''
    def java_mem_opts = "-Xmx${normalize_ga_mem_gb-1}G -Xms${normalize_ga_mem_gb-1}G"
    def alignment_space = area_to_alignment_space(anatomical_area)
    def job_offset_arg = job_offset ? "--offset ${job_offset}" : ''
    def job_size_arg = job_size ? "--size ${job_size}" : ''

    """
    echo "\$(date) Run ${export_type} export job: ${job_id} "
    ${app_runner} java -showversion \
        ${java_opts} ${java_mem_opts} \
        ${log_config_arg} \
        -jar ${app_jar} \
        exportData \
        --config ${db_config_file} \
        --exported-result-type ${export_type} \
        --jacs-url ${jacs_url} \
        --authorization ${jacs_authorization} \
        --read-batch-size ${jacs_read_batch_size} \
        ${job_offset_arg} ${job_size_arg}

    """
}