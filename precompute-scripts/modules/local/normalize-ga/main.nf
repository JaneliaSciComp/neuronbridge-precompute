include {
    area_to_alignment_space;
    get_lib_arg;
} from '../../../nfutils/utils'

process NORMALIZE_GA {
    container { task.ext.container ?: 'janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { normalize_ga_cpus }
    memory "${normalize_ga_mem_gb} GB"
    clusterOptions { task.ext.cluster_opts }


    input:
    tuple val(job_id),
          val(anatomical_area),
          val(masks_library),
          val(masks_offset),
          val(masks_length),
          val(targets_library)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(normalize_ga_cpus)
    val(normalize_ga_mem_gb)
    val(java_opts)
    tuple val(normalize_ga_processing_tag),
          val(masks_published_names),
          val(targets_published_names),
          val(processing_size)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configurationFile=file:${log_config}" : ''
    def java_mem_opts = "-Xmx${normalize_ga_mem_gb-1}G -Xms${normalize_ga_mem_gb-1}G"
    def concurrency_arg = normalize_ga_cpus ? "--task-concurrency ${2 * normalize_ga_cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def targets_library_arg = targets_library ? "--targets-libraries ${targets_library}" : ''
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''

    """
    echo "\$(date) Run job: ${job_id}"
    ${app_runner} java -showversion \
        ${java_opts} ${java_mem_opts} \
        ${log_config_arg} \
        -jar ${app_jar} \
        mormalizeGradientScores \
        --config ${db_config_file} \
        ${concurrency_arg} \
        -as ${alignment_space} \
        --masks-libraries ${masks_arg} \
        ${masks_published_names_arg} \
        ${targets_library_arg} \
        ${targets_published_names_arg} \
        --processing-tag ${normalize_ga_processing_tag} \
    """
}
