include {
    area_to_alignment_space;
    get_lib_arg;
} from '../../../nfutils/utils'

process GA {
    cpus { ga_cpus }
    memory "${ga_mem_gb} GB"
    clusterOptions { task.ext.cluster_opts }


    input:
    tuple val(job_id),
          val(anatomical_area),
          val(masks_library),
          val(masks_offset),
          val(masks_length),
          val(targets_library)
    tuple path(app_jar), val(ga_runner)
    path(db_config_file)
    val(ga_cpus)
    val(ga_mem_gb)
    val(java_opts)
    tuple val(ga_processing_tag),
          val(cache_size),
          val(masks_published_names),
          val(targets_published_names),
          val(mirror_flag),
          val(top_best_line_matches),
          val(top_best_sample_matches_per_line),
          val(top_best_matches_per_sample),
          val(processing_size)

    script:
    def java_mem_opts = "-Xmx${ga_mem_gb}G -Xms${ga_mem_gb}G"
    def cache_size_arg = cache_size ? "--cacheSize ${cache_size}" : ''
    def concurrency_arg = ga_cpus ? "--task-concurrency ${2 * ga_cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def targets_library_arg = targets_library ? "--targets-libraries ${targets_library}" : ''
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''

    """
    echo "\$(date) Run job: ${job_id}"
    ${ga_runner} java \
        ${java_opts} ${java_mem_opts} \
        -jar ${app_jar} \
        ${cache_size_arg} \
        gradientScores \
        --config ${db_config_file} \
        ${concurrency_arg} \
        -as ${alignment_space} \
        --masks-libraries ${masks_arg} \
        ${masks_published_names_arg} \
        ${targets_library_arg} \
        ${targets_published_names_arg} \
        ${mirror_flag_arg} \
        --nBestLines ${top_best_line_matches} \
        --nBestSamplesPerLine ${top_best_sample_matches_per_line} \
        --nBestMatchesPerSample ${top_best_matches_per_sample} \
        --processing-tag ${ga_processing_tag} \
    """
}
