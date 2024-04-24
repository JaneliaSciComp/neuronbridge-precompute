include {
    area_to_alignment_space;
    get_lib_arg;
} from '../../../nfutils/utils'

process GA {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/colormipsearch-tools:3.1.0' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

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
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(ga_processing_tag),
          val(cache_size),
          val(masks_published_names),
          val(targets_published_names),
          val(mirror_flag),
          val(top_best_line_matches),
          val(top_best_sample_matches_per_line),
          val(top_best_matches_per_sample),
          val(processing_size),
          val(process_partitions_concurrently)
    path(mips_base_dir)

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configurationFile=file:${log_config}" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def cache_size_arg = cache_size ? "--cacheSize ${cache_size}" : ''
    def concurrency_arg = cpus ? "--task-concurrency ${2 * cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def targets_library_arg = targets_library ? "--targets-libraries ${targets_library}" : ''
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''
    def process_partitions_concurrently_arg = process_partitions_concurrently ? '--process-partitions-concurrently' : ''

    """
    echo "\$(date) Run gradscore job: ${job_id} on \$(hostname -s)"
    mips_base_fullpath=\$(readlink ${mips_base_dir})
    echo "Mips base dir: \${mips_base_fullpath}"
    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        ${log_config_arg} \
        -jar ${java_app} \
        ${cache_size_arg} \
        gradientScores \
        --config ${db_config_file} \
        ${concurrency_arg} \
        -as ${alignment_space} \
        --masks-libraries ${masks_arg} \
        ${masks_published_names_arg} \
        ${targets_library_arg} \
        ${targets_published_names_arg} \
        ${processing_size_arg} \
        ${process_partitions_concurrently_arg} \
        ${mirror_flag_arg} \
        --nBestLines ${top_best_line_matches} \
        --nBestSamplesPerLine ${top_best_sample_matches_per_line} \
        --nBestMatchesPerSample ${top_best_matches_per_sample} \
        --processing-tag ${ga_processing_tag} \
    """
}
