include {
    area_to_alignment_space;
    get_lib_arg;
    get_values_as_map;
} from '../../../nfutils/utils'

process GA {
    container { task.ext.container }
    cpus { cpus }
    memory { "${mem_gb} GB" }
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
          val(mask_terms),
          val(mask_excluded_terms),
          val(targets_published_names),
          val(target_terms),
          val(target_excluded_terms),
          val(mirror_flag),
          val(top_best_line_matches),
          val(top_best_sample_matches_per_line),
          val(top_best_matches_per_sample),
          val(processing_size),
          val(process_partitions_concurrently),
          val(matches_tags),
          val(masks_processing_tags),
          val(targets_processing_tags),
          val(pixel_percent_ratio),
          val(with_bidirectional_matching)
    path(mips_base_dir)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def cache_size_arg = cache_size ? "--cacheSize ${cache_size}" : ''
    def concurrency_arg = cpus ? "--task-concurrency ${2 * cpus -1}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def mask_terms_arg = mask_terms ? "--masks-terms ${mask_terms}" : ''
    def mask_excluded_terms_arg = mask_excluded_terms ? "--excluded-masks-terms ${mask_excluded_terms}" : ''
    def targets_library_arg = targets_library ? "--targets-libraries ${targets_library}" : ''
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def target_terms_arg = target_terms ? "--targets-terms ${target_terms}" : ''
    def target_excluded_terms_arg = target_excluded_terms ? "--excluded-targets-terms ${target_excluded_terms}" : ''
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''
    def process_partitions_concurrently_arg = process_partitions_concurrently ? '--process-partitions-concurrently' : ''
    def matches_tags_arg = matches_tags ? "--match-tags ${matches_tags}" : ''
    def masks_processing_tags_arg = masks_processing_tags ? "--masks-processing-tags ${get_processing_tags_arg(masks_processing_tags)}" : ''
    def targets_processing_tags_arg = targets_processing_tags ? "--targets-processing-tags ${get_processing_tags_arg(targets_processing_tags)}" : ''
    def pixel_percent_ratio_arg = pixel_percent_ratio && pixel_percent_ratio > 0 ? "--pctPositivePixels ${pixel_percent_ratio}" : ''
    def with_bidirectional_matching_arg = with_bidirectional_matching ? '--use-bidirectional-matching' : ''

    """
    echo "\$(date) Run ${anatomical_area} gradscore job: ${job_id} on \$(hostname -s)"
    mips_base_fullpath=\$(readlink ${mips_base_dir})
    echo "Mips base dir: \${mips_base_fullpath}"

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG="${log_config_arg}"
    else
        LOG_CONFIG_ARG=
    fi

    echo "Log config arg: \${LOG_CONFIG_ARG}"

    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        \${LOG_CONFIG_ARG} \
        -jar ${java_app} \
        ${cache_size_arg} \
        gradientScores \
        --config ${db_config_file} \
        ${concurrency_arg} \
        -as ${alignment_space} \
        --masks-libraries ${masks_arg} \
        ${masks_published_names_arg} \
        ${mask_terms_arg} \
        ${mask_excluded_terms_arg} \
        ${targets_library_arg} \
        ${targets_published_names_arg} \
        ${target_terms_arg} \
        ${target_excluded_terms_arg} \
        ${processing_size_arg} \
        ${process_partitions_concurrently_arg} \
        ${mirror_flag_arg} \
        --processing-tag ${ga_processing_tag} \
        ${matches_tags_arg} \
        ${masks_processing_tags_arg} \
        ${targets_processing_tags_arg} \
        ${pixel_percent_ratio_arg} \
        ${with_bidirectional_matching_arg} \
        --nBestLines ${top_best_line_matches} \
        --nBestSamplesPerLine ${top_best_sample_matches_per_line} \
        --nBestMatchesPerSample ${top_best_matches_per_sample}

    echo "\$(date) Completed ${anatomical_area} gradscore job: ${job_id} on \$(hostname -s)"
    """
}

def get_processing_tags_arg(ptags_as_str) {
    def ptags_map = get_values_as_map(ptags_as_str)
    ptags_map
        .collect { k, vs ->
            def vs_str = vs.join(';')
            "${k}:${vs_str}"
        }
        .inject('') { arg, item ->
            arg ? "${arg},${item}" : item
        }
}