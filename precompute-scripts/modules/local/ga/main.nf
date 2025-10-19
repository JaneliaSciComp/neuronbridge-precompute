include {
    area_to_alignment_space;
    get_concurrency_arg;
    get_java_mem_opts;
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
          val(targets_library),
          val(start_delay)
    tuple path(app_jar),
          path(log_config),
          val(app_runner),
          val(readlink_cmd)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(ga_processing_tag),
          val(concurrency),
          val(cache_size),
          val(masks_published_names),
          val(mask_tags),
          val(mask_excluded_tags),
          val(mask_terms),
          val(mask_excluded_terms),
          val(targets_published_names),
          val(target_tags),
          val(target_excluded_tags),
          val(target_terms),
          val(target_excluded_terms),
          val(mirror_flag),
          val(mask_th),
          val(top_best_line_matches),
          val(top_best_sample_matches_per_line),
          val(top_best_matches_per_sample),
          val(processing_size),
          val(matches_tags),
          val(masks_processing_tags),
          val(targets_processing_tags),
          val(pixel_percent_ratio),
          val(with_bidirectional_matching),
          val(cancel_prev_scores)
    path(mips_base_dir)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(${readlink_cmd} -e ${log_config})" : ''
    def java_mem_opts = get_java_mem_opts(mem_gb)
    def cache_size_arg = cache_size ? "--cacheSize ${cache_size}" : ''
    def concurrency_arg = get_concurrency_arg(concurrency, cpus)
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def mask_tags_arg = mask_tags ? "--masks-tags ${mask_tags}" : ''
    def mask_excluded_tags_arg = mask_excluded_tags ? "--masks-excluded-tags ${mask_excluded_tags}" : ''
    def mask_terms_arg = mask_terms ? "--masks-terms ${mask_terms}" : ''
    def mask_excluded_terms_arg = mask_excluded_terms ? "--excluded-masks-terms ${mask_excluded_terms}" : ''
    def targets_library_arg = targets_library ? "--targets-libraries ${targets_library}" : ''
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def target_tags_arg = target_tags ? "--targets-tags ${target_tags}" : ''
    def target_excluded_tags_arg = target_excluded_tags ? "--targets-excluded-tags ${target_excluded_tags}" : ''
    def target_terms_arg = target_terms ? "--targets-terms ${target_terms}" : ''
    def target_excluded_terms_arg = target_excluded_terms ? "--excluded-targets-terms ${target_excluded_terms}" : ''
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''
    def mask_th_arg = mask_th ? "--maskThreshold ${mask_th}" : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''
    def matches_tags_arg = matches_tags ? "--match-tags ${matches_tags}" : ''
    def masks_processing_tags_arg = masks_processing_tags ? "--masks-processing-tags ${get_processing_tags_arg(masks_processing_tags)}" : ''
    def targets_processing_tags_arg = targets_processing_tags ? "--targets-processing-tags ${get_processing_tags_arg(targets_processing_tags)}" : ''
    def pixel_percent_ratio_arg = pixel_percent_ratio && pixel_percent_ratio > 0 ? "--pctPositivePixels ${pixel_percent_ratio}" : ''
    def with_bidirectional_matching_arg = with_bidirectional_matching ? '--use-bidirectional-matching' : ''
    def cancel_prev_scores_arg = cancel_prev_scores ? '--cancel-previous-gradient-scores' : ''
    def sleep_stmt = start_delay ? "sleep ${start_delay}" : ""

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

    CMD=(
        ${app_runner} java
        ${java_opts} ${java_mem_opts}
        \${LOG_CONFIG_ARG}
        -jar ${java_app}
        ${cache_size_arg}
        gradientScores
        --config ${db_config_file}
        ${concurrency_arg}
        ${alignment_space_arg}
        --masks-libraries ${masks_arg}
        ${masks_published_names_arg}
        ${mask_tags_arg}
        ${mask_excluded_tags_arg}
        ${mask_terms_arg}
        ${mask_excluded_terms_arg}
        ${targets_library_arg}
        ${targets_published_names_arg}
        ${target_tags_arg}
        ${target_excluded_tags_arg}
        ${target_terms_arg}
        ${target_excluded_terms_arg}
        ${processing_size_arg}
        ${mirror_flag_arg}
        ${mask_th_arg}
        --processing-tag ${ga_processing_tag}
        ${matches_tags_arg}
        ${masks_processing_tags_arg}
        ${targets_processing_tags_arg}
        ${pixel_percent_ratio_arg}
        ${with_bidirectional_matching_arg}
        ${cancel_prev_scores_arg}
        --nBestLines ${top_best_line_matches}
        --nBestSamplesPerLine ${top_best_sample_matches_per_line}
        --nBestMatchesPerSample ${top_best_matches_per_sample}
    )

    # random delay to prevent choking the db server
    ${sleep_stmt}

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

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
