include {
    area_to_alignment_space;
    get_concurrency_arg;
    get_java_mem_opts;
    get_lib_arg;
} from '../../../nfutils/utils'

process NORMALIZE_SCORES {
    container { task.ext.container }
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
    tuple val(normalize_ga_processing_tag),
          val(concurrency),
          val(masks_published_names),
          val(mask_terms),
          val(mask_excluded_terms),
          val(targets_published_names),
          val(target_terms),
          val(target_excluded_terms),
          val(processing_size)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = get_java_mem_opts(mem_gb)
    def concurrency_arg = get_concurrency_arg(concurrency, cpus)
    def alignment_space = area_to_alignment_space(anatomical_area)
    def alignment_space_arg = alignment_space ? "-as ${alignment_space}" : ''
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def mask_terms_arg = mask_terms ? "--masks-terms ${mask_terms}" : ''
    def mask_excluded_terms_arg = mask_excluded_terms ? "--excluded-masks-terms ${mask_excluded_terms}" : ''
    def targets_library_arg = targets_library ? "--targets-libraries ${targets_library}" : ''
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def target_terms_arg = target_terms ? "--targets-terms ${target_terms}" : ''
    def target_excluded_terms_arg = target_excluded_terms ? "--excluded-targets-terms ${target_excluded_terms}" : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''

    """
    echo "\$(date) Run ${anatomical_area} normalize-score job: ${job_id} on \$(hostname -s)"

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG="${log_config_arg}"
    else
        LOG_CONFIG_ARG=
    fi

    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        \${LOG_CONFIG_ARG} \
        -jar ${java_app} \
        mormalizeGradientScores \
        --config ${db_config_file} \
        ${concurrency_arg} \
        ${alignment_space_arg} \
        --masks-libraries ${masks_arg} \
        ${masks_published_names_arg} \
        ${mask_terms_arg} \
        ${mask_excluded_terms_arg} \
        ${targets_library_arg} \
        ${targets_published_names_arg} \
        ${target_terms_arg} \
        ${target_excluded_terms_arg} \
        ${processing_size_arg} \
        --processing-tag ${normalize_ga_processing_tag} \

    echo "\$(date) Completed ${anatomical_area} normalize-score job: ${job_id} on \$(hostname -s)"
    """
}
