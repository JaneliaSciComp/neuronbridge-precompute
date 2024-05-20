include { 
    area_to_alignment_space;
    get_lib_arg;
} from '../../../nfutils/utils'

process CDS {
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
          val(targets_library),
          val(targets_offset),
          val(targets_length)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    path(db_config_file)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    tuple val(cds_processing_tag),
          val(cache_size),
          val(masks_published_names),
          val(targets_published_names),
          val(mirror_flag),
          val(mask_th),
          val(target_th),
          val(pix_color_fluctuation),
          val(xy_shift),
          val(pct_pos_pixels),
          val(processing_size),
          val(write_batch_size),
          val(parallelize_write_results)
    path(mips_base_dir)
    val(update_matches)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-3.1.0-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j2.configuration=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def cache_size_arg = cache_size ? "--cacheSize ${cache_size}" : ''
    def concurrency_arg = cpus ? "--task-concurrency ${2 * cpus -1}" : ''
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def masks_published_names_arg = masks_published_names ? "--masks-published-names ${masks_published_names}" : ''
    def targets_arg = get_lib_arg(targets_library, targets_offset, targets_length)
    def targets_published_names_arg = targets_published_names ? "--targets-published-names ${targets_published_names}" : ''
    def alignment_space = area_to_alignment_space(anatomical_area)
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''
    def mask_th_arg = mask_th ? "--maskThreshold ${mask_th}" : ''
    def target_th_arg = target_th ? "--dataThreshold ${target_th}" : ''
    def pix_color_fluctuation_arg = pix_color_fluctuation ? "--pixColorFluctuation ${pix_color_fluctuation}" : ''
    def xy_shift_arg = xy_shift ? "--xyShift ${xy_shift}" : ''
    def pct_pos_pixels_arg = pct_pos_pixels ? "--pctPositivePixels ${pct_pos_pixels}" : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''
    def update_matches_arg = update_matches ? '--update-matches' : ''
    def parallelize_write_results_arg = parallelize_write_results ? '--parallel-write-results' : ''

    """
    echo "\$(date) Run ${anatomical_area} cds job: ${job_id} on \$(hostname -s)"
    mips_base_fullpath=\$(readlink ${mips_base_dir})
    echo "Mips base dir: \${mips_base_fullpath}"

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG=${log_config_arg}
    else
        LOG_CONFIG_ARG=
    fi

    ${app_runner} java \
        ${java_opts} ${java_mem_opts} \
        \${LOG_CONFIG_ARG} \
        -jar ${java_app} \
        ${cache_size_arg} \
        colorDepthSearch \
        --config ${db_config_file} \
        ${concurrency_arg} \
        -as ${alignment_space} \
        -m ${masks_arg} \
        ${masks_published_names_arg} \
        -i ${targets_arg} \
        ${targets_published_names_arg} \
        --processing-tag ${cds_processing_tag} \
        ${mirror_flag_arg} \
        ${mask_th_arg} \
        ${target_th_arg} \
        ${pix_color_fluctuation_arg} \
        ${xy_shift_arg} \
        ${pct_pos_pixels_arg} \
        ${processing_size_arg} \
        --write-batch-size ${write_batch_size} \
        ${parallelize_write_results_arg} \
        ${update_matches_arg}

    echo "\$(date) Completed ${anatomical_area} cds job: ${job_id} on \$(hostname -s)"
    """
}
