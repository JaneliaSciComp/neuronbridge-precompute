include { 
    area_to_alignment_space;
    get_lib_arg;
} from '../../../nfutils/utils'

process CDS {
    cpus { cds_cpus }
    memory "${cds_mem_gb} GB"
    clusterOptions { task.ext.cluster_opts }

    input:
    tuple val(anatomical_area),
          val(masks_library),
          val(masks_offset),
          val(masks_length),
          val(targets_library),
          val(targets_offset),
          val(targets_length)
    tuple path(app_jar), val(cds_runner)
    path(db_config_file)
    val(cds_cpus)
    val(cds_mem_gb)
    val(java_opts)
    tuple val(cds_processing_tag),
          val(mirror_flag),
          val(mask_th),
          val(target_th),
          val(pix_color_fluctuation),
          val(xy_shift),
          val(pct_pos_pixels),
          val(processing_size)


    script:
    def masks_arg = get_lib_arg(masks_library, masks_offset, masks_length)
    def targets_arg = get_lib_arg(targets_library, targets_offset, targets_length)
    def java_mem_opts = "-Xmx${cds_mem_gb}G -Xms${cds_mem_gb}G"
    def alignment_space = area_to_alignment_space(anatomical_area)
    def mirror_flag_arg = mirror_flag ? '--mirrorMask' : ''
    def mask_th_arg = mask_th ? "--maskThreshold ${mask_th}" : ''
    def target_th_arg = target_th ? "--dataThreshold ${target_th}" : ''
    def pix_color_fluctuation_arg = pix_color_fluctuation ? "--pixColorFluctuation ${pix_color_fluctuation}" : ''
    def xy_shift_arg = xy_shift ? "--xyShift ${xy_shift}" : ''
    def pct_pos_pixels_arg = pct_pos_pixels ? "--pctPositivePixels ${pct_pos_pixels}" : ''
    def processing_size_arg = processing_size ? "-ps ${processing_size}" : ''

    """
    ${cds_runner} java \
        ${java_opts} ${java_mem_opts} \
        -jar ${app_jar} \
        colorDepthSearch \
        --config ${db_config_file} \
        -as ${alignment_space} \
        -m ${masks_arg} \
        -i ${targets_arg} \
        --processing-tag ${cds_processing_tag} \
        ${mirror_flag_arg} \
        ${mask_th_arg} \
        ${target_th_arg} \
        ${pix_color_fluctuation_arg} \
        ${xy_shift_arg} \
        ${pct_pos_pixels_arg} \
        ${processing_size_arg}

    """
}
