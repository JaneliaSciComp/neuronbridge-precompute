include { 
    area_to_alignment_space;
} from '../../../nfutils/utils'

process COPY_VARIANTS_TO_LIBSTORE {
    container { task.ext.container }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(anatomical_area),
          val(library_name),
          path(variants_json_file),
          path(libstore_base_dir),
          val(display_cdm_location),
          val(searchable_cdm_location),
          val(grad_location),
          val(zgap_location),
          val(vol_segmentation_location),
          val(junk_location),
          val(ignore_source_cdms),
          val(force_copy),
          val(dry_run)
    tuple path(app_jar),
          path(log_config),
          val(app_runner)
    val(cpus)
    val(mem_gb)
    val(java_opts)
    path(data_paths) // this argument is only sent to ensure all needed volumes are available
    
    output:
    env(full_libstore_dir)

    when:
    task.ext.when == null || task.ext.when

    script:
    def java_app = app_jar ?: '/app/colormipsearch-jar-with-dependencies.jar'
    def log_config_arg = log_config ? "-Dlog4j.configuration=file://\$(readlink -e ${log_config})" : ''
    def java_mem_opts = "-Xmx${mem_gb-1}G -Xms${mem_gb-1}G"
    def alignment_space = area_to_alignment_space(anatomical_area)
    def libstore_dir = "${libstore_base_dir}/${alignment_space}/${library_name}"
    def source_cdm_mapping_arg = !ignore_source_cdms ? "--surjective-variants-mappingcdm=${display_cdm_location}" : ''
    def searchable_mapping_arg = "--surjective-variants-mappingsegmentation=${searchable_cdm_location}"
    def grad_mapping_arg = grad_location ? "--surjective-variants-mappinggrad=${grad_location}" : ''
    def zgap_mapping_arg = zgap_location ? "--surjective-variants-mappingzgap=${zgap_location}" : ''
    def vol_segmentation_mapping_arg = vol_segmentation_location ? "--surjective-variants-mappingvolseg=${vol_segmentation_location}" : ''
    def junk_mapping_arg = junk_location ? "--surjective-variants-mappingjunk=${junk_location}" : ''

    def dry_run_arg = dry_run ? '-n' : ''
    def force_copy_arg = force_copy ? '--force' : ''

    """
    echo "\$(date) Copy ${library_name} variants on \$(hostname -s)"

    if [[ ${log_config} != "" && -f ${log_config} ]];  then
        LOG_CONFIG_ARG="${log_config_arg}"
    else
        LOG_CONFIG_ARG=
    fi

    full_libstore_dir=\$(readlink -m ${libstore_dir})
    if [[ ! -e \${full_libstore_dir} ]]; then
        mkdir -p \${full_libstore_dir}
    fi

    CMD=(
        ${app_runner} java
        ${java_opts} ${java_mem_opts}
        \${LOG_CONFIG_ARG}
        -jar ${java_app}
        copyToMipsStore
        -i ${variants_json_file}
        -od \${full_libstore_dir}
        ${source_cdm_mapping_arg}
        ${searchable_mapping_arg}
        ${grad_mapping_arg}
        ${zgap_mapping_arg}
        ${vol_segmentation_mapping_arg}
        ${junk_mapping_arg}
        ${force_copy_arg}
        ${dry_run_arg}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    echo "\$(date) Completed copying ${library_name} variants on \$(hostname -s)"
    """
}
