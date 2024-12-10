process VALIDATE_EXPORT {
    container { task.ext.container }
    conda { task.ext.conda }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(data_version),
          path(base_export_dir),
          val(release_dirname),
    val(cpus)
    val(mem_gb)
    val(ray_cluster_address)

    script:
    def release_dir = release_dirname ?: "v${data_version}"
    def cluster_arg = ray_cluster_address ? "--cluster ray_cluster_address" : ''

    """
    echo "\$(date) Run validate export on \$(hostname -s)"
    release_export_dir="${base_export_dir}/${release_dir}"

    python -m neuronbridge.validate_ray \
      --data_path \${release_export_dir} \
      --cores ${cpus} \
      ${cluster_arg}


    echo "\$(date) Completed validate export on \$(hostname -s)"
    """
}
