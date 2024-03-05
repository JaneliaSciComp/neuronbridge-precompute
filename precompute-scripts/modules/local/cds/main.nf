process CDS {
    cpus: { cds_cpus }

    input:
    tuple val(anatomical_area),
          val(masks_library),
          val(targets_library)
    path(app_jar)
    val(cds_cpus)
    val(cds_mem_gb)

    script:
    """
    java -jar ${app_jar)
    """
}
