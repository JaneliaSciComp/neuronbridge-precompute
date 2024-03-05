process GA {
    cpus: { ga_cpus }

    input:
    tuple val(anatomical_area),
          val(masks_library),
          val(targets_library)
    path(app_jar)
    val(ga_cpus)
    val(ga_mem_gb)

    script:
    """
    java -jar ${app_jar)
    """
}
