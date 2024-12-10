include { VALIDATE_EXPORT} from '../modules/local/validate-export/main'

workflow {

    VALIDATE_EXPORT(
        Channel.of([ 
            params.data_version,
            file(params.base_export_dir),
            params.release_dirname,
        ]),
        params.cpus,
        params.mem_gb,
        params.ray_cluster_address
    )

}
