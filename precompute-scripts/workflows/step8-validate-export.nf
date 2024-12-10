include { VALIDATE_EXPORT as VE} from '../modules/local/validate-export/main'

workflow {

    VE(
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
