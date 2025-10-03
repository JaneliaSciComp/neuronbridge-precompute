include { PREPARE_VARIANTS_FOR_MIPSTORE } from '../modules/local/prepare-variants-for-libstore/main'
include { COPY_VARIANTS_TO_LIBSTORE     } from '../modules/local/copy-variants-to-libstore/main'

workflow {
    def app_args = [
        params.app ? file(params.app) : [],
        params.log_config ? file(params.log_config) : [],
        params.tool_runner,
        params.readlink_cmd,
    ]
    def variants_output_dir = params.variants_output_dir
        ? file(params.variants_output_dir)
        : []
    def cdmips_paths = [
        params.display_cdm_location,
        params.searchable_cdm_location,
        params.grad_location,
        params.zgap_location,
    ]
    def lib_variants = PREPARE_VARIANTS_FOR_MIPSTORE(
        Channel.of(
            [
                params.anatomical_area,
                params.import_library,
                variants_output_dir,
                file(params.variants_input_dir),
                params.display_cdm_location,
                params.searchable_cdm_location,
                params.grad_location,
                params.zgap_location,
                params.vol_segmentation_location,
                params.junk_location,
                params.variants_json_file,
            ]
        ),
        app_args,
        params.cpus,
        params.mem_gb,
        params.java_opts,
        [
            params.jacs_url,
            params.jacs_authorization,
        ],
        get_data_paths(cdmips_paths),
    ) // [area, library, output]
    | map {
        def (anatomical_area, library, variants_json) =it
        def r = [
            anatomical_area,
            library,
            variants_json,
            file(params.libstore_dir),
            params.display_cdm_dest,
            params.searchable_cdm_dest,
            params.grad_dest,
            params.zgap_dest,
            params.vol_segmentation_dest,
            params.junk_dest,
            params.ignore_source_cdms,
            params.force_copy,
            params.dry_run,
        ]
        log.debug "Variants to copy input: $r"
        r
    }

    COPY_VARIANTS_TO_LIBSTORE(
        lib_variants,
        app_args,
        params.cpus,
        params.mem_gb,
        params.java_opts,
        file(params.variants_input_dir),
    )
}

def get_data_paths(paths) {
    paths
        .findAll { it && it[0] == '/'}
        .collect { file(it) }
}
