include { PREPARE_VARIANTS_FOR_MIPSTORE } from '../modules/local/prepare-variants-for-libstore/main'
include { COPY_VARIANTS_TO_LIBSTORE     } from '../modules/local/copy-variants-to-libstore/main'

workflow {
    def app_args = [
        params.app ? file(params.app) : [],
        params.log_config ? file(params.log_config) : [],
        params.tool_runner,
    ]
    def variants_output_dir = params.variants_output_dir
        ? file(params.variants_output_dir)
        : []
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
        ]
    ) // [area, library, output]
    | map {
        def (anatomical_area, library, variants_json) =it
        [
            anatomical_area,
            library,
            variants_json,
            file(params.libstore_dir),
            params.display_cdm_dest,
            params.searchable_cdm_dest,
            params.grad_dest,
            params.zgap_dest,
            params.dry_run,
        ]
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