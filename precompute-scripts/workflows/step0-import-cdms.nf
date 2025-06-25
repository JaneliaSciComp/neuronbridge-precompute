include { IMPORT_CDMS } from '../modules/local/import-cdms/main'

workflow {
    def app_args = [
        params.app ? file(params.app) : [],
        params.log_config ? file(params.log_config) : [],
        params.tool_runner,
    ]
    def db_config_file = file(params.db_config)
    def cdmips_paths = [
        params.library_base_dir,
        params.display_cdm_location,
        params.searchable_cdm_location,
        params.grad_location,
        params.zgap_location,
        params.junk_location,
    ]
    IMPORT_CDMS(
        Channel.of(
            [
                params.anatomical_area,
                params.import_library,
                params.library_base_dir,
                params.source_cdm_location,
                params.searchable_cdm_location,
                params.grad_location,
                params.zgap_location,
                params.vol_segmentation_location,
                params.junk_location,
            ]
        ),
        app_args,
        db_config_file,
        params.cpus,
        params.mem_gb,
        params.java_opts,
        [
            params.jacs_url,
            params.jacs_authorization,
            params.import_tag,
            params.junk_import_tag,
            params.import_mips,
            params.import_published_names,
            params.import_releases,
            params.excluded_libraries,
            params.included_neurons,
            params.excluded_neurons,
        ],
        get_data_paths(cdmips_paths),
    ) // [area, library, import_tag]

}


def get_data_paths(paths) {
    paths
        .findAll { it && it[0] == '/'}
        .collect { file(it) }
}
