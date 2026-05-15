include { UPLOAD                   } from '../modules/local/upload/main'
include { get_values_as_collection } from '../nfutils/utils'

workflow {

    def upload_types = get_values_as_collection(params.upload_type)
    def anatomical_areas = get_values_as_collection(params.upload_anatomical_areas)
    def dry_run = Boolean.valueOf(params.dry_run)
    def upload_inputs = channel.of(
        [
            params.base_data_dir,
            params.release_dirname,
            params.data_version,
        ]
    )
    upload_inputs.subscribe { it -> log.debug "Upload inputs (dry run is ${dry_run}): $it" }
    def upload_results = UPLOAD(
        upload_inputs,
        params.aws_runner,
        params.upload_cpus,
        params.upload_mem_gb,
        params.upload_bucket,
        anatomical_areas,
        upload_types,
        params.readlink_cmd,
        dry_run,
    )

    upload_results | view
}
