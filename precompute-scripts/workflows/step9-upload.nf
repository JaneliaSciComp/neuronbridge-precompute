include { UPLOAD                   } from '../modules/local/upload/main'
include { get_values_as_collection } from '../nfutils/utils'

workflow {

    def upload_types = get_values_as_collection(params.upload_type)
    def anatomical_areas = get_values_as_collection(params.upload_anatomical_areas)
    def upload_inputs = Channel.of(
        [
            params.base_data_dir,
            params.data_version,
        ]
    )
    def upload_results = UPLOAD(
        upload_inputs,
        params.aws_runner,
        params.upload_cpus,
        params.upload_mem_gb,
        params.upload_bucket,
        anatomical_areas,
        upload_types,
        params.dry_run,
    )

    upload_results | view
}
