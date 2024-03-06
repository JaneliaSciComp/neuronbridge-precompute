include { GA } from '../modules/local/ga/main.nf'
include { DBQUERY as COUNT_MASKS } from '../modules/local/dbquery/main.nf'

include { partition_work } from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def unique_masks_count = COUNT_MASKS(
        Channel.of([
            params.anatomical_area,
            params.masks_library,
            params.masks_published_names,
            params.masks_tags,
            true,
        ]),
        db_config_file,
    )

    // split the work
    def gradscore_inputs = unique_masks_count
    | flatMap { anatomical_area, masks_library, nmasks ->
        def gradscore_jobs = partition_work(nmasks, params.gradscore_batch_size)
        gradscore_jobs.collect { job_offset, job_size ->
            [
                anatomical_area,
                masks_library,
                job_offset,
                job_size,
                params.targets_library
            ]
        }
    }

    gradscore_inputs | view

}
