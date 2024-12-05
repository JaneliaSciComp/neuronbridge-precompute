include { NORMNALIZE_GA } from '../modules/local/normalize-ga/main.nf'
include { DBQUERY as COUNT_MASKS } from '../modules/local/dbquery/main.nf'

include {
    is_job_id_in_process_list;
    partition_work;
} from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def unique_masks_count = COUNT_MASKS(
        Channel.of([
            params.anatomical_area,
            params.masks_library,
            params.masks_published_names,
            params.masks_mip_ids,
            params.masks_tags,
            params.masks_excluded_tags,
            params.mask_terms,
            params.mask_excluded_terms,
            params.masks_processing_tags,
            true,
        ]),
        db_config_file,
    )

    // split the work
    def normalize_gradscore_inputs = unique_masks_count
    | flatMap { anatomical_area, masks_library, nmasks ->
        def gradscore_jobs = partition_work(nmasks, params.normalize_ga_batch_size)
        gradscore_jobs
            .withIndex()
            .collect { job, idx ->
                def (job_offset, job_size) = job
                [
                    idx+1, // jobs are 1-indexed
                    anatomical_area,
                    masks_library,
                    job_offset,
                    job_size,
                    params.targets_library
                ]
            }
            .findAll {
                def (job_idx) = it
                is_job_id_in_process_list(job_idx, params.job_list, params.first_job, params.last_job)
            }
    }
    normalize_gradscore_inputs.subscribe {
        log.debug "Normalize grad score: $it"
    }
    NORMALIZE_GA(normalize_gradscore_inputs,
       [
           params.app ? file(params.app) : [],
           params.log_config ? file(params.log_config) : [],
           params.tool_runner,
       ],
       db_config_file,
       params.cpus,
       params.mem_gb,
       params.java_opts,
       [
            params.normalize_score_processing_tag,
            params.masks_published_names,
            params.targets_published_names,
            params.normalize_score_batch_size,
       ],
    )
}
