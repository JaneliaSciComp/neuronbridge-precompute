include { DELETE_CDMATCHES       } from '../modules/local/delete-cdmatches/main'
include { DBQUERY as COUNT_MASKS } from '../modules/local/dbquery/main'

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
    def delete_inputs = unique_masks_count
    | flatMap { anatomical_area, masks_library, nmasks ->
        def delete_jobs = partition_work(nmasks, params.delete_batch_size)
        log.info "${nmasks} masks generated ${delete_jobs.size} jobs"
        delete_jobs
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
                def first_job_idx = params.first_job > 0 ? params.first_job : 1
                def last_job_idx = params.last_job > 0 ? params.last_job : delete_jobs.size

                def excluded_first_job_idx = params.excluded_first_job > 0 ? params.excluded_first_job : delete_jobs.size
                def excluded_last_job_idx = params.excluded_last_job > 0 ? params.excluded_last_job : 1

                def job_is_included = is_job_id_in_process_list(job_idx,
                                                                params.job_list,
                                                                first_job_idx,
                                                                last_job_idx)
                // for excluded jobs we only check if the total jobs is > 1
                def job_is_excluded = delete_jobs.size > 1 &&
                                      is_job_id_in_process_list(job_idx,
                                                                params.excluded_job_list,
                                                                excluded_first_job_idx,
                                                                excluded_last_job_idx)
                return job_is_included && !job_is_excluded
            }
    }
    delete_inputs.subscribe {
        log.debug "Delete: $it"
    }
    DELETE_CDMATCHES(delete_inputs,
       [
           params.app ? file(params.app) : [],
           params.log_config ? file(params.log_config) : [],
           params.tool_runner,
           params.readlink_cmd,
       ],
       db_config_file,
       params.cpus,
       params.mem_gb,
       params.java_opts,
       [
            params.delete_concurrency,
            params.masks_published_names,
            params.mask_terms,
            params.mask_excluded_terms,
            params.targets_published_names,
            params.target_terms,
            params.target_excluded_terms,
            params.delete_processing_size,
            params.include_matches_with_gradscore,
            params.delete_only,
       ],
    )
}
