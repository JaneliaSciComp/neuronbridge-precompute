include { GA                     } from '../modules/local/ga/main'
include { DBQUERY as COUNT_MASKS } from '../modules/local/dbquery/main'

include {
    is_job_id_in_process_list;
    partition_work;
} from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def masks_count_input = Channel.of([
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
        ])

    def unique_masks_count = COUNT_MASKS(masks_count_input, db_config_file)

    // split the work
    def gradscore_inputs = unique_masks_count
    | flatMap { anatomical_area, masks_library, nmasks ->
        def gradscore_jobs = partition_work(nmasks, params.gradscore_batch_size)
        log.info "Partition gradient score for ${nmasks} ${masks_library} masks into ${gradscore_jobs.size} jobs"
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
    gradscore_inputs.subscribe {
        log.debug "Run grad score: $it"
    }
    GA(gradscore_inputs,
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
            params.gradscore_processing_tag,
            params.gradscore_concurrency,
            params.gradscore_cache_size,
            params.masks_published_names,
            params.mask_terms,
            params.mask_excluded_terms,
            params.targets_published_names,
            params.target_terms,
            params.target_excluded_terms,
            params.mirror_flag,
            params.top_best_line_matches,
            params.top_best_sample_matches_per_line,
            params.top_best_matches_per_sample,
            params.ga_processing_size,
            params.matches_tags,
            params.masks_processing_tags,
            params.targets_processing_tags,
            params.pct_pos_pixels,
            params.with_bidirectional_matching,
       ],
       file(params.mips_base_dir),
    )
}
