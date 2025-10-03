include { CDS                      } from '../modules/local/cds/main'
include { DBQUERY as COUNT_MASKS   } from '../modules/local/dbquery/main'
include { DBQUERY as COUNT_TARGETS } from '../modules/local/dbquery/main'

include {
    is_job_id_in_process_list;
    partition_work;
} from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def masks_count = COUNT_MASKS(
        Channel.of([
            params.anatomical_area,
            params.masks_library,
            params.masks_published_names,
            params.masks_mip_ids,
            params.masks_tags,
            params.masks_excluded_tags,
            params.mask_terms,
            params.mask_excluded_terms,
            '', // we don't care about processing tags for cds
            false,
        ]),
        db_config_file,
    )

    def targets_count = COUNT_TARGETS(
        Channel.of([
            params.anatomical_area,
            params.targets_library,
            params.targets_published_names,
            params.targets_mip_ids,
            params.targets_tags,
            params.targets_excluded_tags,
            params.target_terms,
            params.target_excluded_terms,
            '',
            false,
        ]),
        db_config_file,
    )

    // split the work
    def cds_inputs = masks_count
    | join(targets_count)
    | flatMap { anatomical_area, masks_library, nmasks, targets_library, ntargets -> 
        def masks_jobs = partition_work(nmasks, params.cds_mask_batch_size)
        def targets_jobs = partition_work(ntargets, params.cds_target_batch_size)
        def all_cds_jobs = [masks_jobs, targets_jobs].combinations()
        log.info "Partition color depth search for ${nmasks} ${masks_library} masks and ${ntargets} ${targets_library} targets into ${all_cds_jobs.size} jobs"
        all_cds_jobs
            .withIndex()
            .collect { mtpair, idx ->
                def (masks_limits, targets_limits) = mtpair
                def (masks_offset, masks_size) = masks_limits
                def (targets_offset, targets_size) = targets_limits
                [
                    idx+1, // jobs are 1-indexed
                    anatomical_area,
                    masks_library,
                    masks_offset,
                    masks_size,
                    targets_library,
                    targets_offset,
                    targets_size,
                ]
            }
            .findAll {
                def (job_idx) = it
                def first_job_idx = params.first_job > 0 ? params.first_job : 1
                def last_job_idx = params.last_job > 0 ? params.last_job : all_cds_jobs.size()

                def excluded_first_job_idx = params.excluded_first_job > 0 ? params.excluded_first_job : all_cds_jobs.size() + 1
                def excluded_last_job_idx = params.excluded_last_job > 0 ? params.excluded_last_job : -1

                def job_is_included = is_job_id_in_process_list(job_idx,
                                                                params.job_list,
                                                                first_job_idx,
                                                                last_job_idx)
                def job_is_excluded = all_cds_jobs.size > 1 &&
                                      is_job_id_in_process_list(job_idx,
                                                                params.excluded_job_list,
                                                                excluded_first_job_idx,
                                                                excluded_last_job_idx)
                return job_is_included && !job_is_excluded
            }
    }
    cds_inputs.subscribe {
        log.debug "Run cds: $it"
    }
    CDS(cds_inputs,
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
            params.cds_processing_tag,
            params.cds_concurrency,
            params.cds_cache_size,
            params.masks_published_names,
            params.masks_tags,
            params.mask_terms,
            params.mask_excluded_terms,
            params.targets_published_names,
            params.targets_tags,
            params.target_terms,
            params.target_excluded_terms,
            params.mirror_flag,
            params.mask_th,
            params.target_th,
            params.pix_color_fluctuation,
            params.xy_shift,
            params.pct_pos_pixels,
            params.cds_processing_size,
            params.cds_write_batch_size,
            params.cds_parallelize_write_results,
        ],
        file(params.mips_base_dir),
        params.update_cds_matches,
    )

}
