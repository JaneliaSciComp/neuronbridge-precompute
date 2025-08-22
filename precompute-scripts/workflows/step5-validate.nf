include { VALIDATE              } from '../modules/local/validate/main'
include { DBQUERY as COUNT_MIPS } from '../modules/local/dbquery/main'

include {
    is_job_id_in_process_list;
    partition_work;
} from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def unique_mips_count = COUNT_MIPS(
        Channel.of([
            params.anatomical_area,
            params.validate_libs,
            params.validate_published_names,
            params.validate_mip_ids,
            params.validate_tags,
            params.mip_excluded_tags,
            params.mip_terms,
            params.mip_excluded_terms,
            params.mip_processing_tags,
            true,
        ]),
        db_config_file,
    )

    unique_mips_count.subscribe {
        log.debug "MIPs to validate count: $it"
    }

    // split the work
    def validation_inputs = unique_mips_count
    | flatMap { anatomical_area, mips_libraries, nmips ->
        def export_jobs = partition_work(nmips, params.export_batch_size)
        log.debug "Partition validation for ${nmips} ${mips_libraries} mips into ${export_jobs.size} jobs"
        export_jobs
            .withIndex()
            .collect { job, idx ->
                def (job_offset, job_size) = job
                [
                    idx+1, // jobs are 1-indexed
                    params.data_version,
                    anatomical_area,
                    mips_libraries,
                    job_offset,
                    job_size
                ]
            }
            .findAll {
                def (job_idx) = it
                def first_job_idx = params.first_job > 0 ? params.first_job : 1
                def last_job_idx = params.last_job > 0 ? params.last_job : gradscore_jobs.size

                def excluded_first_job_idx = params.excluded_first_job > 0 ? params.excluded_first_job : gradscore_jobs.size
                def excluded_last_job_idx = params.excluded_last_job > 0 ? params.excluded_last_job : 1

                def job_is_included = is_job_id_in_process_list(job_idx,
                                                                params.job_list,
                                                                first_job_idx,
                                                                last_job_idx)
                def job_is_excluded = is_job_id_in_process_list(job_idx,
                                                                params.excluded_job_list,
                                                                excluded_first_job_idx,
                                                                excluded_last_job_idx)
                return job_is_included && !job_is_excluded
            }
    }
    validation_inputs.subscribe {
        log.debug "Run validation: $it"
    }
    VALIDATE(validation_inputs,
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
            params.validate_samples,
            params.validate_data_releases,
            params.validate_tags,
            params.target_mip_excluded_tags,
            params.jacs_url,
            params.jacs_authorization,
            params.default_image_store,
            params.image_stores_map,
            params.jacs_read_batch_size,
            params.export_processing_size,
       ],
       params.mips_base_dir
    )
}
