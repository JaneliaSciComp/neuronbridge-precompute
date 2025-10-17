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
        def validation_jobs = partition_work(nmips, params.validation_batch_size)
        log.info "Partition validation for ${nmips} ${mips_libraries} mips into ${validation_jobs.size} jobs"
        validation_jobs
            .withIndex()
            .collect { job, idx ->
                def (job_offset, job_size) = job
                [
                    idx+1, // jobs are 1-indexed
                    anatomical_area,
                    mips_libraries,
                    job_offset,
                    job_size
                ]
            }
            .findAll {
                def (job_idx) = it
                def first_job_idx = params.first_job > 0 ? params.first_job : 1
                def last_job_idx = params.last_job > 0 ? params.last_job : validation_jobs.size

                def excluded_first_job_idx = params.excluded_first_job > 0 ? params.excluded_first_job : validation_jobs.size
                def excluded_last_job_idx = params.excluded_last_job > 0 ? params.excluded_last_job : 1

                def job_is_included = is_job_id_in_process_list(job_idx,
                                                                params.job_list,
                                                                first_job_idx,
                                                                last_job_idx)
                def job_is_excluded = validation_jobs.size > 1 &&
                                      is_job_id_in_process_list(job_idx,
                                                                params.excluded_job_list,
                                                                excluded_first_job_idx,
                                                                excluded_last_job_idx)
                return job_is_included && !job_is_excluded
            }
    }
    validation_inputs.subscribe {
        log.debug "Run validation: $it"
    }
    VALIDATE(validation_inputs, // [index, area, libs, range ]
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
            params.validate_data_releases,
            params.validate_published_names,
            params.validate_samples,
            params.validate_mip_ids,
            params.validate_tags,
            params.mip_excluded_tags,
            params.mip_terms,
            params.mip_excluded_terms,
            params.jacs_url,
            params.jacs_authorization,
            params.jacs_read_batch_size,
            params.validation_processing_size,
            params.validation_error_tag,
       ],
       params.mips_base_dir
    )
}
