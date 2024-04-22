include { EXPORT } from '../modules/local/export/main.nf'
include { DBQUERY as COUNT_MIPS } from '../modules/local/dbquery/main.nf'

include { partition_work } from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def unique_mips_count = COUNT_MIPS(
        Channel.of([
            params.anatomical_area,
            params.mip_libraries,
            params.mip_published_names,
            params.mip_tags,
            params.mip_excluded_tags,
            true,
        ]),
        db_config_file,
    )

    // split the work
    def export_inputs = unique_mips_count
    | flatMap { anatomical_area, mips_libraries, nmips ->
        def export_jobs = partition_work(nmips, params.export_batch_size)
        log.info "Partition export for ${nmips} ${mips_libraries} mips into ${export_jobs.size} jobs"
        export_jobs
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
                // first_job and last_job parameters are 1-index and they are inclusive
                (params.first_job <= 0 || job_idx >= params.first_job) &&
                (params.last_job <= 0 || job_idx <= params.last_job)
            }
    }
    export_inputs.subscribe {
        log.debug "Run export: $it"
    }
    EXPORT(export_inputs,
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
            params.export_type,
            params.exported_tags,
            params.excluded_tags,
            params.jacs_url,
            params.jacs_authorization,
            params.jacs_read_batch_size,
       ]
    )
}
