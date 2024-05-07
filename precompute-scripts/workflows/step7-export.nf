include { EXPORT } from '../modules/local/export/main.nf'
include { DBQUERY as COUNT_MIPS } from '../modules/local/dbquery/main.nf'

include {
    partition_work;
    is_job_id_in_process_list;
} from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)
    def exported_mask_libs = get_exported_mask_libs(params.export_type, params.exported_mask_libs)

    def unique_mips_count = COUNT_MIPS(
        Channel.of([
            params.anatomical_area,
            exported_mask_libs,
            params.mip_published_names,
            params.data_version,
            params.mip_excluded_tags,
            true,
        ]),
        db_config_file,
    )

    unique_mips_count.subscribe {
        log.debug "MIPs to export count: $it"
    }

    // split the work
    def export_inputs = unique_mips_count
    | flatMap { anatomical_area, mips_libraries, nmips ->
        def export_jobs = partition_work(nmips, params.export_batch_size)
        log.debug "Partition export for ${nmips} ${mips_libraries} mips into ${export_jobs.size} jobs"
        export_jobs
            .withIndex()
            .collect { job, idx ->
                def (job_offset, job_size) = job
                [
                    idx+1, // jobs are 1-indexed
                    params.data_version,
                    anatomical_area,
                    file(params.base_export_dir),
                    get_relative_output_dir(params.export_type),
                    mips_libraries,
                    get_exported_target_libs(params.export_type, params.exported_target_libs),
                    job_offset,
                    job_size
                ]
            }
            .findAll {
                def (job_idx) = it
                is_job_id_in_process_list(job_idx, params.job_list, params.first_job, params.last_job)
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
            params.mip_excluded_tags,
            params.target_mip_excluded_tags,
            params.jacs_url,
            params.jacs_authorization,
            params.default_image_store,
            params.image_stores_map,
            params.jacs_read_batch_size,
            params.export_processing_size,
       ]
    )
}

def get_exported_mask_libs(export_type, exported_mask_libs) {
    if (exported_mask_libs) {
        return exported_mask_libs
    }
    switch(export_type) {
        case 'EM_CD_MATCHES':
            return params.all_brain_and_vnc_EM_libraries.join(',')
        case 'LM_CD_MATCHES':
            return params.all_brain_and_vnc_LM_libraries.join(',')
        case 'EM_PPP_MATCHES':
            return params.all_brain_and_vnc_EM_libraries.join(',')
        case 'EM_MIPS':
            return params.all_brain_and_vnc_EM_libraries.join(',')
        case 'LM_MIPS':
            return params.all_brain_and_vnc_LM_libraries.join(',')
        default: throw new IllegalArgumentException("Invalid export type: ${export_type}")
    }
}

def get_exported_target_libs(export_type, exported_target_libs) {
    if (exported_target_libs) {
        return exported_target_libs
    }
    switch(export_type) {
        case 'EM_CD_MATCHES':
            return params.all_brain_and_vnc_LM_libraries.join(',')
        case 'LM_CD_MATCHES':
            return params.all_brain_and_vnc_EM_libraries.join(',')
        case 'EM_PPP_MATCHES':
            return params.all_brain_and_vnc_LM_libraries.join(',')
        case 'EM_MIPS':
            return ''
        case 'LM_MIPS':
            return ''
        default: throw new IllegalArgumentException("Invalid export type: ${export_Type}")
    }
}

def get_relative_output_dir(export_type) {
    switch(export_type) {
        case 'EM_CD_MATCHES':
            return 'cdmatches/em-vs-lm'
        case 'LM_CD_MATCHES':
            return 'cdmatches/lm-vs-em'
        case 'EM_PPP_MATCHES':
            return 'pppmatches/em-vs-lm'
        case 'EM_MIPS':
            return 'mips/embodies'
        case 'LM_MIPS':
            return 'mips/lmlines'
        default: throw new IllegalArgumentException("Invalid export type: ${export_type}")
    }
}