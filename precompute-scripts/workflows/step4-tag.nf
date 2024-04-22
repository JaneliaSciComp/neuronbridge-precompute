include { TAG } from '../modules/local/tag/main.nf'

workflow {
    TAG(
       [
            params.tag,
            params.anatomical_area,
       ],
       [
            params.app ? file(params.app) : [],
            params.log_config ? file(params.log_config) : [],
            params.tool_runner,
       ],
       file(params.db_config),
       params.cpus,
       params.mem_gb,
       params.java_opts,
       [
            params.mip_libraries,           
            params.mip_tags,
            params.mip_release_labels,
            params.mip_processing_tags,
            params.mip_excluded_tags,
            params.mip_published_names,
            params.mip_ids,
       ]
    )
}
