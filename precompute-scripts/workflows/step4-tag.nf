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
            params.data_tags,
            params.data_labels,
            params.processing_tags,
            params.excluded_tags,
            params.published_names,
            params.mip_ids,
       ]
    )
}
