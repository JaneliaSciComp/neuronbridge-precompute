/**
 DBQUERY queries the MIPs count from NeuronBridge.neuronMetadata collection.
*/

include {
    area_to_alignment_space;
    get_values_as_map;
} from '../../../nfutils/utils'

process DBQUERY {
    container { task.ext.container ?: 'docker.io/mongo:7.0.17' }
    label 'process_low'
    label 'prefer_local'

    input:
    tuple val(anatomical_area),
          val(library_names),
          val(published_names),
          val(mip_ids),
          val(mip_tags),
          val(excluded_tags),
          val(mips_terms),
          val(excluded_terms),
          val(processing_tags),
          val(unique_mips)

    path(db_config_file)
    
    output:
    tuple val(anatomical_area), val(library_names), env(mips_count_res)

    when:
    task.ext.when == null || task.ext.when

    script:
    def alignment_space = area_to_alignment_space(anatomical_area)
    def library_filter = library_names ? "libraryName: ${get_in_filter(library_names)}," : ''
    def as_filter = alignment_space ? "alignmentSpace: \"${alignment_space}\"," : ''
    def tag_filter
    if (mip_tags && excluded_tags) {
        tag_filter = """
        tags: {
            ${get_values_filter('in', mip_tags)},
            ${get_values_filter('nin', excluded_tags)},
        },
        """
    } else if (mip_tags) {
        tag_filter = "tags: ${get_in_filter(mip_tags)},"
    } else if (excluded_tags) {
        tag_filter = "tags: ${get_nin_filter(excluded_tags)},"
    } else {
        tag_filter = ''
    }
    def terms_filter
    if (mips_terms && excluded_terms) {
        terms_filter = """
        neuronTerms: {
            ${get_values_filter('in', mips_terms)},
            ${get_values_filter('nin', excluded_terms)},
        },
        """
    } else if (mips_terms) {
        terms_filter = "neuronTerms: ${get_in_filter(mips_terms)},"
    } else if (excluded_terms) {
        terms_filter = "neuronTerms: ${get_nin_filter(excluded_terms)},"
    } else {
        terms_filter = ''
    }
    def published_name_filter = published_names ? "publishedName: ${get_in_filter(published_names)}," : ''
    def mip_id_filter = mip_ids ? "mipId: ${get_in_filter(mip_ids)}," : ''
    def processing_tags_filter = processing_tags ? "${get_processing_tag_in_filter(processing_tags)}," : ''
    def with_usefull_input_files_filter = "'computeFiles.InputColorDepthImage': {\\\$exists: true},"
    def unique_pipeline = unique_mips 
        ? "{\\\$group: {_id: \"\\\$mipId\"}},"
        : ''
    """
    mongodb_server=\$(grep -e "MongoDB.Server=" ${db_config_file} | sed s/MongoDB.Server=//)
    mongodb_database=\$(grep -e "MongoDB.Database=" ${db_config_file} | sed s/MongoDB.Database=//)
    mongodb_authdatabase=\$(grep -e "MongoDB.AuthDatabase=" ${db_config_file} | sed s/MongoDB.AuthDatabase=//)
    mongodb_username=\$(grep -e "MongoDB.Username=" ${db_config_file} | sed s/MongoDB.Username=//)
    mongodb_password=\$(grep -e "MongoDB.Password=" ${db_config_file} | sed s/MongoDB.Password=//)
    mongodb_replicaset=\$(grep -e "MongoDB.ReplicaSet=" ${db_config_file} | sed s/MongoDB.ReplicaSet=//)
    if [[ "\${mongodb_replicaset}" == "" ]]; then
        replicaset_param=""
    else
        replicaset_param="&replicaSet=\${mongodb_replicaset}"
    fi

    mongosh "mongodb://\${mongodb_username}:\${mongodb_password}@\${mongodb_server}/\${mongodb_database}?authSource=\${mongodb_authdatabase}\${replicaset_param}" <<-EOF > mongo_output
    db.neuronMetadata.aggregate([
        {
            \\\$match: {
                ${as_filter}
                ${library_filter}
                ${tag_filter}
                ${terms_filter}
                ${published_name_filter}
                ${mip_id_filter}
                ${processing_tags_filter}
                ${with_usefull_input_files_filter}
            }
        },
        ${unique_pipeline}
        {
            \\\$count: "mips_count"
        },
    ])    
    EOF
    mips_count=\$(grep -o -e "mips_count.*[0-9]*" mongo_output | awk '{ print \$(NF-2)}')
    mips_count_res=\${mips_count:-0}
    """
}

/**
  This methods 
*/
def get_in_filter(list_as_str) {
    return "{${get_values_filter('in', list_as_str)}}"
}

def get_nin_filter(list_as_str) {
    return "{${get_values_filter('nin', list_as_str)}}"
}

def get_processing_tag_in_filter(ptags_as_str) {
    def ptags_map = get_values_as_map(ptags_as_str)
    ptags_map
        .collect { k, vs ->
            "\"processedTags.$k\": ${get_in_filter(vs)}"
        }
        .inject('') { arg, item ->
            arg ? "${arg}, ${item}" : item
        }
}

def get_values_filter(filter_op, list_as_str) {
    def vs = list_as_str instanceof List ? list_as_str : "${list_as_str}".split(',')
    def list_values = vs
        .collect { v ->
            "\"${v.trim()}\""
        }
        .inject('') {arg, item -> 
            arg ? "${arg},${item}" : "${item}"
        }
    return "\\\$${filter_op}: [${list_values}]"
}
