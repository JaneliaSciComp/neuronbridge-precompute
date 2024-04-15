/**
 DBQUERY queries the MIPs count from NeuronBridge.neuronMetadata collection.
*/

include { area_to_alignment_space } from '../../../nfutils/utils'

process DBQUERY {
    container { task.ext.container ?: 'mongo:7.0.6' }
    label 'prefer_local'

    input:
    tuple val(anatomical_area),
          val(library_names),
          val(published_names),
          val(mips_tags),
          val(excluded_tags),
          val(unique_mips)

    path(db_config_file)
    
    output:
    tuple val(anatomical_area), val(library_names), env(mips_count_res)

    script:
    def alignment_space = area_to_alignment_space(anatomical_area)
    def library_filter = library_names ? "libraryName: ${get_in_filter(library_names)}," : ''
    def as_filter = "alignmentSpace: \"${alignment_space}\","
    def tag_filter = mips_tags ? "tags: ${get_in_filter(mips_tags)}," : ''
    def excluded_tag_filter = excluded_tags ? "tags: ${get_nin_filter(excluded_tags)}," : ''
    def published_name_filter = published_names ? "publishedName: ${get_in_filter(published_names)}," : ''
    def unique_pipeline = unique_mips 
        ? "{\\\$group: {_id: \"\\\$mipId\"}},"
        : ''
    def match_op = '$match'
    def count_op = '$count'
    """
    mongodb_server=\$(grep -e "MongoDB.Server=" ${db_config_file} | sed s/MongoDB.Server=//)
    mongodb_database=\$(grep -e "MongoDB.Database=" ${db_config_file} | sed s/MongoDB.Database=//)
    mongodb_authdatabase=\$(grep -e "MongoDB.AuthDatabase=" ${db_config_file} | sed s/MongoDB.AuthDatabase=//)
    mongodb_username=\$(grep -e "MongoDB.Username=" ${db_config_file} | sed s/MongoDB.Username=//)
    mongodb_password=\$(grep -e "MongoDB.Password=" ${db_config_file} | sed s/MongoDB.Password=//)
    mongodb_replicaset=\$(grep -e "MongoDB.ReplicaSet=" ${db_config_file} | sed s/MongoDB.ReplicaSet=//)

    mongosh "mongodb://\${mongodb_username}:\${mongodb_password}@\${mongodb_server}/\${mongodb_database}?authSource=\${mongodb_authdatabase}&replicaSet=\${mongodb_replicaset}" <<-EOF > mongo_output
    db.neuronMetadata.aggregate([
        {
            \\\$match: {
                ${as_filter}
                ${library_filter}
                ${tag_filter}
                ${published_name_filter}
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
    def list_values = "${list_as_str}".split(',')
        .collect {
            "\"${it.trim()}\""
        }
        .inject('') {arg, item -> 
            arg ? "${arg},${item}" : "${item}"
        }
    return "{\\\$in: [${list_values}]}"

}