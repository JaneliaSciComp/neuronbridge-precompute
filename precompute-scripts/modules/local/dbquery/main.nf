/**
 DBQUERY queries the MIPs count from NeuronBridge.neuronMetadata collection.
*/

include { area_to_alignment_space } from '../../../nfutils/utils'

process DBQUERY {
    container { task.ext.container ?: 'mongo:7.0.6' }
    label 'always_use_local'

    input:
    tuple val(anatomical_area),
          val(library_name),
          val(published_names),
          val(mips_tag),
          val(unique_mips)

    path(db_config_file)
    
    output:
    tuple val(anatomical_area), val(library_name), env(mips_count_res)

    script:
    def alignment_space = area_to_alignment_space(anatomical_area)
    def library_filter = "libraryName: \"${library_name}\","
    def as_filter = "alignmentSpace: \"${alignment_space}\","
    def tag_filter = mips_tag ? "tags: \"${mips_tag}\"," : ''
    def published_name_filter = published_names ? "publishedName: ${get_published_name_filter(published_names)}," : ''
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
def get_published_name_filter(published_names_str) {
    def published_names = "${published_names_str}".split(',')
        .collect {
            "\"${it.trim()}\""
        }
        .inject('') {arg, item -> 
            arg ? "${arg},${item}" : "${item}"
        }
    return "{\\\$in: [${published_names}]}"

}