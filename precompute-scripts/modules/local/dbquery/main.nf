process DBQUERY {
    container { task.ext.container ?: 'mongo:7.0.6' }

    input:
    tuple path(db_config_file),
          val(alignment_space),
          val(library_name),
          val(mip_tag)
    
    output:
    stdout

    script:
    def library_filter = "libraryName: \"${library_name}\","
    def as_filter = "alignmentSpace: \"${alignment_space}\","
    def tag_filter = mip_tag ? "tags: \"${mip_tag}\"," : ''
    def match_op = '$match'
    def count_op = '$count'
    """
    mongodb_server=\$(grep -e "MongoDB.Server=" ${db_config_file} | sed s/MongoDB.Server=//)
    mongodb_database=\$(grep -e "MongoDB.Database=" ${db_config_file} | sed s/MongoDB.Database=//)
    mongodb_authdatabase=\$(grep -e "MongoDB.AuthDatabase=" ${db_config_file} | sed s/MongoDB.AuthDatabase=//)
    mongodb_username=\$(grep -e "MongoDB.Username=" ${db_config_file} | sed s/MongoDB.Username=//)
    mongodb_password=\$(grep -e "MongoDB.Password=" ${db_config_file} | sed s/MongoDB.Password=//)
    mongodb_replicaset=\$(grep -e "MongoDB.ReplicaSet=" ${db_config_file} | sed s/MongoDB.ReplicaSet=//)

    mongosh "mongodb://\${mongodb_username}:\${mongodb_password}@\${mongodb_server}/\${mongodb_database}?authSource=\${mongodb_authdatabase}&replicaSet=\${mongodb_replicaset}" <<-EOF
    db.neuronMetadata.aggregate([
        {
            \\\$match: {
                ${as_filter}
                ${library_filter}
                ${tag_filter}
            }
        },
        {
            \\\$count: "mips_count"
        },
    ])    
    EOF
    """

}