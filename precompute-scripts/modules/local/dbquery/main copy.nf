process DBQUERY {
    container { task.ext.container ?: 'mongo:7.0.6' }

    input:
    tuple path(db_config_file),
          val(alignment_space),
          val(library_name),
          val(mip_tag)
    

    script:
    def library_filter = "libraryName: \"${libraryName}\","
    def as_filter = "alignmentSpace: \"${alignmentSpace}\","
    def tag_filter = mip_tag
        ? "tags: \"${mip_tag}\","
        : ''
    def query = "db.neuronMetadata.aggregate([ \
        { \
            \$match: { \
                ${as_filter} \
                ${library_filter} \
                ${tag_filter} \
            } \
        }, \
        { \
            \$count: \"mips_count\" \
        }, \
    ])"
    """
    mongodb_server=\$(grep -e "MongoDB.Server=" ${db_config_file} | sed s/MongoDB.Server=//)
    mongodb_database=\$(grep -e "MongoDB.Database=" ${db_config_file} | sed s/MongoDB.Database=//)
    mongodb_authdatabase=\$(grep -e "MongoDB.AuthDatabase=" ${db_config_file} | sed s/MongoDB.AuthDatabase=//)
    mongodb_username=\$(grep -e "MongoDB.Username=" ${db_config_file} | sed s/MongoDB.Username=//)
    mongodb_password=\$(grep -e "MongoDB.Password=" ${db_config_file} | sed s/MongoDB.Password=//)
    mongodb_replicaset=\$(grep -e "MongoDB.ReplicaSet=" ${db_config_file} | sed s/MongoDB.ReplicaSet=//)

    cat > qscript.js <<EOF
    ${query}
    EOF

    mongosh "mongodb://\${mongodb_username}:\${mongodb_password}@\${mongodb_server}/\${mongodb_database}?authSource=\${mongodb_authdatabase}&replicaSet=\${mongodb_replicaset}" < \
        qscript.js
    """

}