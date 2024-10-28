process UPLOAD {
    container { task.ext.container ?: 'docker.io/amazon/aws-cli' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'
    secret 'AWS_ACCESS_KEY'
    secret 'AWS_SECRET_KEY'

    input:
    tuple val(anatomical_area),
          path(base_data_dir),
          val(data_version)
    val(app_runner)
    each(upload_type)  // EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
    val(s3_bucket)
    val(dry_run)

    output:
    tuple env(full_data_dir), val(s3_uri)

    when:
    task.ext.when == null || task.ext.when

    script:
    def data_location = get_data_dir(upload_type, data_version, anatomical_area)
    def s3_prefix = get_s3_prefix(upload_type, data_version)
    def data_dir = "${base_data_dir}/${data_location}"
    def s3_uri = "s3://${s3_bucket}/${s3_prefix}"
    def upload_cmd = get_upload_cmd(app_runner, data_dir, s3_uri, dry_run)

    """
    echo "\$(date) Run ${anatomical_area} ${upload_type} upload on \$(hostname -s)"
    full_data_dir=\$(readlink -m \${data_dir})
    AWS_ACCESS_KEY_ID=\${AWS_ACCESS_KEY} AWS_SECRET_ACCESS_KEY=\${AWS_SECRET_KEY} ${upload_cmd}
    echo "\$(date) Completed ${anatomical_area} ${upload_type} upload on \$(hostname -s)"
    """
}

def get_data_dir(upload_type, data_version, anatomical_area) {
    switch(value) {
        case 'EM_MIPS' -> "v${data_version}/${anatomical_area}/mips/embodies"
        case 'LM_MIPS' -> "v${data_version}/${anatomical_area}/mips/lmlines"
        case 'EM_CD_MATCHES' -> "v${data_version}/${anatomical_area}/cdmatches/em-vs-lm"
        case 'LM_CD_MATCHES' -> "v${data_version}/${anatomical_area}/cdmatches/lm-vs-em"
        case 'EM_PPP_MATCHES' -> "v${data_version}/${anatomical_area}/pppmatches/em-vs-lm"
    }
    throw new IllegalArgumentException("Invalid upload type: ${upload_type}")
}

def get_s3_prefix(upload_type, data_version) {
    def s3_data_version = data_version.replaceAll('.', '_')
    switch(value) {
        case 'EM_MIPS' -> "${s3_data_version}/metadata/by_line"
        case 'LM_MIPS' -> "${s3_data_version}/metadata/by_body"
        case 'EM_CD_MATCHES' -> "${s3_data_version}/metadata/cdsresults"
        case 'LM_CD_MATCHES' -> "${s3_data_version}/metadata/cdsresults"
        case 'EM_PPP_MATCHES' -> "${s3_data_version}/metadata/pppmresults"
    }
    throw new IllegalArgumentException("Invalid upload type: ${upload_type}")
}

def get_upload_cmd(app_runner, local_data_dir, s3_uri, dry_run) {
    def dry_run_arg = dry_run ? '--dryrun' : ''
    "${app_runner} aws s3 cp $local_data_dir ${s3_uri} ${dry_run_arg} --recursive"
}
