process UPLOAD {
    container { task.ext.container ?: 'docker.io/amazon/aws-cli' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'
    secret 'AWS_ACCESS_KEY'
    secret 'AWS_SECRET_KEY'

    input:
    tuple path(base_data_dir),
          val(local_release_dirname),
          val(data_version)
    val(app_runner)
    val(cpus)
    val(mem_gb)
    val(s3_bucket)
    each(anatomical_area)
    each(upload_type)  // EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
    val(dry_run)

    output:
    tuple env(full_data_dir), val(s3_uri)

    when:
    task.ext.when == null || task.ext.when

    script:
    def upload_type_arg = upload_type
    def (data_location, s3_prefix) = get_data_dirs(upload_type_arg, local_release_dirname, data_version, anatomical_area)

    def data_dir = "${base_data_dir}/${data_location}"
    def s3_uri_arg = "s3://${s3_bucket}/${s3_prefix}"
    def upload_cmd = get_upload_cmd(app_runner, data_dir, s3_uri_arg, dry_run)
    s3_uri = s3_uri_arg

    """
    echo "\$(date) Run ${anatomical_area} ${upload_type_arg} upload on \$(hostname -s)"
    full_data_dir=\$(readlink -m ${data_dir})
    AWS_ACCESS_KEY_ID=\${AWS_ACCESS_KEY} AWS_SECRET_ACCESS_KEY=\${AWS_SECRET_KEY} ${upload_cmd}
    echo "\$(date) Completed ${anatomical_area} ${upload_type_arg} upload on \$(hostname -s)"
    """
}

def get_data_dirs(upload_type, local_data_dirname, data_version, anatomical_area) {
    log.debug "Get data locations for $upload_type, ${local_data_dirname}, ${data_version}"
    def s3_data_version = data_version.replaceAll('\\.', '_')
    switch(upload_type) {
        case 'EM_MIPS' -> [ 
            "${local_data_dirname}/${anatomical_area}/mips/embodies",
            "v${s3_data_version}/metadata/by_body",
        ]
        case 'LM_MIPS' -> [
            "${local_data_dirname}/${anatomical_area}/mips/lmlines",
            "v${s3_data_version}/metadata/by_line",
        ]
        case 'EM_CD_MATCHES' -> [
            "${local_data_dirname}/${anatomical_area}/cdmatches/em-vs-lm",
            "v${s3_data_version}/metadata/cdsresults",
        ]
        case 'LM_CD_MATCHES' -> [
            "${local_data_dirname}/${anatomical_area}/cdmatches/lm-vs-em",
            "v${s3_data_version}/metadata/cdsresults",
        ]
        case 'EM_PPP_MATCHES' -> [
            "${local_data_dirname}/${anatomical_area}/pppmatches/em-vs-lm",
            "v${s3_data_version}/metadata/pppmresults",
        ]
        default -> throw new IllegalArgumentException("Invalid upload type: ${upload_type}")
    }
}

def get_upload_cmd(app_runner, local_data_dir, s3_uri, dry_run) {
    def dry_run_arg = dry_run ? '--dryrun' : ''
    "${app_runner} aws s3 cp $local_data_dir ${s3_uri} ${dry_run_arg} --recursive"
}
