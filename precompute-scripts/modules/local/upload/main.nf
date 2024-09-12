// WIP
process UPLOAD {
    container { task.ext.container ?: 'docker.io/amazon/aws-cli' }
    cpus { cpus }
    memory "${mem_gb} GB"
    label 'neuronbridgeTools'

    input:
    tuple val(job_id),
          val(upload_type),  // EM_MIPS, LM_MIPS, EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES
          val(anatomical_area),
          path(base_export_dir),
          val(relative_output_dir)
    var val(app_runner)
    tuple 
          val(s3_bucket),
          

    output:

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    echo "\$(date) Run ${anatomical_area} ${export_type} export job: ${job_id} on \$(hostname -s)"
    release_export_dir="${base_export_dir}/v${data_version}"
    mkdir -p \${release_export_dir}
    result_export_dir="\${release_export_dir}/${anatomical_area}/${relative_output_dir}"
    full_result_dir=\$(readlink -m \${result_export_dir})

    echo "\$(date) Completed ${anatomical_area} ${export_type} upload job: ${job_id} on \$(hostname -s)"
    """
}


def upload_mips(app_runner, local_data_dir, anatomical_area, mips_type, s3_bucket, s3_data_version) {
    """
    case ${mips_type} in
        lm_mips|LM_MIPS)
            mips_dest=by_line
            ;;
        em_mips|EM_MIPS)
            mips_dest=by_body
            ;;
        *)
            echo "Unsupported mips type: ${mips_type}"
            exit 1
    esac

    # upload
    ${app_runner} aws s3 cp $local_data_dir s3://${s3_bucket}/${s3_data_version}/metadata/\${mips_dest} --recursive
    """
}

def upload_matches(app_runner, local_data_dir, anatomical_area, matches_type, s3_bucket, s3_data_version) {
    """
    case ${mips_type} in
        em_cd_matches|EM_CD_MATCHES)
            src_subdir=
            target_subdir=cdsresults
            ;;
        lm_cd_matches|LM_CD_MATCHES)
            src_subdir=
            target_subdir=cdsresults
            ;;
        *)
            echo "Unsupported matches type: ${matches_type}"
            exit 1
    esac

    # upload

    """
}
