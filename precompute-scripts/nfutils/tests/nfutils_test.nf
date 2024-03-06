include {
    partition_work;
    get_lib_arg;
} from '../utils'

workflow total_jobs_not_exact_multiple {
    Channel.of('work')
    | combine(partition_work(101, 10))
    | map { name,offset,length ->
        get_lib_arg(name, offset, length)
    }
    | view
}

workflow total_jobs_exact_multiple {
    Channel.of('work')
    | combine(partition_work(20, 5))
    | map { name,offset,length ->
        get_lib_arg(name, offset, length)
    }
    | view
}

workflow no_partitioning_requested {
    def jobs = partition_work(23, 0)
    log.info "Expected: $jobs"
}

workflow no_partitioning_needed {
    def jobs = partition_work(21, 24)
    log.info "Expected: $jobs"
}


workflow combine_work_for_multiple_datasets {
    Channel.of(['ds1', 'ds2'])
    | combine([partition_work(10, 4), partition_work(20, 5)].combinations())
    | map { name1, name2, ds1_limits, ds2_limits ->
        def (offset1, length1) = ds1_limits
        def (offset2, length2) = ds2_limits
        [
            get_lib_arg(name1, offset1, length1),
            get_lib_arg(name2, offset2, length2),
        ]
    }
    | view
}
