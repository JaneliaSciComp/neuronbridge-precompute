def area_to_alignment_space(area) {
    log.info "!!!!! AREA $area"
    switch (area) {
        case ~/^(?i)brain\+vnc$/:
            return ''
        case ~/^(?i)vnc\+brain$/:
            return ''
        case ~/^(?i)vnc$/:
            return 'JRC2018_VNC_Unisex_40x_DS'
        case ~/^(?i)brain$/ : 
            return 'JRC2018_Unisex_20x_HR'
        default : 
            throw new IllegalArgumentException("Invalid area: ${area}")
    }
}

def partition_work(total_work_size_param, job_size_param) {
    def total_work_size = total_work_size_param as int
    def job_size = job_size_param as int
    def njobs = job_size > 0 
        ? ((total_work_size % job_size) > 0 
            ? (total_work_size / job_size + 1)
            : (total_work_size / job_size)) as int
        : 1
    
    njobs == 1
        ? [[0, total_work_size]]
        : (0..njobs-1).collect {
            def current_job_size = (it + 1) * job_size > total_work_size
                ? (total_work_size - it * job_size)
                : job_size
            [it * job_size, current_job_size]
        }
}

def get_lib_arg(lib, offset, length) {
    def offset_arg = offset > 0 ? "${offset}" : "0"
    def length_arg = length > 0 ? "${length}" : ""
    "${lib}:${offset_arg}:${length_arg}"
}

def get_list_arg(values) {
    if (values) {
        def vs = values.tokenize(',')
        return vs.join(' ')
    } else {
        return ''
    }
}
