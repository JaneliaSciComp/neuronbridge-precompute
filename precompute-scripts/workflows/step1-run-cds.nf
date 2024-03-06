include { CDS } from '../modules/local/cds/main.nf'
include { DBQUERY as COUNT_MASKS } from '../modules/local/dbquery/main.nf'
include { DBQUERY as COUNT_TARGETS } from '../modules/local/dbquery/main.nf'

include { partition_work } from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def masks_count = COUNT_MASKS(
        Channel.of([
            params.anatomical_area,
            params.masks_library,
            '',
            false,
        ]),
        db_config_file,
    )

    def targets_count = COUNT_TARGETS(
        Channel.of([
            params.anatomical_area,
            params.targets_library,
            '',
            false,
        ]),
        db_config_file,
    )

    def cds_inputs = masks_count
    | join(targets_count)
    | flatMap { anatomical_area, masks_library, nmasks, targets_library, ntargets -> 
        def masks_jobs = partition_work(nmasks, params.cds_mask_batch_size)
        def targets_jobs = partition_work(ntargets, params.cds_target_batch_size)
        [masks_jobs, targets_jobs]
            .combinations()
            .collect {
                def (masks_limits, targets_limits) = it
                def (masks_offset, masks_size) = masks_limits
                def (targets_offset, targets_size) = targets_limits
                [
                    anatomical_area,
                    masks_library,
                    masks_offset,
                    masks_size,
                    targets_library,
                    targets_offset,
                    targets_size,
                ]
            }
    }

    cds_inputs.subscribe {
        log.debug "Run cds: $it"
    }
    CDS(cds_inputs,
        [
            file(params.app),
            params.cds_runner
        ],
        db_config_file,
        params.cpus,
        params.mem_gb,
        params.java_opts,
        [
            params.cds_processing_tag,
            params.mirror_flag,
            params.mask_th,
            params.target_th,
            params.pix_color_fluctuation,
            params.xy_shift,
            params.pct_pos_pixels,
            params.cds_processing_size,
        ]
    )

}
