include { GA } from '../modules/local/ga/main.nf'
include { DBQUERY as COUNT_MASKS } from '../modules/local/dbquery/main.nf'

include { partition_work } from '../nfutils/utils'

workflow {

    def db_config_file = file(params.db_config)

    def unique_masks_count = COUNT_MASKS(
        Channel.of([
            params.anatomical_area,
            params.masks_library,
            '',
            true,
        ]),
        db_config_file,
    )

    unique_masks_count | view


}
