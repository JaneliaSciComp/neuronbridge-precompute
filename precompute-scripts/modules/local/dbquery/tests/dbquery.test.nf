include { DBQUERY } from '../main.nf'

workflow all_mips {

    DBQUERY(
        Channel.of([
            params.anatomical_area,
            params.library,
            params.mips_tag,
            false,
        ]),
        file(params.db_config),
    )
    | view

}

workflow unique_mips {

    DBQUERY(
        Channel.of([
            params.anatomical_area,
            params.library,
            params.mips_tag,
            true,
        ]),
        file(params.db_config),
    )
    | view

}
