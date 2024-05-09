include { DBQUERY } from '../main.nf'

workflow all_mips {

    DBQUERY(
        Channel.of([
            params.anatomical_area,
            params.library,
            params.published_names,
            params.mip_tags,
            params.excluded_mip_tags,
            params.mip_terms,
            params.excluded_mip_terms,
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
            params.published_names,
            params.mip_tags,
            params.excluded_mip_tags,
            params.mip_terms,
            params.excluded_mip_terms,
            true,
        ]),
        file(params.db_config),
    )
    | view

}
