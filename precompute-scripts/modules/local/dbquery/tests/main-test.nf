include { DBQUERY } from '../main.nf'

workflow {
    DBQUERY(Channel.of([
        file(params.db_config),
        params.alignment_space,
        params.library,
        params.tag
    ])) 
    | map {
        print "!!!$it"
    }
}
