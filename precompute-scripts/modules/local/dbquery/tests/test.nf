include { DBQUERY } from '../main.nf'

workflow {
    DBQUERY(Channel.of([
        file(params.db_config),
        params.alignment_space,
        params.library,
        params.tag
    ])) 
    // | map { content ->
    //     def lines = content.split('\n')

    //     def mips_count_line = lines
    //         .find { it.indexOf('mips_count') >= 0 }
    //     def count_matcher =  mips_count_line =~ /\d+/
    //     print "!!!! $mips_count_line ${count_matcher.size()} ${count_matcher[0]} !!!"
    //     count_matcher[0] as Integer
    // }
    | view
}
