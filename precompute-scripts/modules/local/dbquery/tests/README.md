### Run this test using:

nextflow run modules/local/dbquery/tests/dbquery.test.nf -c modules/local/dbquery/tests/nf-test.config -entry all_mips --anatomical_area vnc

nextflow run modules/local/dbquery/tests/dbquery.test.nf -c modules/local/dbquery/tests/nf-test.config -entry unique_mips --published_names MB082C,SS50946,SS50947

nextflow run modules/local/dbquery/tests/dbquery.test.nf -c modules/local/dbquery/tests/nf-test.config -entry all_mips --published_names 1041407681 --library flyem_hemibrain_1_2_1
