''' This program will check sample IDs from a publishing database against sample IDs
    from the publishedURL table (MongoDB neuronbridge). Any samples in the publishing
    database but not neuronbridge will be reported, and files of these samples and
    images requiring segmentations will be produced.
'''
__version__ = '1.1.0'

import argparse
from operator import attrgetter
import re
import sys
import boto3
import inquirer
import MySQLdb
import jrc_common.jrc_common as JRC

# Database
DB = {}
COLL = {}
READ = {"RELEASE": "SELECT DISTINCT line,slide_code,workstation_sample_id,alps_release "
                   + "FROM image_data_mv WHERE alps_release=%s",
        "SAMPLE": "SELECT DISTINCT line,slide_code,workstation_sample_id,alps_release "
                  + "FROM image_data_mv WHERE workstation_sample_id=%s",
        "ALL_SAMPLES": "SELECT DISTINCT workstation_sample_id,alps_release FROM image_data_mv",
        "RSECDATA": "SELECT image_id,product,url,original_line,slide_code,objective,gender,area,"
                    + "alignment_space_unisex,workstation_sample_id FROM secondary_image_vw s "
                    + "JOIN image_data_mv i ON (s.image_id=i.id) WHERE alps_release=%s"
       }
# General
REQUIRED = ["original_line", "slide_code", "objective", "area", "alignment_space_unisex",
            "workstation_sample_id", "url"]

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def get_parms():
    """ Set ARG.LIBRARY and ARG.VERSION parms
        Keyword arguments:
          None
        Returns:
          None
    """
    COLL['neuronMetadata'] = DB['neuronbridge'].neuronMetadata
    if ARG.LIBRARY:
        ARG.LIBRARY = ARG.LIBRARY.split(",")
    else:
        defaults = ["flylight_split_gal4_published"]
        if ARG.DATABASE == "gen1mcfo":
            defaults = ["flylight_annotator_gen1_mcfo_published", "flylight_gen1_mcfo_published"]
        results = COLL['neuronMetadata'].distinct("libraryName")
        libraries = []
        for row in results:
            libraries.append(row)
        libraries.sort()
        quest = [inquirer.Checkbox('checklist',
                 message='Select libraries to process',
                 choices=libraries, default=defaults)]
        ARG.LIBRARY = inquirer.prompt(quest)['checklist']
    if not ARG.VERSION:
        versions = {}
        payload = {"libraryName": {"$in": ARG.LIBRARY}}
        results = COLL['neuronMetadata'].distinct("tags", payload)
        last = ''
        for row in results:
            if re.match(r"^\d+\.", row):
                versions[row] = True
                last = row
        quest = [inquirer.List('version',
                 message='Select NeuronBridge version',
                 choices=versions.keys(), default=last)]
        ARG.VERSION = inquirer.prompt(quest)['version']


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # pylint: disable=broad-exception-caught
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    # Database
    for source in (ARG.DATABASE, "neuronbridge", "jacs"):
        manifold = "staging" if source == ARG.DATABASE else ARG.MANIFOLD
        dbo = attrgetter(f"{source}.{manifold}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            if source == ARG.DATABASE:
                DB[source] = {}
            DB[source] = JRC.connect_database(dbo)
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)
    COLL['publishedURL'] = DB['neuronbridge'].publishedURL
    # Parms
    get_parms()
    # DynamoDB
    try:
        DB['dynamo'] = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)


def missing_from_nb(missing_rel, published, nbd):
    ''' Find samples that are in the publishing database but not publishedURL
        Keyword arguments:
          published: dict of published sample IDs (value=release)
          nbd: dict of samples in NeuronBridge
        Returns:
          dict of releases (value=list of sample IDs)
    '''
    for row in published:
        if "Sample#" + row not in nbd:
            if published[row] not in missing_rel:
                missing_rel[published[row]] = [row]
            else:
                missing_rel[published[row]].append(row)


def check_dynamodb(lines):
    ''' Check DynamoDB for lines
        Keyword arguments:
          lines: dict of lines for missing samples
        Returns:
          None
    '''
    table = "janelia-neuronbridge-published-v" + ARG.VERSION
    LOGGER.info("Checking for lines in DynamoDB %s", table)
    missing_line = False
    for line in lines:
        response = DB['dynamo'].get_item(TableName=table,
                                         Key={'itemType': {'S': 'searchString'},
                                              'searchKey': {'S': line.lower()}})
        if response['ResponseMetadata']['HTTPStatusCode'] == 200 and "Item" in response:
            lines[line] = True
        else:
            missing_line = True
    if missing_line:
        with open("missing_from_dynamodb.txt", "w", encoding="ascii") as outstream:
            for key,value in sorted(lines.items()):
                if not value:
                    outstream.write(f"{key}\n")


def get_short_objective(obj):
    """ Return a short objective name
        Keyword arguments:
          obj: full objectine name
        Returns:
          short objective name
    """
    obj = obj.lower()
    short_obj = 'unknown'
    for tobj in ('20x', '40x', '63x'):
        if tobj in obj:
            short_obj = tobj
    return short_obj


def produce_stack_file(missing_rel):
    ''' Produce file of H5Js to process
        Keyword arguments:
          missing_rel: dict of releases (value=list of sample IDs)
        Returns:
          None
    '''
    LOGGER.info("Producing list of stacks")
    with open("missing_segmentations.txt", "w", encoding="ascii") as outstream:
        for rel, smplist in missing_rel.items():
            try:
                DB[ARG.DATABASE]["cursor"].execute(READ['RSECDATA'], [rel])
                sds = DB[ARG.DATABASE]["cursor"].fetchall()
            except MySQLdb.Error as err:
                terminate_program(JRC.sql_error(err))
            for row in sds:
                if row['workstation_sample_id'] not in smplist:
                    continue
                if row['product'] != 'aligned_jrc2018_unisex_hr_stack':
                    continue
                ignore = False
                for req in REQUIRED:
                    if not row[req]:
                        LOGGER.error("%s is not defined for image ID %s", req, row['image_id'])
                        ignore = True
                        continue
                if ignore:
                    continue
                obj = get_short_objective(row['objective'])
                if not row['gender']:
                    row['gender'] = "x"
                prefix = "-".join([row['original_line'], row['slide_code'], row['gender'], obj,
                                   row['area'], row['alignment_space_unisex'],
                                   row['workstation_sample_id']])
                outstream.write(f"{row['url']}\t{prefix}\n")


def analyze_results(published, release_size, nbd):
    ''' Compare published sample IDs to those in NeuronBridge
        Keyword arguments:
          published: dict of published sample IDs (value=release)
          release_size: dict of published releases (value= #samples)
          nbd: dict of samples in NeuronBridge
        Returns:
          None
    '''
    missing_rel = {}
    missing_from_nb(missing_rel, published, nbd)
    lines = {}
    with open("missing_samples.txt", "w", encoding="ascii") as outstream:
        for rel in sorted(missing_rel):
            if len(missing_rel[rel]) == release_size[rel]:
                del missing_rel[rel]
                if ARG.EXCLUDENEW:
                    continue
                print(f"{rel} is not in NeuronBridge")
                try:
                    DB[ARG.DATABASE]['cursor'].execute(READ['RELEASE'], (rel,))
                    rows = DB[ARG.DATABASE]['cursor'].fetchall()
                except MySQLdb.Error as err:
                    terminate_program(JRC.sql_error(err))
                for row in rows:
                    lines[row['line']] = False
                    outstream.write(f"{row['line']}\t{row['slide_code']}\t"
                                    + f"{row['workstation_sample_id']}\t{row['alps_release']}\n")
            else:
                print(f"{rel} is missing {len(missing_rel[rel])}/{release_size[rel]} samples")
        LOGGER.info("Preparing output file")
        for rel, smplist in missing_rel.items():
            for smp in smplist:
                try:
                    DB[ARG.DATABASE]['cursor'].execute(READ['SAMPLE'], (smp,))
                    rows = DB[ARG.DATABASE]['cursor'].fetchall()
                except MySQLdb.Error as err:
                    terminate_program(JRC.sql_error(err))
                for row in rows:
                    lines[row['line']] = False
                    outstream.write(f"{row['line']}\t{row['slide_code']}\t"
                                    + f"{row['workstation_sample_id']}\t{row['alps_release']}\n")
    produce_stack_file(missing_rel)
    check_dynamodb(lines)


def perform_flylight_checks():
    ''' Prepare comparison dicts and perform checks for FlyLight
        Keyword arguments:
          None
        Returns:
          None
    '''
    libs = []
    for lib in ARG.LIBRARY:
        if "flylight" in lib:
            libs.append(lib)
    if not libs:
        return
    try:
        DB[ARG.DATABASE]['cursor'].execute(READ['ALL_SAMPLES'])
        rows = DB[ARG.DATABASE]['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    LOGGER.info("Found %d sample%s in %s", len(rows), "" if len(rows) == 1 else "s", ARG.DATABASE)
    published = {}
    release_size = {}
    for row in rows:
        published[row['workstation_sample_id']] = row['alps_release']
        if row['alps_release'] not in release_size:
            release_size[row['alps_release']] = 1
        else:
            release_size[row['alps_release']] += 1
    # NeuronBridge
    payload = {"libraryName": {"$in": libs}}
    rows = COLL['publishedURL'].distinct("sampleRef", payload)
    nbd = {}
    for row in rows:
        nbd[row] = True
    LOGGER.info("Found %d sample%s in NeuronBridge", len(nbd), "" if len(nbd) == 1 else "s")
    # Report
    analyze_results(published, release_size, nbd)


def perform_flyem_checks():
    ''' Prepare comparison dicts and perform checks for FlyLight
        Keyword arguments:
          None
        Returns:
          None
    '''
    libs = []
    bodylibs = []
    for lib in ARG.LIBRARY:
        if "flyem" in lib:
            libs.append(lib)
            lib = lib.replace("flyem_", "")
            lib = lib.replace("_", ":v", 1)
            bodylibs.append(lib.replace("_", "."))
    if not libs:
        return
    # JACS
    COLL['emBody'] = DB['jacs'].emBody
    payload = {"dataSetIdentifier": {"$in": bodylibs}, "status": {"$in": ["Traced", "RT Orphan"]}}
    rows = COLL['emBody'].distinct("name", payload)
    jacs = {}
    for row in rows:
        jacs[row] = True
    LOGGER.info("Found %d body ID%s in JACS (%s)", len(jacs), "" if len(jacs) == 1 else "s",
                ", ".join(bodylibs))
    # NeuronBridge
    payload = {"libraryName": {"$in": libs}}
    rows = COLL['publishedURL'].distinct("publishedName", payload)
    nbd = {}
    for row in rows:
        nbd[row.split(":")[-1]] = True
    LOGGER.info("Found %d body ID%s in NeuronBridge (%s)", len(nbd), "" if len(nbd) == 1 else "s",
                ", ".join(libs))
    # Compare
    bad = good = 0
    with open("missing_bodyids.txt", "w", encoding="ascii") as outstream:
        for row in sorted(jacs):
            if row not in nbd:
                bad += 1
                outstream.write(f"{row}\n")
            else:
                good += 1
    print(f"{good}/{len(jacs)} ({good/len(jacs)*100:.2f}%) are in NeuronBridge")
    print(f"{bad}/{len(jacs)} ({bad/len(jacs)*100:.2f}%) are not in NeuronBridge")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload prechecks")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        default='mbew', choices=['mbew', 'gen1mcfo', 'raw'],
                        help='Publishing database')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        help='NeuronBridge version')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--excludenew', dest='EXCLUDENEW', action='store_true',
                        default=False, help='Exclude newly published releases')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    perform_flyem_checks()
    perform_flylight_checks()
    terminate_program()
