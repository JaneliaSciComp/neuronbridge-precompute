''' This program will check sample IDs from a publishing database against sample IDs
    from the publishedURL table (MongoDB neuronbridge). Any samples in the publishing
    database but not neuronbridge will be reported.
'''
__version__ = '1.0.0'

import argparse
from operator import attrgetter
import sys
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
       }

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


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    # Database
    for source in (ARG.DATABASE, "neuronbridge"):
        manifold = "prod" if source == ARG.DATABASE else ARG.MANIFOLD
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
    if ARG.LIBRARY:
        ARG.LIBRARY = ARG.LIBRARY.split(",")
    else:
        defaults = ["flylight_split_gal4_published"]
        if ARG.DATABASE == "gen1mcfo":
            defaults = ["flylight_annotator_gen1_mcfo_published", "flylight_gen1_mcfo_published"]
        results = DB['neuronbridge'].neuronMetadata.distinct("libraryName")
        libraries = []
        for row in results:
            if "flyem" not in row:
                libraries.append(row)
        libraries.sort()
        quest = [inquirer.Checkbox('checklist',
                 message='Select libraries to process',
                 choices=libraries, default=defaults)]
        ARG.LIBRARY = inquirer.prompt(quest)['checklist']


def missing_from_nb(missing_rel, published, nbd):
    ''' Find samples that are in the publishing database but not publishedURL
        Keyword arguments:
          published: dict of published sample IDs (value=release)
          nbd: dict of samples in NeuronBridge
        Returns:
          dict or releases (value=list of sample IDs)
    '''
    for row in published:
        if "Sample#" + row not in nbd:
            if published[row] not in missing_rel:
                missing_rel[published[row]] = [row]
            else:
                missing_rel[published[row]].append(row)


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
                    outstream.write(f"{row['line']}\t{row['slide_code']}\t"
                                    + f"{row['workstation_sample_id']}\t{row['alps_release']}\n")
            else:
                print(f"{rel} is missing {len(missing_rel[rel])}/{release_size[rel]} samples")
        for rel, smplist in missing_rel.items():
            for smp in smplist:
                try:
                    DB[ARG.DATABASE]['cursor'].execute(READ['SAMPLE'], (smp,))
                    rows = DB[ARG.DATABASE]['cursor'].fetchall()
                except MySQLdb.Error as err:
                    terminate_program(JRC.sql_error(err))
                for row in rows:
                    outstream.write(f"{row['line']}\t{row['slide_code']}\t"
                                    + f"{row['workstation_sample_id']}\t{row['alps_release']}\n")


def perform_checks():
    ''' Prepare comparison dicts and perform checks
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        DB[ARG.DATABASE]['cursor'].execute(READ['ALL_SAMPLES'])
        rows = DB[ARG.DATABASE]['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    LOGGER.info("Found %d sample%s on %s", len(rows), "" if len(rows) == 1 else "s", ARG.DATABASE)
    published = {}
    release_size = {}
    for row in rows:
        published[row['workstation_sample_id']] = row['alps_release']
        if row['alps_release'] not in release_size:
            release_size[row['alps_release']] = 1
        else:
            release_size[row['alps_release']] += 1
    # NeuronBridge
    payload = {"libraryName": {"$in": ARG.LIBRARY}}
    rows = COLL['publishedURL'].distinct("sampleRef", payload)
    nbd = {}
    for row in rows:
        nbd[row] = True
    LOGGER.info("Found %d sample%s in NeuronBridge", len(nbd), "" if len(nbd) == 1 else "s")
    # Report
    analyze_results(published, release_size, nbd)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload prechecks")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        default='mbew', choices=['mbew', 'gen1mcfo', 'raw'],
                        help='Publishing database')
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
    perform_checks()
    terminate_program()
