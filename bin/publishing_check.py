''' This program will...
'''
__version__ = '0.0.1'

import argparse
from operator import attrgetter
import sys
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_lib as NB

# Database
DB = {}
COLL = {}
READ = {"RELEASE": "SELECT DISTINCT slide_code,workstation_sample_id FROM image_data_mv "
                   + "WHERE alps_release=%s",
        "SAMPLE": "SELECT DISTINCT slide_code,workstation_sample_id FROM image_data_mv "
                  + "WHERE workstation_sample_id=%s",
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
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(DB['neuronbridge'].neuronMetadata, "flyem")


def analyze_results(published, release_size, nb):
    ''' Compare published sample IDs to those in NeuronBridge
        Keyword arguments:
          published: dict of published sample IDs (value=release)
          release_size: dict of published releases (value= #samples)
          nb: dict of samples in NeuronBridge
        Returns:
          None
    '''
    missing_rel = {} # release: [samples]
    for row in published:
        if "Sample#" + row not in nb:
            if published[row] not in missing_rel:
                missing_rel[published[row]] = [row]
            else:
                missing_rel[published[row]].append(row)
    with open("missing_samples.txt", "w", encoding="ascii") as outstream:
        for rel in sorted(missing_rel):
            if len(missing_rel[rel]) == release_size[rel]:
                print(f"{rel} is not in NeuronBridge")
                try:
                    del missing_rel[rel]
                    DB[ARG.DATABASE]['cursor'].execute(READ['RELEASE'], (rel,))
                    rows = DB[ARG.DATABASE]['cursor'].fetchall()
                except MySQLdb.Error as err:
                    terminate_program(JRC.sql_error(err))
                for row in rows:
                    outstream.write(f"{row['slide_code']}\t{row['workstation_sample_id']}\n")
            else:
                print(f"{rel} is missing {len(missing_rel[rel])}/{release_size[rel]} samples")
        for rel in missing_rel:
            for smp in missing_rel[rel]:
                try:
                    DB[ARG.DATABASE]['cursor'].execute(READ['SAMPLE'], (smp,))
                    rows = DB[ARG.DATABASE]['cursor'].fetchall()
                except MySQLdb.Error as err:
                    terminate_program(JRC.sql_error(err))
                for row in rows:
                    outstream.write(f"{row['slide_code']}\t{row['workstation_sample_id']}\n")


def perform_checks():
    # Publishing
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
    payload = {"libraryName": ARG.LIBRARY}
    rows = COLL['publishedURL'].distinct("sampleRef", payload)
    nb = {}
    for row in rows:
        nb[row] = True
    LOGGER.info("Found %d sample%s in NeuronBridge", len(nb), "" if len(nb) == 1 else "s")
    analyze_results(published, release_size, nb)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload prechecks")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        default='mbew', choices=['mbew', 'gen1mcfo', 'raw'], help='Publishing database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    perform_checks()
    terminate_program()
