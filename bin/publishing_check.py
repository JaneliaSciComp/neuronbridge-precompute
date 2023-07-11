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
READ = {"RELEASE": "SELECT alps_release,MAX(workstation_sample_id) AS smp FROM image_data_mv GROUP BY 1",
        "SAMPLES": "SELECT DISTINCT workstation_sample_id,alps_release FROM image_data_mv",
       }
# Counters
COUNT = {"releases": 0, "mismatch": 0}

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


def perform_checks2():
    try:
        DB[ARG.DATABASE]['cursor'].execute(READ['SAMPLES'])
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
    payload = {"libraryName": ARG.LIBRARY}
    count = COLL['publishedURL'].count_documents(payload)
    LOGGER.info("Found %d sample%s in NeuronBridge", count, "" if count == 1 else "s")
    rows = COLL['publishedURL'].find(payload, {"sampleRef": 1})
    nb = {}
    for row in rows:
        nb[row['sampleRef']] = True
    missing = {}
    for row in tqdm(published):
        if "Sample#" + row not in nb:
            if published[row] not in missing:
                missing[published[row]] = 1
            else:
                missing[published[row]] += 1
    for key in sorted(missing):
        if missing[key] == release_size[key]:
            print(f"{key} is not in NeuronBridge")
        else:
            print(f"{key} is missing {missing[key]}/{release_size[key]} samples")


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
    perform_checks2()
    terminate_program()
