''' crosscheck_library.py
    This program will crosscheck samples in neuronMetadata and publishedURL
'''

import argparse
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC
import neuronbridge_lib as NB

#pylint: disable=logging-fstring-interpolation

# Database
DATABASE = {}


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
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"neuronbridge.{ARG.MANIFOLD}.read")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DATABASE['neuronbridge'] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(source='mongo', coll=DATABASE['neuronbridge']['publishedURL'])


def mongo_check():
    ''' Backcheck publishedURL to neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    print(f"Checking {ARG.LIBRARY} version {ARG.VERSION}")
    cross = {"neuronMetadata": {}, "publishedURL": {}}
    for collection in ["neuronMetadata", "publishedURL"]:
        coll = DATABASE['neuronbridge'][collection]
        col = "sourceRefId" if collection == "neuronMetadata" else "sampleRef"
        payload  = {"libraryName": ARG.LIBRARY, "tags": ARG.VERSION}
        rows = coll.find(payload)
        for row in rows:
            cross[collection][row[col]] = True
        LOGGER.info(f"Found {len(cross[collection]):,} sample IDs for {collection}")
    missing = []
    for row in cross["neuronMetadata"]:
        if row not in cross["publishedURL"]:
            missing.append(row)
    print(f"Samples in neuronMetadata not in publishedURL: {len(missing):,}")
    with open("neuronmetadata_not_publishedurl.txt", "w", encoding="ascii") as outstream:
        for row in missing:
            outstream.write(f"{row}\n")
    missing = []
    for row in cross["publishedURL"]:
        if row not in cross["neuronMetadata"]:
            missing.append(row)
    print(f"Samples in publishedURL not in neuronMetadata: {len(missing):,}")
    with open("publishedurl_not_neuronmetadata.txt", "w", encoding="ascii") as outstream:
        for row in missing:
            outstream.write(f"{row}\n")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backcheck publishedURL to neuronMetadata")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        help='NeuronBridge library')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        default="3.1.1", help='NeuronBridge version')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=["dev", "prod"], default="prod", help='MongoDB manifold')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    mongo_check()
    terminate_program()
