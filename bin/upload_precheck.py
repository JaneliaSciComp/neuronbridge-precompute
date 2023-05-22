''' This program will check neuronMetadata for a NeuronBridge release for
    sample issues, optionally retagging documents.
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
# Counters
COUNT = {"images": 0, "found": 0, "updated": 0}

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
    for source in ("sage", "neuronbridge"):
        manifold = "prod" if source == "sage" else ARG.MANIFOLD
        rwp = "write" if ARG.WRITE else "read"
        dbo = attrgetter(f"{source}.{manifold}.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            if source == "sage":
                DB[source] = {}
            DB[source] = JRC.connect_database(dbo)
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)
    # Parms
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(DB['neuronbridge'].neuronMetadata)
    if not ARG.VERSION:
        ARG.VERSION = NB.get_neuronbridge_version(DB['neuronbridge'].neuronMetadata)


def tag_release(row, release):
    ''' Tag the image with the ALPs release and the word "unstaged"
        Keyword arguments:
          row: neuronMetadata row
          release: ALPS release
        Returns:
          None
    '''
    COUNT['found'] += 1
    listsize = len(row['tags'])
    if release not in row['tags']:
        row['tags'].append(release)
    if "unstaged" not in row['tags']:
        row['tags'].append("unstaged")
    if len(row['tags']) == listsize:
        return
    LOGGER.debug(row)
    if ARG.WRITE:
        payload = { "$set": { 'tags': row['tags']} }
        result = COLL['neuronMetadata'].update_one({"_id": row['_id']}, payload)
        if result.modified_count:
            COUNT["updated"] += 1
        else:
            LOGGER.error("Could not update %s in neuronMetadata", row['_id'])



def check_image(row, non_public):
    ''' Check an image to see if it's ready for uploading
        Keyword arguments:
          row: neuronMetadata row
          non_public: hash of non-public slide codes
        Returns:
          None
    '''
    if row['slideCode'] in non_public:
        release = non_public[row['slideCode']]
        LOGGER.warning("Image %s is in non-public release %s", row['_id'], release)
        tag_release(row, release)
    elif not row['publishedName']:
        LOGGER.error("No publishing name for %s", row['_id'])


def perform_checks():
    ''' Check all images for a given library/version
        Keyword arguments:
          None
        Returns:
          None
    '''
    COLL['lmRelease'] = DB['neuronbridge'].lmRelease
    results = COLL['lmRelease'].find({"public": False})
    non_public = [row['release'] for row in results]
    COLL['neuronMetadata'] = DB['neuronbridge'].neuronMetadata
    payload = {"libraryName": ARG.LIBRARY,
               "$and": [{"tags": ARG.VERSION},
                        {"tags": {"$nin": ["unstaged"]}}]}
    count = COLL['neuronMetadata'].count_documents(payload)
    if not count:
        terminate_program(f"There are no processed tags for version {ARG.VERSION} in {ARG.LIBRARY}")
    print(f"Images in {ARG.LIBRARY} {ARG.VERSION}: {count}")
    sql = "SELECT DISTINCT slide_code,alps_release FROM image_data_mv WHERE alps_release IN (%s)"
    sql = sql % ('"' + '","'.join(non_public) + '"',)
    LOGGER.info("Finding non-public images in SAGE")
    DB['sage']['cursor'].execute(sql)
    rows =  DB['sage']['cursor'].fetchall()
    non_public = {row['slide_code']: row['alps_release']  for row in rows}
    project = {"libraryName": 1, "publishedName": 1, "slideCode": 1,
               "tags": 1, "neuronInstance": 1, "neuronType": 1}
    results = COLL['neuronMetadata'].find(payload, project)
    for row in tqdm(results, desc="publishedName", total=count):
        COUNT['images'] += 1
        check_image(row, non_public)
    print(f"Images found:    {COUNT['images']}")
    print(f"Images to retag: {COUNT['found']}")
    print(f"Images retagged: {COUNT['updated']}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload prechecks")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        default='', help='NeuronBridge data version')
    PARSER.add_argument('--tag', dest='TAG', action='store',
                        default='', help='MongoDB neuronMetadata tag')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='S3 manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually write to JACS (and AWS if flag set)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    perform_checks()
    terminate_program()
