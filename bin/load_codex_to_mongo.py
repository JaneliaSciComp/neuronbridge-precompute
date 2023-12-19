''' This program update emBody and emDataSet in MongoDB from the classification.csv
    data from Codex (https://codex.flywire.ai/api/download)
'''
__version__ = '0.0.1'

import argparse
import csv
from datetime import datetime
from operator import attrgetter
import socket
import sys
import time
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught, logging-fstring-interpolation
# Database
DB = {}
# UUIDs
KEYS = {}

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
    # pylint: disable=broad-exception-caught
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    # Database
    for source in ("jacs",):
        rwp = 'write' if ARG.WRITE else 'read'
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)


def generate_uid(deployment_context=2):
    """ Generate a JACS-style UID
        Keyword arguments:
          deployment_context: deployment context [0]
        Returns:
          UID
    """
    current_time_offset = 921700000000
    max_tries = 1023
    current_index = 0
    try:
        hostname = socket.gethostname()
        ipa = socket.gethostbyname(hostname)
    except Exception:
        ipa = socket.gethostbyname('localhost')
    ip_component = int(ipa.split('.')[-1]) & 0xFF
    next_uid = None
    while current_index <= max_tries and not next_uid:
        time_component = int(time.time()*1000) - current_time_offset
        LOGGER.debug(f"time_component: {time_component:b}")
        time_component = time_component << 22
        LOGGER.debug(f"current_index: {current_index:b}")
        LOGGER.debug(f"deployment_context: {deployment_context:b}")
        LOGGER.debug(f"ip_component: {ip_component:b}")
        next_uid = time_component + (current_index << 12) + (deployment_context << 8) + ip_component
        if current_index > max_tries or next_uid in KEYS:
            LOGGER.debug("UUID collision %d (%d)", next_uid, current_index)
            current_index += 1
            next_uid = None
            continue
    if not next_uid:
        terminate_program("Could not generate UID")
    LOGGER.debug("UID: %d", next_uid)
    if next_uid in KEYS:
        terminate_program(f"Duplicate UID {next_uid}")
    else:
        KEYS[next_uid] = 1
    return next_uid


def insert_dataset(coll):
    """ Insert a record for the dataset in jacs.emDataSet
        Keyword arguments:
          coll: MongoDB collection
        Returns:
          UID
    """
    LOGGER.warning("Dataset codex:%s will be created", ARG.VERSION)
    dtm = datetime.now()
    payload = {'class': 'org.janelia.model.domain.flyem.EMDataSet',
               'ownerKey': 'group:flyem',
               'readers': ['group:flyem'],
               'writers': ['group:flyem'],
               'name': 'codex',
               'version': ARG.VERSION,
               'uuid': ARG.VERSION,
               'gender': '',
               'anatomicalArea': 'Brain',
               'creationDate': dtm,
               'updatedDate': dtm,
               'active': True,
               'published': True
              }
    last_uid = generate_uid()
    payload['_id'] = last_uid
    if ARG.WRITE:
        result = coll.insert(payload)
        return result
    return None


def create_body_payload(entry, dtm, dsid):
    """ Create the payload for an insertion into jacs.emBody
        Keyword arguments:
          entry: contains Codex root ID and neuron type
          dtm: date timestamp
          dsid: dataset ID in jacs.emDataSet
        Returns:
          payload
    """
    payload = {'class': 'org.janelia.model.domain.flyem.EMBody',
               'dataSetIdentifier': f"codex:{ARG.VERSION}",
               'ownerKey': 'group:flyem',
               'readers': ['group:flyem'],
               'writers': ['group:flyem'],
               'dataSetRef': f"EMDataSet#{dsid}",
               'status': None,
               'statusLabel': None,
               'neuronInstance': None,
               'voxelSize': None,
               'creationDate': dtm,
               'updatedDate': dtm
              }
    last_uid = generate_uid()
    payload['_id'] = last_uid
    payload['name'] = entry[0]
    payload['neuronType'] = entry[1]
    return payload


def process_entries(entries, dsid):
    """ Build an array of records to insert into jacs.emBody
        Keyword arguments:
          entries: list of Codex root IDs and neuron types
          dsid: dataset ID in jacs.emDataSet
        Returns:
          payload
    """
    dtm = datetime.now()
    LOGGER.info("Found %d entries", len(entries))
    coll = DB['jacs'].emBody
    docs = []
    for entry in tqdm(entries, desc='Building insert array'):
        docs.append(create_body_payload(entry, dtm, dsid))
        time.sleep(.00001)
    LOGGER.info("Writing %d records to emBody", len(docs))
    if ARG.WRITE:
        coll.insert_many(docs)


def process_codex():
    """ Insert data from Codex file into MongoDB
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['jacs'].emDataSet
    result = coll.count_documents({'name': 'codex', 'version': ARG.VERSION})
    if result:
        LOGGER.warning('Dataset codex:%s already exists', ARG.VERSION)
        terminate_program()
        #result = coll.find({'name': 'codex', 'version': ARG.VERSION})
        #dsid = result[0]['_id']
    else:
        dsid = insert_dataset(coll)
    entries = []
    with open(ARG.FILE, 'r', encoding='ascii') as instream:
        for row in csv.reader(instream, quotechar='"', delimiter=','):
            if not row[0] or row[0] == 'root_id':
                continue
            entries.append([row[0], row[6]])
    process_entries(entries, dsid)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload data from Codex")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='classification.csv', help='Codex file')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        required=True, help='Codex version (snapshot)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to MongoDB')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_codex()
    terminate_program()
