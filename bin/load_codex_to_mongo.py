''' load_codex_to_mongo.py
    This program update emBody and emDataSet in MongoDB from the classification.csv
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
import boto3
from boto3.dynamodb.conditions import Key
import inquirer
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught, logging-fstring-interpolation
# Database
DB = {}
# UUIDs
KEYS = {}
# Codex IDs and types
CODEX_TYPE = {}
# Actions
ACTION = {}
# Counters
COUNT = {'found': 0, 'hinsertions': 0, 'iinsertions': 0, 'minsertions': 0}

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


def get_table():
    """ Allow usser to select a table
        Keyword arguments:
          None
        Returns:
          None
    """
    DB['dynamo'] = boto3.resource("dynamodb")
    dtables = list(DB['dynamo'].tables.all())
    tname = []
    for tbl in dtables:
        ddbt = DB['dynamo'].Table(tbl.name)
        if ddbt.name.startswith('janelia-neuronbridge-published-v'):
            tname.append(ddbt.name)
    quest = [inquirer.List('table',
             message='Select NeuronBridge version',
             choices=tname)]
    ans = inquirer.prompt(quest)
    if not ans:
        terminate_program()
    ARG.TABLE = ans['table']


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
    for dbn in ("MongoDB", "DynamoDB"):
        ACTION[dbn] = False
    quest = [inquirer.Checkbox("actions",
                               message="Which databases should be updated?",
                               choices=["MongoDB", "DynamoDB"],
                               default=["MongoDB", "DynamoDB"],)]
    ans = inquirer.prompt(quest)
    for dbn in ("MongoDB", "DynamoDB"):
        if dbn in ans['actions']:
            ACTION[dbn] = True
    if ACTION['DynamoDB'] and not ARG.TABLE:
        get_table()

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
    if not ACTION['MongoDB']:
        return None
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
    if ACTION['MongoDB'] and ARG.WRITE:
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



def read_neuron_type(ntype, tbl):
    """ Retrieve a neuron record from DynamoDB table
        Keyword arguments:
          ntype: neuron type
          tbl: DynamoDB table
        Returns:
          DynamoDB record
    """
    response = tbl.query(
        KeyConditionExpression=Key('itemType').eq('searchString') \
                                   & Key('searchKey').eq(ntype.lower())
    )
    return response


def add_body_ids(htype, payload):
    """ Retrieve a neuron record from DynamoDB table
        Keyword arguments:
          htype: hemibrain (neuron) type
          payload: current payload for neuron type
        Returns:
          None
    """
    tbl = DB['dynamo'].Table(ARG.TABLE)
    rec = read_neuron_type(htype, tbl)
    if rec and rec['Items']:
        for itm in rec['Items']:
            if 'bodyIDs' in itm:
                COUNT['found'] += 1
                payload['bodyIDs'] = itm['bodyIDs']


def write_dynamodb(codex_id):
    """ Write Codex hemibrain types and IDs to DynamoDB
        Keyword arguments:
          codex_id: Codex ID
        Returns:
          None
    """
    LOGGER.debug(codex_id)
    hbatch = []
    for htype in tqdm(CODEX_TYPE, desc='Processing Codex types'):
        payload = {'itemType': 'searchString',
                   'searchKey': htype.lower(),
                   'filterKey': htype.lower(),
                   'keyType': 'neuronType',
                   'name': htype}
        payload['codexIDs'] = []
        for cid in CODEX_TYPE[htype]:
            payload['codexIDs'].append({cid: True})
        add_body_ids(htype, payload)
        hbatch.append(payload)
    if not ARG.WRITE:
        return
    tbl = DB['dynamo'].Table(ARG.TABLE)
    # Hemibrain types
    LOGGER.info(f"Batch writing {len(hbatch):,} hemibrain types to {ARG.TABLE}")
    with tbl.batch_writer() as writer:
        for item in tqdm(hbatch, desc="Writing Codex types"):
            if ARG.THROTTLE and (not COUNT["hinsertions"] % ARG.THROTTLE):
                time.sleep(2)
            writer.put_item(Item=item)
            COUNT["hinsertions"] += 1
    # Codex IDs
    LOGGER.info(f"Batch writing {len(codex_id):,} Codex IDs to {ARG.TABLE}")
    with tbl.batch_writer() as writer:
        for item in tqdm(codex_id, desc="Writing Codex IDs"):
            if ARG.THROTTLE and (not COUNT["iinsertions"] % ARG.THROTTLE):
                time.sleep(2)
            writer.put_item(Item=item)
            COUNT["iinsertions"] += 1


def create_dynamo_payload(entry):
    """ Create the payload for a Codex ID insertion into DynamoDB
        Keyword arguments:
          entry: contains Codex root ID and neuron type
        Returns:
          payload
    """
    # Codex ID
    cid = f"c{entry[0]}"
    payload = {'itemType': 'searchString',
               'searchKey': cid,
               'filterKey': cid,
               'keyType': 'codexID',
               'name': cid}
    # Hemibrain type
    if entry[1]:
        for htype in entry[1].split(","):
            if htype not in CODEX_TYPE:
                CODEX_TYPE[htype] = {}
            CODEX_TYPE[htype][cid] = True
    # cdm? ppp?
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
    LOGGER.info(f"Found {len(entries):,} entries")
    coll = DB['jacs'].emBody
    docs = []
    codex_id = []
    for entry in tqdm(entries, desc='Building insert list'):
        docs.append(create_body_payload(entry, dtm, dsid))
        codex_id.append(create_dynamo_payload(entry))
    if ACTION['MongoDB'] and ARG.WRITE:
        print(f"Writing {len(docs):,} records to emBody")
        coll.insert_many(docs)
    if ACTION['DynamoDB']:
        print(f"Found {len(codex_id):,} Codex IDs")
        print(f"Found {len(CODEX_TYPE):,} Codex hemibrain types")
        write_dynamodb(codex_id)


def process_codex():
    """ Insert data from Codex file into MongoDB
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['jacs'].emDataSet
    result = coll.count_documents({'name': 'codex', 'version': ARG.VERSION})
    if result and ACTION['MongoDB']:
        LOGGER.warning('Dataset codex:%s already exists', ARG.VERSION)
        terminate_program()
    else:
        dsid = insert_dataset(coll)
    entries = []
    with open(ARG.FILE, 'r', encoding='ascii') as instream:
        for row in csv.reader(instream, quotechar='"', delimiter=','):
            if not row[0] or row[0] == 'root_id':
                continue
            entries.append([row[0], row[6]])
    process_entries(entries, dsid)
    print(f"MongoDB Codex ID updates:    {COUNT['minsertions']}")
    print(f"DynamoDB Codex ID updates:   {COUNT['iinsertions']}")
    print(f"DynamoDB Codex type updates: {COUNT['hinsertions']}")
    print(f"Previously existing types:   {COUNT['found']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload data from Codex")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        required=True, help='Codex file')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        required=True, help='Codex version (snapshot)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--table', dest='TABLE', help='DynamoDB table')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to MongoDB')
    PARSER.add_argument('--throttle', type=int, dest='THROTTLE',
                        default=0, help='DynamoDB batch write throttle (# items)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_codex()
    terminate_program()
