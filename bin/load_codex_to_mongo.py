''' load_codex_to_mongo.py
    This program will update emBody and emDataSet in MongoDB as well as the versioned published
    table in DynamoDB from Codex data
    Codex files used:
      classification.csv (FAFB only)
      neurons.csv
    (https://codex.flywire.ai/api/download?dataset=banc)
    (https://codex.flywire.ai/api/download?dataset=fafb)
'''
__version__ = '2.0.0'

import argparse
import collections
import csv
from datetime import datetime
import json
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
# Globals
ARG = LOGGER = None
# Database
DB = {}
# UUIDs
KEYS = {}
# Codex IDs and labels
CODEX_LABEL = {}
EXISTING_LABELS = {}
NEW_LABELS = {}
# Actions
ACTION = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def get_table():
    """ Allow usser to select a table
        Keyword arguments:
          None
        Returns:
          None
    """
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
    try:
        rows = DB['jacs'].emBody.find({'dataSetIdentifier': {"$regex": "^flywire_"}})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        for term in row['terms']:
            EXISTING_LABELS[term] = True
    LOGGER.info(f"Found {len(EXISTING_LABELS):,} existing labels")
    quest = [inquirer.Checkbox("actions",
                               message="Which databases should be updated?",
                               choices=["MongoDB", "DynamoDB"],
                               default=["MongoDB", "DynamoDB"],)]
    ans = inquirer.prompt(quest)
    for dbn in ("MongoDB", "DynamoDB"):
        if dbn in ans['actions']:
            ACTION[dbn] = True
    DB['dynamo'] = boto3.resource("dynamodb")
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
          Data set identifier
    """
    uuid = f"{ARG.DATASET}:v{ARG.VERSION}"
    LOGGER.warning(f"Dataset {uuid} will be created")
    dtm = datetime.now()
    payload = {'class': 'org.janelia.model.domain.flyem.EMDataSet',
               'ownerKey': 'group:flyem',
               'readers': ['group:flyem'],
               'writers': ['group:flyem'],
               'name': ARG.DATASET,
               'version': ARG.VERSION,
               'uuid': uuid,
               'gender': 'f',
               'anatomicalArea': ARG.AREA,
               'creationDate': dtm,
               'updatedDate': dtm,
               'active': True,
               'published': True
              }
    last_uid = generate_uid()
    payload['_id'] = last_uid
    if ACTION['MongoDB'] and ARG.WRITE:
        result = coll.insert_one(payload)
        return result.inserted_id
    return '1'


def create_body_payload(name, types, dtm, dsid):
    """ Create the payload for an insertion into jacs.emBody
        Keyword arguments:
          name: Codex root ID
          types: list of labels
          dtm: date timestamp
          dsid: dataset ID in jacs.emDataSet
        Returns:
          payload
    """
    payload = {'class': 'org.janelia.model.domain.flyem.EMBody',
               'dataSetIdentifier': f"{ARG.DATASET}:v{ARG.VERSION}",
               'ownerKey': 'group:flyem',
               'readers': ['group:flyem'],
               'writers': ['group:flyem'],
               'dataSetRef': f"EMDataSet#{dsid}",
               'name': name,
               'terms': types,
               'status': None,
               'statusLabel': None,
               'neuronType': None,
               'neuronInstance': None,
               'voxelSize': None,
               'creationDate': dtm,
               'updatedDate': dtm
              }
    last_uid = generate_uid()
    payload['_id'] = last_uid
    COUNT['minsertions'] += 1
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
    # Build a list of associated body IDs. Start with Codex IDs, then add EM bodies.
    body_ids = {}
    for cid in CODEX_LABEL[htype]:
        body_ids[cid] = True
    if rec and rec['Items']:
        for itm in rec['Items']:
            if 'bodyIDs' in itm:
                for bid in itm['bodyIDs']:
                    if isinstance(bid, dict):
                        body_ids[list(bid.keys())[0]] = True
                    else:
                        body_ids[bid] = True
                COUNT['found'] += 1
    payload['bodyIDs'] = []
    for bid in body_ids:
        payload['bodyIDs'].append({bid: True})


def write_dynamodb(codex_id):
    """ Write Codex hemibrain types and IDs to DynamoDB
        Keyword arguments:
          codex_id: Codex ID
        Returns:
          None
    """
    LOGGER.debug(codex_id)
    hbatch = []
    for htype in tqdm(CODEX_LABEL, desc='Processing Codex types'):
        payload = {'itemType': 'searchString',
                   'searchKey': htype.lower(),
                   'filterKey': htype.lower(),
                   'keyType': 'neuronType',
                   'name': htype}
        add_body_ids(htype, payload)
        hbatch.append(payload)
    if not ARG.WRITE:
        return
    tbl = DB['dynamo'].Table(ARG.TABLE)
    # Codex types
    LOGGER.info(f"Batch writing {len(hbatch):,} Codex types to {ARG.TABLE}")
    with tbl.batch_writer() as writer:
        for item in tqdm(hbatch, desc="Writing Codex types"):
            if ARG.THROTTLE and (not COUNT["hinsertions"] % ARG.THROTTLE):
                time.sleep(2)
            try:
                writer.put_item(Item=item)
            except Exception as err:
                print(f"Item {item['name']} contains {len(item['bodyIDs']):,} " \
                      + f"IDs ({sys.getsizeof(item['bodyIDs']):,}B)")
                terminate_program(err)
            COUNT["hinsertions"] += 1
    # Codex IDs
    LOGGER.info(f"Batch writing {len(codex_id):,} Codex IDs to {ARG.TABLE}")
    with tbl.batch_writer() as writer:
        for item in tqdm(codex_id, desc="Writing Codex IDs"):
            if ARG.THROTTLE and (not COUNT["iinsertions"] % ARG.THROTTLE):
                time.sleep(2)
            writer.put_item(Item=item)
            COUNT["iinsertions"] += 1


def create_dynamo_id_payload(name, types):
    """ Create the payload for a Codex ID insertion into DynamoDB
        Keyword arguments:
          name: Codex root ID
          types: list of labels
        Returns:
          payload
    """
    # Codex ID
    payload = {'itemType': 'searchString',
               'searchKey': name,
               'filterKey': name,
               'keyType': 'bodyID',
               'name': name}
    # Label
    if types:
        for lab in types:
            if lab not in CODEX_LABEL:
                CODEX_LABEL[lab] = {}
            CODEX_LABEL[lab][name] = True
    return payload


def process_entries(labels, dlabels, dsid):
    """ Build an array of records to insert into jacs.emBody
        Keyword arguments:
          labels: dict of Codex root IDs and labels for MongoDB
          dlabels: dict of Codex root IDs and labels for DynamoDB
          dsid: dataset ID in jacs.emDataSet
        Returns:
          payload
    """
    dtm = datetime.now()
    coll = DB['jacs'].emBody
    docs = []
    codex_id = []
    for key, val in tqdm(labels.items(), desc='Building MongoDB insert lists'):
        if not val:
            continue
        for term in val:
            NEW_LABELS[term] = True
        if ACTION['MongoDB']:
            docs.append(create_body_payload(key, val, dtm, dsid))
    for key, val in tqdm(dlabels.items(), desc='Building DynamoDB insert lists'):
        if not val:
            continue
        if ACTION['DynamoDB']:
            codex_id.append(create_dynamo_id_payload(key, val))
    if ACTION['MongoDB']:
        if ARG.WRITE:
            print(f"Writing {len(docs):,} records to emBody")
            coll.insert_many(docs)
        with open(f"{ARG.DATASET}_mongodb.json", 'w', encoding='ascii') as outstream:
            json.dump(docs, outstream, indent=4, default=str)
    if ACTION['DynamoDB']:
        print(f"Found {len(codex_id):,} Codex IDs")
        print(f"Found {len(CODEX_LABEL):,} Codex types")
        write_dynamodb(codex_id)
        with open(f"{ARG.DATASET}_dynamodb.json", 'w', encoding='ascii') as outstream:
            json.dump(codex_id, outstream, indent=4, default=str)


def process_banc_files(dsid):
    """ Get labels from FAFB codex files
        Keyword arguments:
          dsid: dataset ID in jacs.emDataSet
        Returns:
          None
    """
    entriesl = []
    labels = {} # body ID: [labels]
    dlabels = {} # body ID: cell type
    file = f"{ARG.DATASET}_neurons_{ARG.VERSION}.csv"
    # Add super_class, class, sub_class, cell_type, and hemibrain_type from neurons
    with open(file, 'r', encoding='ascii') as instream:
        for row in csv.reader(instream, quotechar='"', delimiter=','):
            if not row[0] or row[0] == 'Root ID':
                continue
            COUNT['read'] += 1
            labels[row[0]] = []
            dlabels[row[0]] = []
            entriesl.append([row[0], row[13]])
            # super_class, class, sub_class
            for col in range(10, 13): # 10-12: super_class, class, sub_class
                if row[col]:
                    labels[row[0]].append(row[col])
            # cell_type
            if row[16]:
                labels[row[0]].append(row[16])
                dlabels[row[0]].append(row[16])
            # hemibrain_type
            if row[13]:
                for lab in row[13].split(','):
                    if lab not in labels[row[0]]:
                        labels[row[0]].append(lab)
                        dlabels[row[0]].append(lab)
            if not labels[row[0]]:
                del labels[row[0]]
                del dlabels[row[0]]
        LOGGER.info(f"Found {len(labels):,} entries in neurons")
    process_entries(labels, dlabels, dsid)


def process_fafb_files(dsid):
    """ Get labels from FAFB codex files
        Keyword arguments:
          dsid: dataset ID in jacs.emDataSet
        Returns:
          None
    """
    entriesl = []
    labels = {}
    dlabels = {}
    file = f"classification_{ARG.VERSION}.csv"
    # Add super_class, class, sub_class, cell_type, and hemibrain_type from classification
    with open(file, 'r', encoding='ascii') as instream:
        for row in csv.reader(instream, quotechar='"', delimiter=','):
            if not row[0] or row[0] == 'root_id':
                continue
            COUNT['read'] += 1
            labels[row[0]] = []
            dlabels[row[0]] = []
            entriesl.append([row[0], row[6]])
            # super_class, class, sub_class
            for col in range(2, 5):
                if row[col]:
                    labels[row[0]].append(row[col])
            # cell_type
            if row[5]:
                labels[row[0]].append(row[5])
                dlabels[row[0]].append(row[5])
            # hemibrain_type
            if row[6]:
                for lab in row[6].split(','):
                    if lab not in labels[row[0]]:
                        labels[row[0]].append(lab)
                        dlabels[row[0]].append(lab)
        LOGGER.info(f"Found {len(labels):,} entries in classification")
    # Add group from neurons
    file = f"neurons_{ARG.VERSION}.csv"
    with open(file, 'r', encoding='ascii') as instream:
        for row in csv.reader(instream, quotechar='"', delimiter=','):
            if not row[0] or row[0] == 'root_id' or not row[1]:
                continue
            labels[row[0]].append(row[1])
    process_entries(labels, dlabels, dsid)


def process_codex():
    """ Insert data from Codex file into MongoDB
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['jacs'].emDataSet
    result = coll.find_one({'name': ARG.DATASET, 'version': ARG.VERSION})
    if result and ACTION['MongoDB']:
        LOGGER.warning(f"Dataset {result['uuid']} already exists")
        dsid = result['_id']
    else:
        dsid = insert_dataset(coll)
    if ARG.DATASET == 'flywire_banc':
        process_banc_files(dsid)
    else:
        process_fafb_files(dsid)
    # New terms
    with open(f"{ARG.DATASET}_terms.txt", 'w', encoding='ascii') as outstream:
        for key in sorted(NEW_LABELS):
            outstream.write(f"{key}\n")
    # Duplicate terms
    duplicates = []
    for key in NEW_LABELS:
        if key in EXISTING_LABELS:
            duplicates.append(key)
    if duplicates:
        with open(f"{ARG.DATASET}_duplicates.txt", 'w', encoding='ascii') as outstream:
            for key in sorted(duplicates):
                outstream.write(f"{key}\n")
    print(f"Bodies read:                 {COUNT['read']:,}")
    print(f"Existing labels:             {len(EXISTING_LABELS):,}")
    print(f"New labels:                  {len(NEW_LABELS):,}")
    print(f"Duplicate labels:            {len(duplicates):,}")
    print(f"MongoDB Codex ID updates:    {COUNT['minsertions']:,}")
    print(f"DynamoDB Codex ID updates:   {COUNT['iinsertions']:,}")
    print(f"DynamoDB Codex type updates: {COUNT['hinsertions']:,}")
    print(f"Previously existing types:   {COUNT['found']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload data from Codex")
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        default='flywire_fafb', help='Codex version (snapshot)')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        required=True, help='Codex version (snapshot)')
    PARSER.add_argument('--area', dest='AREA', action='store',
                        default='Brain', choices=['Brain', 'VNC', 'CNS'],
                        help='Anatomical area')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--table', dest='TABLE', action='store', help='DynamoDB table')
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
