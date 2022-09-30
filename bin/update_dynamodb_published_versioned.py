''' update_dynamodb_published_versioned.py
    Update a janelia-neuronbridge-published-* DynamoDB table.
'''

import argparse
import json
import os
import sys
from types import SimpleNamespace
import boto3
import colorlog
from pymongo import MongoClient
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm

# Configuration
CONFIG = {'config': {'url': os.environ.get('CONFIG_SERVER_URL')}}
# Database
DATABASE = {}
ITEMS = []
# Counters
COUNT = {"bodyID": 0, "publishingName": 0,
         "images": 0, "missing": 0, "consensus": 0,
         "insertions": 0}


def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
        Returns:
          JSON response
    '''
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Could not get response from {url}: {req.text}")
    return False


def create_config_object(config):
    """ Convert the JSON received from a configuration to an object
        Keyword arguments:
          config: configuration name
        Returns:
          Configuration object
    """
    data = (call_responder("config", f"config/{config}"))["config"]
    return json.loads(json.dumps(data), object_hook=lambda dat: SimpleNamespace(**dat))


def get_version(coll):
    ''' Allow the user to select a NeuronBridge version
        Keyword arguments:
          coll: MongoDB collection
        Returns:
          None
    '''
    versions = {}
    results = coll.distinct("processedTags")
    for row in results:
        for match in ["ColorDepthSearch", "PPPMatch"]:
            if match in row:
                for ver in row[match]:
                    versions[ver] = True
    versions = list(versions.keys())
    versions.sort()
    print("Select a NeuronBridge data version:")
    terminal_menu = TerminalMenu(versions)
    chosen = terminal_menu.show()
    if chosen is None:
        terminate_program("No NeuronBridge data version selected")
    ARG.VERSION = versions[chosen]


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    dbconfig = create_config_object("db_config")
    # MongoDB
    LOGGER.info("Connecting to Mongo on %s", ARG.MONGO)
    try:
        dbc = getattr(getattr(dbconfig, "neuronbridge-mongo"), ARG.MONGO)
        rset = 'rsProd' if ARG.MONGO == 'prod' else 'rsDev'
        client = MongoClient(dbc.read.host, replicaSet=rset)
        dbm = client.admin
        dbm.authenticate(dbc.read.user, dbc.read.password)
        DATABASE["NB"] = client.neuronbridge
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    # DynamoDB
    if not ARG.VERSION:
        get_version(DATABASE["NB"]["neuronMetadata"])
    table = "janelia-neuronbridge-published-test"
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    try:
        _ = dynamodb_client.describe_table(TableName=table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        LOGGER.error("Table %s doesn't exist", table)
        print("You can create it using " \
              + "neuronbridge-utilities/dynamodb/create_janelia-neuronbridge-published.sh")
        sys.exit(-1)
    DATABASE["DYN"] = dynamodb.Table(table)


def batch_row(row, keytype, matches):
    ''' Create and save a payload for a single row
        Keyword arguments:
          row: row from neuronMetadata
          keytype: key type for DynamoDB
          matches: CDM/PPP match dict
        Returns:
          None
    '''
    name = row["publishedName"]
    payload = {"itemType": "searchString",
               "searchKey": name.lower(),
               "filterKey": name.lower(),
               "name": name,
               "keyType": keytype,
               "cdm": matches["cdm"],
               "ppp": matches["ppp"]}
    ITEMS.append(payload)
    COUNT[keytype] += 1


def primary_update(rlist, matches):
    ''' Run primary update to batch simple items (publishingName and bodyID)
        Keyword arguments:
          rlist: record list
          matches: match dict
        Returns:
          None
    '''
    for row in tqdm(rlist, desc="Primary update"):
        if "publishedName" not in row or not row["publishedName"]:
            continue
        keytype = "publishingName"
        if row["libraryName"].startswith("flyem"):
            keytype = "bodyID"
        batch_row(row, keytype, matches[row["publishedName"]])


def write_dynamodb():
    ''' Write rows from ITEMS to DynamoDB in batch
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Batch writing items to DynamoDB")
    with DATABASE["DYN"].batch_writer() as writer:
        for item in tqdm(ITEMS, desc="DynamoDB"):
            writer.put_item(Item=item)
            COUNT["insertions"] += 1


def update_dynamo():
    ''' Main routine to update DynamoDB from MongoDB neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DATABASE["NB"]["neuronMetadata"]
    payload = {"$or": [{"processedTags.ColorDepthSearch": ARG.VERSION},
                       {"processedTags.PPPMatch": ARG.VERSION}]}
    project = {"libraryName": 1, "publishedName": 1, "processedTags": 1}
    results = coll.find(payload, project)
    count = coll.count_documents(payload)
    matches = {}
    rlist = []
    for row in tqdm(results, desc="publishedName", total=count):
        COUNT["images"] += 1
        if "publishedName" not in row or not row["publishedName"]:
            LOGGER.error("Missing publishedName for %s", row['_id'])
            COUNT["missing"] += 1
            continue
        pname = row["publishedName"]
        if pname.lower() == "no consensus":
            COUNT["consensus"] += 1
            continue
        if pname not in matches:
            matches[pname] = {"cdm": False, "ppp": False}
            rlist.append(row)
        if "ColorDepthSearch" in row["processedTags"] \
           and ARG.VERSION in row["processedTags"]["ColorDepthSearch"]:
            matches[pname]["cdm"] = True
        if "PPPMatch" in row["processedTags"] \
           and ARG.VERSION in row["processedTags"]["PPPMatch"]:
            matches[pname]["ppp"] = True
    primary_update(rlist, matches)
    if ARG.WRITE:
        write_dynamodb()
    else:
        COUNT["insertions"] = len(ITEMS)
    print(f"Images read:               {COUNT['images']}")
    if COUNT['missing']:
        print(f"Missing publishing name:   {COUNT['missing']}")
    if COUNT['consensus']:
        print(f"No consensus:              {COUNT['consensus']}")
    print(f"Items written to DynamoDB: {COUNT['insertions']}")
    print(f"  bodyID:                  {COUNT['bodyID']}")
    print(f"  publishingName:          {COUNT['publishingName']}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update a janelia-neuronbridge-published-* table")
    PARSER.add_argument('--version', dest='VERSION', default='', help='NeuronBridge version')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write to DynamoDB')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    REST = create_config_object("rest_services")
    initialize_program()
    update_dynamo()
    terminate_program()
