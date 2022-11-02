''' update_dynamodb_published_skeletons.py
    Update a janelia-neuronbridge-skeletons DynamoDB table.
'''

import argparse
import json
import os
import sys
from types import SimpleNamespace
import boto3
import colorlog
import inquirer
from pymongo import MongoClient
import requests
from tqdm import tqdm

# pylint: disable=W0703, E1101
# Database
DATABASE = {}
ITEMS = []
# Counters
COUNT = {"bodyids": 0, "insertions": 0}
KEYS = {}


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
        req = requests.get(url, timeout=10)
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


def create_dynamodb_table(dynamodb, table):
    """ Create a DynamoDB table
        Keyword arguments:
          dynamodb: DynamoDB resource
          table: table name
        Returns:
          None
    """
    payload = {"TableName": table,
               "KeySchema": [{"AttributeName": "publishedName", "KeyType": "HASH"}
                            ],
               "AttributeDefinitions": [{'AttributeName': 'publishedName', 'AttributeType': 'S'}
                                       ],
               "BillingMode": "PAY_PER_REQUEST",
               "Tags": [{"Key": "PROJECT", "Value": "NeuronBridge"},
                        {"Key": "DEVELOPER", "Value": "svirskasr"},
                        {"Key": "STAGE", "Value": ARG.MANIFOLD}]
              }
    if ARG.WRITE:
        print(f"Creating DynamoDB table {table}")
        table = dynamodb.create_table(**payload)
        table.wait_until_exists()


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    dbconfig = create_config_object("db_config")
    # MongoDB
    LOGGER.info("Connecting to neuronbridge MongoDB on %s", ARG.MONGO)
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
    table = "janelia-neuronbridge-published-skeletons"
    if ARG.MANIFOLD != "prod":
        table += f"-{ARG.MANIFOLD}"
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    try:
        _ = dynamodb_client.describe_table(TableName=table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        LOGGER.warning("Table %s doesn't exist", table)
        create_dynamodb_table(dynamodb, table)
    LOGGER.info("Will write results to DynamoDB table %s", table)
    DATABASE["DYN"] = dynamodb.Table(table)


def batch_row(row):
    ''' Create and save a payload for a single row
        Keyword arguments:
          row: row from publishedURL
        Returns:
          None
    '''
    if row["publishedName"] in KEYS:
        return
    payload = {"publishedName": row["publishedName"],
               "alignmentSpace": row["alignmentSpace"],
               "libraryName": row["libraryName"]
              }
    for skel in ["skeletonobj", "skeletonswc"]:
        if skel in row["uploaded"]:
            payload[skel] = row["uploaded"][skel]
            COUNT[row["libraryName"]][skel] += 1
    ITEMS.append(payload)
    KEYS[row["publishedName"]] = True
    COUNT["bodyids"] += 1


def write_dynamodb():
    ''' Write rows from ITEMS to DynamoDB in batch
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Batch writing %s items to DynamoDB", len(ITEMS))
    with DATABASE["DYN"].batch_writer() as writer:
        for item in tqdm(ITEMS, desc="DynamoDB"):
            try:
                writer.put_item(Item=item)
            except Exception as err:
                print(item)
                terminate_program(err)
            COUNT["insertions"] += 1


def update_dynamo():
    ''' Main routine to update DynamoDB from MongoDB neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Finding skeletons")
    coll = DATABASE["NB"]["publishedURL"]
    libs = coll.distinct("libraryName", {"libraryName": {"$regex": "^flyem_"}})
    if ARG.LIBRARY:
        if ARG.LIBRARY not in libs:
            terminate_program(f"{ARG.LIBRARY} is not a valid library")
        will_load = [ARG.LIBRARY]
    else:
        quest = [inquirer.Checkbox('checklist',
                                   message='Select libraries to upload',
                                   choices=libs, default=libs)]
        will_load = inquirer.prompt(quest)['checklist']
    for lib in will_load:
        COUNT[lib] = {"skeletonobj": 0, "skeletonswc": 0}
    payload = {"libraryName": {"$in": will_load},
               "$or": [{"uploaded.skeletonswc": {"$exists" : True}},
                       {"uploaded.skeletonobj": {"$exists" : True}}]}
    rows = coll.find(payload)
    count = coll.count_documents(payload)
    for row in tqdm(rows, total=count):
        batch_row(row)
    if ARG.WRITE:
        write_dynamodb()
    print(f"Body IDs found:     {count}")
    print(f"Body IDs processed: {COUNT['bodyids']}")
    print(f"Body IDs written:   {COUNT['insertions']}")
    print("Skeleton counts:")
    for key, cnt in COUNT.items():
        if key in ["bodyids", "insertions"]:
            continue
        print(f"  {key}")
        for skel in cnt.keys():
            print(f"    {skel}: {cnt.get(skel)}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update a janelia-neuronbridge-skeletons table")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='Library')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod', 'devpre', 'prodpre'],
                        help='DynamoDB manifold')
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
