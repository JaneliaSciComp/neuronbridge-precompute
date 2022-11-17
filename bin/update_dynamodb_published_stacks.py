''' This program will update janelia-neuronbridge-published-stacks
'''

import argparse
from copy import deepcopy
import json
import os
import sys
from types import SimpleNamespace
import boto3
from colorama import Fore, Style
import colorlog
import requests
from pymongo import MongoClient
from tqdm import tqdm


# Configuration
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
KEY = "searchString"
INSERTED = {}
SLIDE_CODE = {}
# Database
MONGODB = 'neuronbridge-mongo'
DBASE = {}
ITEMS = []
# General
COUNT = {"write": 0}

# pylint: disable=W0703,E1101

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


def sql_error(err):
    """ Log a critical SQL error and exit
        Keyword arguments:
          err: error object
        Returns:
          None
    """
    try:
        msg = f"MySQL error [{err.args[0]}]: {err.args[1]}"
    except IndexError:
        msg = f"MySQL error: {err}"
    terminate_program(msg)


def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
          endpoint: endpoint
        Returns:
          JSON
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    if req.status_code == 400:
        try:
            if "error" in req.json():
                LOGGER.error("%s %s", url, req.json()["error"])
        except Exception:
            pass
        return False
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def create_config_object(config):
    """ Convert the JSON received from a configuration to an object
        Keyword arguments:
          config: configuration name
        Returns:
          Configuration object
    """
    data = (call_responder("config", f"config/{config}"))["config"]
    return json.loads(json.dumps(data), object_hook=lambda dat: SimpleNamespace(**dat))


def initialize_program():
    """ Initialize
    """
    dbconfig = create_config_object("db_config")
    # MongoDB
    LOGGER.info("Connecting to Mongo on %s", ARG.MANIFOLD)
    rwp = 'write' if ARG.WRITE else 'read'
    dbc = getattr(getattr(getattr(dbconfig, MONGODB), ARG.MANIFOLD), rwp)
    try:
        client = MongoClient(dbc.host, replicaSet=dbc.replicaset, username=dbc.user,
                             password=dbc.password)
        DBASE["mongo"] = client.neuronbridge
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))
    # DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    ddt = "janelia-neuronbridge-published-stacks"
    LOGGER.info("Connecting to %s", ddt)
    DBASE["ddb"] = dynamodb.Table(ddt)


def set_payload(row):
    """ Set a DynamoDB item payload
        Keyword arguments:
          row: row from MongoDB publishedLMImage collection
        Returns:
          payload
    """
    key = row["slideCode"]
    skey = "-".join([row["objective"], row["alignmentSpace"]])
    ckey = "-".join([key, skey])
    if ckey in INSERTED:
        terminate_program(f"Key {ckey} is already in table")
    SLIDE_CODE[row["slideCode"]] = True
    INSERTED[ckey] = True
    payload = {"itemType": ckey.lower(),
              }
    for itm in ["name", "area", "tile", "releaseName", "slideCode", "objective", "alignmentSpace"]:
        payload[itm] = row[itm]
    payload["files"] = deepcopy(row["files"])
    return payload


def write_dynamodb():
    ''' Write rows from ITEMS to DynamoDB in batch
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Batch writing %s items to DynamoDB", len(ITEMS))
    with DBASE["ddb"].batch_writer() as writer:
        for item in tqdm(ITEMS, desc="DynamoDB"):
            writer.put_item(Item=item)
            COUNT["write"] += 1


def process_mongo():
    """ Use a JACS sample result to find the Unisex CDM
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        coll = DBASE["mongo"].publishedLMImage
        rows = coll.find()
        count = coll.count_documents({})
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))
    LOGGER.info("Records in Mongo publishedLMImage: %d", count)
    for row in tqdm(rows, total=count):
        payload = set_payload(row)
        ITEMS.append(payload)
    if ARG.WRITE:
        write_dynamodb()
    else:
        COUNT["write"] = count
    tcolor = Fore.GREEN if count == COUNT["write"] else Fore.RED
    print(f"Items read:    {tcolor + str(count) + Style.RESET_ALL}")
    print(f"Slide codes:   {len(SLIDE_CODE)}")
    print(f"Items written: {tcolor + str(COUNT['write']) + Style.RESET_ALL}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update janelia-neuronbridge-published-stacks")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='Manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to databases')
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
    process_mongo()
    sys.exit(0)
