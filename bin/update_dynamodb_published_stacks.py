''' This program will update janelia-neuronbridge-published-stacks
'''

import argparse
from copy import deepcopy
import sys
import boto3
from colorama import Fore, Style
import colorlog
import requests
from pymongo import MongoClient
from tqdm import tqdm


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
KEY = "searchString"
INSERTED = {}
SLIDE_CODE = {}
# Database
DBM = ''
TABLE = ''
# General
COUNT = {"error":  0, "write": 0}

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
        msg = 'MySQL error [%d]: %s' % (err.args[0], err.args[1])
    except IndexError:
        msg = 'MySQL error: %s' % (err)
    terminate_program(msg)


def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
          endpoint: endpoint
        Returns:
          JSON
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    if req.status_code == 400:
        try:
            if "error" in req.json():
                LOGGER.error("%s %s", url, req.json()["error"])
        except Exception as err:
            pass
        return False
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize
    """
    global CONFIG, DBM, TABLE  # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    # Connect to Mongo
    LOGGER.info("Connecting to Mongo on %s", ARG.MANIFOLD)
    rwp = 'read'
    try:
        if ARG.MANIFOLD == 'prod':
            client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'],
                                 replicaSet='replWorkstation')
        else:
            client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'])
        DBM = client.jacs
        if ARG.MANIFOLD == 'prod':
            DBM.authenticate(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['user'],
                             data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['password'])
    except Exception as err:
        terminate_program('Could not connect to Mongo: %s' % (err))
    # DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    ddt = "janelia-neuronbridge-published-stacks"
    LOGGER.info("Connecting to %s", ddt)
    TABLE = dynamodb.Table(ddt)


def set_payload(row):
    """ Set a DynamoDB item payload
        Keyword arguments:
          row: row from MongoDB publishedImage collection
        Returns:
          payload
    """
    key = row["slideCode"]
    skey = "-".join([row["objective"], row["alignmentSpace"]])
    ckey = "-".join([key, skey])
    if ckey in INSERTED:
        terminate_program("Key %s is already in table" % (ckey))
    SLIDE_CODE[row["slideCode"]] = True
    INSERTED[ckey] = True
    payload = {"itemType": ckey.lower(),
              }
    for itm in ["name", "area", "tile", "releaseName", "slideCode", "objective", "alignmentSpace"]:
        payload[itm] = row[itm]
    payload["files"] = deepcopy(row["files"])
    return payload


def write_payload(payload):
    """ Write an item payload to DynamoDB
        Keyword arguments:
          payload: payload to write
        Returns:
          None
    """
    if ARG.WRITE:
        response = TABLE.put_item(Item=payload)
        if 'ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] == 200:
            COUNT['write'] += 1
        else:
            COUNT['error'] += 1
    else:
        COUNT['write'] += 1


def process_mongo():
    """ Use a JACS sample result to find the Unisex CDM
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DBM.publishedImage
    rows = coll.find({}).sort("slideCode")
    record_cnt = coll.count_documents({})
    LOGGER.info("Records in Mongo publishedImage: %d", record_cnt)
    for row in tqdm(rows, total=record_cnt):
        payload = set_payload(row)
        write_payload(payload)
    tcolor = Fore.GREEN if record_cnt == COUNT["write"] else Fore.RED
    print("Items read:    %s" % (tcolor + str(record_cnt) + Style.RESET_ALL))
    print("Slide codes:   %d" % (len(SLIDE_CODE)))
    print("Items written: %s" % (tcolor + str(COUNT["write"]) + Style.RESET_ALL))
    print("Write errors:  %d" % (COUNT["error"]))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update janelia-neuronbridge-published-stacks")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', choices=['dev', 'prod'], help='Manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to databases')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    initialize_program()
    process_mongo()
    sys.exit(0)
