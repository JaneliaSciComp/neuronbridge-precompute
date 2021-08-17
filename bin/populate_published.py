''' This program will
'''

import argparse
from glob import glob
import json
import re
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import colorlog
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm

# pylint: disable=no-member
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
PUBLISH = {'published': 'Y', 'published_externally': '1', 'published_to': None,
           'publishing_name': None, 'to_publish': 'Y'}
TYPE_BODY = dict()
INSTANCE_BODY = dict()
# Database
TABLE = ''
INSERTED = dict()

# General use
#RELEASE_LIBRARY_BASE = "/groups/scicompsoft/informatics/data/release_libraries"
RELEASE_LIBRARY_BASE = "/Users/svirskasr/Documents/workspace/Git/neuronbridge-utilities/bin/release_libraries"
COUNT = {"error": 0, "skipped": 0, "write": 0}
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
# pylint: disable=W0703


def call_responder(server, endpoint):
    """ Call a REST API
        Keyword arguments:
          server: server name
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
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    global CONFIG, TABLE # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/aws')
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    TABLE = dynamodb.Table('janelia-neuronbridge-published')


def get_nb_version():
    """ Prompt the user for a MeuronBridge version from subdirs in the base dir
        Keyword arguments:
          None
        Returns:
          None (sets ARG.NEURONBRIDGE)
    """
    version = [re.sub('.*/', '', path)
               for path in glob(RELEASE_LIBRARY_BASE + '/v[0-9]*')]
    print("Select a NeuronBridge version:")
    terminal_menu = TerminalMenu(version)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No NeuronBridge version selected")
        sys.exit(0)
    ARG.NEURONBRIDGE = version[chosen]


def get_library():
    """ Prompt the user for a library
          None
        Returns:
          None (sets ARG.LIBRARY)
    """
    library = [re.sub('.*/', '', path)
               for path in glob('/'.join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE, '*.json']))]
    print("Select a library file:")
    terminal_menu = TerminalMenu(library)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No library file selected")
        sys.exit(0)
    ARG.LIBRARY = library[chosen]


def perform_body_mapping(data):
    loaded = list()
    try:
        response = TABLE.scan()
        if "Items" in response and response["Items"]:
            loaded = response["Items"]
            while 'LastEvaluatedKey' in response:
                response = TABLE.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                loaded.extend(response['Items'])
    except ClientError as err:
        LOGGER.error(err.response['Error']['Message'])
    except Exception as err:
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)
    LOGGER.info("Items in table: %d", len(loaded))
    if loaded:
        for item in tqdm(loaded, "Assigning neurons"):
            if item["keyType"] == "neuronType":
                TYPE_BODY[item["key"]] = item["bodyIDs"]
            elif item["keyType"] == "neuronInstance":
                INSTANCE_BODY[item["key"]] = item["bodyIDs"]
    LOGGER.info("Neuron types loaded from table: %d", len(TYPE_BODY))
    LOGGER.info("Neuron instances loaded from table: %d", len(INSTANCE_BODY))
    for item in tqdm(data, "Mapping body IDs"):
        if "neuronType" in item:
            if item["neuronType"] in TYPE_BODY:
                key = item["neuronType"]
                if item["publishedName"] not in TYPE_BODY[key]:
                    TYPE_BODY[key].append(item["publishedName"])
            else:
                TYPE_BODY[item["neuronType"]] = list([item["publishedName"]])
        if "neuronInstance" in item:
            key = item["neuronInstance"]
            if key in INSTANCE_BODY:
                if item["publishedName"] not in INSTANCE_BODY[key]:
                    INSTANCE_BODY[key].append(item["publishedName"])
            else:
                #print("Added %s to %s" % (item["publishedName"], key))
                INSTANCE_BODY[key] = list([item["publishedName"]])
    LOGGER.info("Neuron types cached: %d", len(TYPE_BODY))
    LOGGER.info("Neuron instances cached: %d", len(INSTANCE_BODY))


def get_row(key, key_type):
    if key in INSERTED and key_type in INSERTED[key]:
        return INSERTED[key][key_type], True
    try:
        response = TABLE.get_item(Key={"key": key, "keyType": key_type})
    except ClientError as err:
        LOGGER.error(err.response['Error']['Message'])
    except Exception as err:
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)
    if "Item" in response and response["Item"]:
        if key not in INSERTED:
            INSERTED[key] = {key_type: response["Item"]}
        else:
            INSERTED[key][key_type] = response["Item"]
        return response["Item"], False
    return None, False


def insert_row(key, key_type, result_type):
    payload, skip = get_row(key, key_type)
    if skip:
        COUNT['skipped'] += 1
        return
    if key_type in ["bodyID", "publishedName"] and payload and result_type in payload \
                                               and payload[result_type]:
        COUNT['skipped'] += 1
        return
    LOGGER.debug("Insert %s (%s)", key, key_type)
    if not payload:
        payload = {"key": key, "keyType": key_type}
    payload["searchKey"] = key.lower()
    payload[result_type] = True
    if ARG.TYPE == "EM" and ARG.RESULT == "cdm":
        if key_type == "neuronType":
            TYPE_BODY[key].sort()
            payload["bodyIDs"] = TYPE_BODY[key]
        elif key_type == "neuronInstance":
            INSTANCE_BODY[key].sort()
            payload["bodyIDs"] = INSTANCE_BODY[key]
    if ARG.WRITE:
        response = TABLE.put_item(Item=payload)
        if 'ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] == 200:
            COUNT['write'] += 1
        else:
            COUNT['error'] += 1
    else:
        COUNT['write'] += 1



def process_single_item(item):
    if ARG.TYPE == "EM":
        if "publishedName" in item:
            insert_row(item["publishedName"], "bodyID", ARG.RESULT)
        if "neuronType" in item:
            insert_row(item["neuronType"], "neuronType", ARG.RESULT)
        if "neuronInstance" in item:
            insert_row(item["neuronInstance"], "neuronInstance", ARG.RESULT)
    else:
        if "publishedName" in item:
            insert_row(item["publishedName"], "publishingName", ARG.RESULT)


def populate_table():
    if not ARG.RESULT:
        print("Select result type:")
        allowed = ['cdm', 'ppp']
        terminal_menu = TerminalMenu(allowed)
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.error("No result type selected")
            sys.exit(0)
        ARG.RESULT = allowed[chosen]
    if not ARG.TYPE:
        print("Select library type:")
        allowed = ['EM', 'LM']
        terminal_menu = TerminalMenu(allowed)
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.error("No library type selected")
            sys.exit(0)
        ARG.TYPE = allowed[chosen]
    if not ARG.NEURONBRIDGE:
        get_nb_version()
    if not ARG.LIBRARY:
        get_library()
    # Read JSON file into data
    path = '/'.join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE, ARG.LIBRARY])
    try:
        with open(path) as handle:
            data = json.load(handle)
    except Exception as err:
        LOGGER.error("Could not open %s", path)
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)
    LOGGER.info("Loaded %d items from %s", len(data), path)
    if ARG.TYPE == "EM" and ARG.RESULT == "cdm":
        perform_body_mapping(data)
    for item in tqdm(data, "Processing items"):
        process_single_item(item)
    print(COUNT)


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Populate the janelia-neuronbridge-published DynamoDB table')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        help='Library file')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', action='store',
                        help='NeuronBridge data version')
    PARSER.add_argument('--result', dest='RESULT', action='store',
                        choices=['cdm', 'ppp'], help='Result type')
    PARSER.add_argument('--type', dest='TYPE', action='store',
                        choices=['EM', 'LM'], help='Library type')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write changes to database')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
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
    populate_table()
    sys.exit(0)
