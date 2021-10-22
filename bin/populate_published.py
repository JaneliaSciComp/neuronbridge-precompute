''' This program will update the janelia-neuronbridge-published table in DynamoDB
    To properly popualte this table, take the following steps:
    1) Start with a blank table
    2) Run the program with --result ppp --action index
    3) Run the program once for every EM/LM release to process
    4) Run the program with --result ppp --action populate
'''

import argparse
from glob import glob
import json
import re
import sys
import boto3
from botocore.exceptions import ClientError
import colorlog
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm

# pylint: disable=no-member
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
TYPE_BODY = dict()
INSTANCE_BODY = dict()
NEURON_PPP = {"neuronType": dict(), "neuronInstance": dict()}
KEY = "searchString"
# Database
TABLE = ''
INSERTED = dict()

# General use
RELEASE_LIBRARY_BASE = "/groups/scicompsoft/informatics/data/release_libraries"
PPP_BASE = "/nrs/neuronbridge/ppp_imagery"
COUNT = {"error": 0, "skipped": 0, "write": 0, "insert": 0, "update": 0}
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
USED_PPP = dict()
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
    global CONFIG # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']


def get_nb_version():
    """ Prompt the user for a MeuronBridge version from subdirs in the base dir
        Keyword arguments:
          None
        Returns:
          None (sets ARG.NEURONBRIDGE)
    """
    base_path = RELEASE_LIBRARY_BASE if ARG.RESULT == "cdm" else PPP_BASE
    version = [re.sub('.*/', '', path)
               for path in glob(base_path + '/v[0-9]*')]
    print("Select a NeuronBridge version:")
    terminal_menu = TerminalMenu(version)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No NeuronBridge version selected")
        sys.exit(0)
    ARG.NEURONBRIDGE = version[chosen]


def get_library():
    """ Prompt the user for a library
        Keyword arguments:
          None
        Returns:
          None (sets ARG.LIBRARY)
    """
    if ARG.RESULT == "ppp":
        base_path = '/'.join([PPP_BASE, ARG.NEURONBRIDGE, '*'])
    elif ARG.TYPE == "EM":
        base_path = '/'.join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE, '*.json'])
    else:
        base_path = '/'.join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE, '*.names'])
    library = list()
    for path in glob(base_path):
        if ARG.RESULT == "cdm":
            if  "ppp_" in path:
                continue
            if ARG.TYPE == "EM" and "flylight" in path:
                continue
            if ARG.TYPE == "LM" and "flyem" in path:
                continue
        library.append(re.sub('.*/', '', path))
    print("Select a library file:")
    terminal_menu = TerminalMenu(library)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No library file selected")
        sys.exit(0)
    ARG.LIBRARY = library[chosen]


def get_result():
    """ Prompt the user for a result type and library type
        Keyword arguments:
          None
        Returns:
          None (sets ARG.RESULT and ARG.TYPE)
    """
    if not ARG.RESULT:
        print("Select result type:")
        allowed = ['cdm', 'ppp']
        terminal_menu = TerminalMenu(allowed)
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.error("No result type selected")
            sys.exit(0)
        ARG.RESULT = allowed[chosen]
    if ARG.RESULT == 'cdm':
        if not ARG.TYPE:
            print("Select library type:")
            allowed = ['EM', 'LM']
            terminal_menu = TerminalMenu(allowed)
            chosen = terminal_menu.show()
            if chosen is None:
                LOGGER.error("No library type selected")
                sys.exit(0)
            ARG.TYPE = allowed[chosen]
    else:
        ARG.TYPE = 'EM'


def get_ppp_action():
    """ Prompt the user for a PPP action
        Keyword arguments:
          None
        Returns:
          None (sets ARG.ACTION)
    """
    print("Select PPP action:")
    allowed = ['Index', 'Populate']
    terminal_menu = TerminalMenu(allowed)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No PPP action selected")
        sys.exit(0)
    ARG.ACTION = allowed[chosen].lower()


def scan_table():
    """ Get a list of keys from DynamoDB
        Keyword arguments:
          None
        Returns:
          list of keys from DynamoDB
    """
    LOGGER.info("Getting list of keys in DynamoDB")
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
    return loaded


def set_body(item):
    """ Cache neuron types and instances in TYPE_BODY and INSTANCE_BODY
        Keyword arguments:
          item: dictionary of keys/key types
        Returns:
          None
    """
    bodydict = {item["publishedName"]: bool(item["publishedName"] in USED_PPP)}
    if "neuronType" in item:
        if item["neuronType"] in TYPE_BODY:
            key = item["neuronType"]
            if item["publishedName"] not in TYPE_BODY[key]:
                TYPE_BODY[key].append(bodydict)
        else:
            TYPE_BODY[item["neuronType"]] = list([bodydict])
    if "neuronInstance" in item:
        key = item["neuronInstance"]
        if key in INSTANCE_BODY:
            if item["publishedName"] not in INSTANCE_BODY[key]:
                INSTANCE_BODY[key].append(bodydict)
        else:
            INSTANCE_BODY[key] = list([bodydict])


def set_neuron_ppp():
    """ Populate the NEURONPPP dictionary. It is keyed by key type/key, and contains
        True if any associated body is in bodies known to PPP.
        Keyword arguments:
          None
        Returns:
          None
    """
    # Entries in TYPE_BODY and INSTANCE_BODY will look something like this:
    # 'ORN_DL2d': [{'5812995304': True}, {'5812995304': True}]}
    for item in tqdm(TYPE_BODY, "Mapping neuron types"):
        NEURON_PPP["neuronType"][item] = False
        for body in TYPE_BODY[item]:
            if list(body.values())[0]:
                NEURON_PPP["neuronType"][item] = True
                break
    for item in tqdm(INSTANCE_BODY, "Mapping neuron instances"):
        NEURON_PPP["neuronInstance"][item] = False
        for body in INSTANCE_BODY[item]:
            if list(body.values())[0]:
                NEURON_PPP["neuronInstance"][item] = True
                break


def compress_list(plist):
    """ Shorten lists in the _BODY dictionaries by removing duplicates.
        Keyword arguments:
          plist: list to compress
        Returns:
          Processed dictionary
    """
    for item in plist:
        ndict = dict()
        for bdict in plist[item]:
            for body in bdict:
                ndict[body] = bdict[body]
        plist[item] = list()
        for body in ndict:
            plist[item].append({body: ndict[body]})


def perform_body_mapping(data):
    """ Cache neuron types and instances in TYPE_BODY and INSTANCE_BODY
        Keyword arguments:
          data: JSON color depth MIP data
        Returns:
          None
    """
    loaded = scan_table()
    if loaded:
        for item in tqdm(loaded, "Assigning neurons"):
            if item["keyType"] == "neuronType":
                TYPE_BODY[item["name"]] = item["bodyIDs"]
            elif item["keyType"] == "neuronInstance":
                INSTANCE_BODY[item["name"]] = item["bodyIDs"]
    LOGGER.info("Neuron types loaded from table: %d", len(TYPE_BODY))
    LOGGER.info("Neuron instances loaded from table: %d", len(INSTANCE_BODY))
    for item in tqdm(data, "Mapping body IDs"):
        set_body(item)
    LOGGER.info("Neuron types cached: %d", len(TYPE_BODY))
    LOGGER.info("Neuron instances cached: %d", len(INSTANCE_BODY))
    compress_list(TYPE_BODY)
    compress_list(INSTANCE_BODY)
    set_neuron_ppp()


def get_row(key, key_type):
    """ Get a row from DynamoDB
        Keyword arguments:
          key: key
          key_type: key type
        Returns:
          row from DynamoDB
          True or False indicating if row was returned from cache
    """
    if key in INSERTED and key_type in INSERTED[key]:
        return INSERTED[key][key_type], True
    try:
        response = TABLE.get_item(Key={"itemType": KEY, "searchKey": key.lower()})
    except ClientError as err:
        LOGGER.error(err.response['Error']['Message'])
    except Exception as err:
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)
    if not response:
        return None, False
    if "Item" in response and response["Item"] and ARG.RESULT in response["Item"]:
        if key not in INSERTED:
            INSERTED[key] = {key_type: response["Item"]}
        else:
            INSERTED[key][key_type] = response["Item"]
        return response["Item"], False
    return None, False


def insert_row(key, key_type):
    """ Insert a row into DynamoDB
        Keyword arguments:
          key: key
          key_type: key type
        Returns:
          None
    """
    if key_type == "bodyID":
        if key not in USED_PPP:
          LOGGER.warning("%s is not in PPP", key)
    payload, skip = get_row(key, key_type)
    # Skip keys that this process has already written
    if skip:
        COUNT['skipped'] += 1
        return
    if not payload:
        COUNT["insert"] += 1
    else:
        COUNT["update"] += 1
    # Skip publishng names/bodies that already have a CDM or PPP result
    if key_type in ["bodyID", "publishedName"] and payload and ARG.RESULT in payload \
                                               and payload[ARG.RESULT]:
        COUNT['skipped'] += 1
        return
    LOGGER.debug("Insert %s (%s)", key, key_type)
    if not payload:
        payload = {"itemType": KEY, "searchKey": key.lower()}
    payload["keyType"] = key_type
    payload["filterKey"] = key.lower()
    payload["name"] = key
    payload[ARG.RESULT] = True
    if ARG.RESULT == "ppp":
        payload["cdm"] = False
    payload["ppp"] = bool(key in USED_PPP)
    # Add neuron types and instances
    if ARG.TYPE == "EM" and ARG.RESULT == "cdm" and key_type != "bodyID":
        if NEURON_PPP[key_type][key]:
            payload["ppp"] = True
        if key_type == "neuronType":
            payload["bodyIDs"] = TYPE_BODY[key]
        elif key_type == "neuronInstance":
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
    """ Process a single item from color depth MIP JSON
        Keyword arguments:
          item: item from color depth MIP JSON
        Returns:
          None
    """
    if ARG.TYPE == "EM":
        if "publishedName" in item:
            insert_row(item["publishedName"], "bodyID")
        if "neuronType" in item:
            insert_row(item["neuronType"], "neuronType")
        if "neuronInstance" in item:
            insert_row(item["neuronInstance"], "neuronInstance")
    else:
        if "publishedName" in item:
            insert_row(item["publishedName"], "publishingName")


def populate_cdm():
    """ Read and process a color depth MIP JSON file
        Keyword arguments:
          item: item from color depth MIP JSON
        Returns:
          None
    """
    path = '/'.join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE, ARG.LIBRARY])
    data = list()
    if ARG.TYPE == "EM":
        try:
            with open(path) as handle:
                data = json.load(handle)
        except Exception as err:
            LOGGER.error("Could not open %s", path)
            LOGGER.error(TEMPLATE, type(err).__name__, err.args)
            sys.exit(-1)
        perform_body_mapping(data)
    else:
        try:
            with open(path) as handle:
                for line in handle:
                    data.append({"publishedName": line.rstrip()})
        except Exception as err:
            LOGGER.error("Could not open %s", path)
            LOGGER.error(TEMPLATE, type(err).__name__, err.args)
            sys.exit(-1)
    LOGGER.info("Loaded %d items from %s", len(data), path)
    for item in tqdm(data, "Processing items"):
        process_single_item(item)


def index_ppp():
    """ Create text file indices for PPP matches
        Keyword arguments:
          None
        Returns:
          None
    """
    base_path = '/'.join([PPP_BASE, ARG.NEURONBRIDGE, ARG.LIBRARY, '*', '*'])
    outer = glob(base_path)
    published = {"bodies": dict(), "names": dict()}
    LOGGER.info("Collecting body IDs and publishing names")
    for path in tqdm(outer, desc="Body ID", position=0):
        # Process a single body ID
        inner = glob(path + "/*.png")
        if inner:
            published["bodies"][path.split("/")[-1]] = 1
            for filepath in glob(path + "/*.png"):
                _, name, _ = (filepath.split("/")[-1]).split("-", 2)
                published["names"][name] = 1
    if not ARG.WRITE:
        print("Not in --write mode: will not write files")
        return
    file = {"bodies": "ppp_bodies.names",
            "names": "ppp_publishing_names.names"}
    base_path = '/'.join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE])
    for ftype in file:
        LOGGER.info("Writing %s file", ftype)
        stream = open("/".join([base_path, file[ftype]]), 'w')
        for item in tqdm(published[ftype], desc="Writing %s" % (ftype)):
            stream.write("%s\n" % (item))
            COUNT['write'] += 1
        stream.close()
    print("PPP bodies: %d" % (len(published["bodies"])))
    print("PPP names : %d" % (len(published["names"])))


def ppp_action():
    """ Process PPP matches (create or process an index)
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.ACTION == "index":
        index_ppp()
        return
    # Populate DynamoDB with PPP-only matches
    loaded = scan_table()
    items = dict()
    for item in loaded:
        if item["keyType"] not in ["neuronInstance", "neuronType"]:
            items[item["name"]] = item["keyType"]
    # Read PPP results
    for fname in ["ppp_bodies.names", "ppp_publishing_names.names"]:
        count = 0
        ppp_file = "/".join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE, fname])
        with open(ppp_file) as itemfile:
            for line in itemfile:
                USED_PPP[line.strip()] = 1
                count += 1
        itemfile.close()
        LOGGER.info("Items from %s: %d", ppp_file, count)
    for key in USED_PPP:
        if key not in items:
            LOGGER.warning(key)
            keytype = "bodyID" if key.isdigit() else "publishingName"
            insert_row(key, keytype)


def populate_table():
    """ Populate the janelia-neuronbridge-published table
        Keyword arguments:
          None
        Returns:
          None
    """
    global TABLE # pylint: disable=W0603
    get_result()
    if not ARG.NEURONBRIDGE:
        get_nb_version()
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    ddt = "janelia-neuronbridge-published"
    ddt += "-" + ARG.NEURONBRIDGE
    TABLE = dynamodb.Table(ddt)
    if ARG.RESULT == 'ppp' and not ARG.ACTION:
        get_ppp_action()
    if not ARG.LIBRARY:
        if not (ARG.RESULT == "ppp" and ARG.ACTION == "populate"):
            get_library()
    if ARG.RESULT == "cdm":
        # Read PPP results
        ppp_file = "/".join([RELEASE_LIBRARY_BASE, ARG.NEURONBRIDGE,
                             ("ppp_publishing_names.names" if ARG.TYPE == 'LM' \
                              else "ppp_bodies.names")])
        with open(ppp_file) as itemfile:
            for line in itemfile:
                USED_PPP[line.strip()] = 1
        itemfile.close()
        LOGGER.info("Read %d PPP matches", len(USED_PPP))
        populate_cdm()
    else:
        ppp_action()
    print("Inserts: %d" % (COUNT["insert"]))
    print("Updates: %d" % (COUNT["update"]))
    print("Writes:  %d" % (COUNT["write"]))
    print("Skipped: %d" % (COUNT["skipped"]))
    print("Errors:  %d" % (COUNT["error"]))


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
    PARSER.add_argument('--action', dest='ACTION', action='store',
                        choices=['index', 'populate'], help='PPP action')
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
