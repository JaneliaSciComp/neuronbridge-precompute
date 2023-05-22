''' update_dynamodb_published_versioned.py
    Update a janelia-neuronbridge-published-* DynamoDB table.
'''

import argparse
import json
import os
import re
import sys
import time
from types import SimpleNamespace
import boto3
import colorlog
import inquirer
import MySQLdb
from pymongo import MongoClient
import requests
from tqdm import tqdm
import neuronbridge_lib as NB

# pylint: disable=W0703, E1101
# Configuration
NEURON_DATA = ["neuronInstance", "neuronType"]
# Database
DATABASE = {}
ITEMS = []
# Counters
COUNT = {"bodyID": 0, "publishingName": 0, "neuronInstance": 0, "neuronType": 0,
         "images": 0, "missing": 0, "consensus": 0,
         "insertions": 0}
FAILURE = {}
KEYS = {}
KNOWN_PPP = {}


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
               "KeySchema": [{"AttributeName": "itemType", "KeyType": "HASH"},
                             {"AttributeName": "searchKey", "KeyType": "RANGE"}
                            ],
               "AttributeDefinitions": [{'AttributeName': 'itemType', 'AttributeType': 'S'},
                                        {'AttributeName': 'searchKey', 'AttributeType': 'S'}
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
    return msg


def db_connect(dbc):
    """ Connect to a database
        Keyword arguments:
          dbd: database object
        Returns:
          connector and cursor
    """
    LOGGER.info("Connecting to %s on %s", dbc.name, dbc.host)
    try:
        conn = MySQLdb.connect(host=dbc.host, user=dbc.user,
                               passwd=dbc.password, db=dbc.name)
    except MySQLdb.Error as err:
        terminate_program(sql_error(err))
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    except MySQLdb.Error as err:
        terminate_program(sql_error(err))
    return conn, cursor


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    dbconfig = create_config_object("db_config")
    # MySQL
    LOGGER.info("Connecting to SAGE")
    dbc = getattr(getattr(dbconfig, "sage"), "prod")
    DATABASE['sage'] = {"conn": "", "cursor": ""}
    DATABASE['sage']['conn'], DATABASE['sage']['cursor'] = db_connect(dbc)
    # MongoDB
    LOGGER.info("Connecting to neuronbridge MongoDB on %s", ARG.MONGO)
    try:
        dbc = getattr(getattr(dbconfig, "neuronbridge-mongo"), ARG.MONGO)
        rset = 'rsProd' if ARG.MONGO == 'prod' else 'rsDev'
        client = MongoClient(dbc.read.host, replicaSet=rset, username=dbc.read.user,
                             password=dbc.read.password)
        DATABASE["NB"] = client.neuronbridge
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    # DynamoDB
    if not ARG.VERSION:
        ARG.VERSION = NB.get_neuronbridge_version(DATABASE["NB"]["neuronMetadata"])
        if not ARG.VERSION:
            terminate_program("No NeuronBridge version selected")
    if ARG.DDBVERSION:
        if not re.match(r"v\d+(?:\.\d+)+", ARG.DDBVERSION):
            terminate_program(f"{ARG.DDBVERSION} is not a valid version")
        table = "janelia-neuronbridge-published-" + ARG.DDBVERSION
    else:
        ver = ARG.VERSION
        if not ver.startswith("v"):
            ver = f"v{ARG.VERSION}"
        table = "janelia-neuronbridge-published-" + ver
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


def get_release(slide_code):
    sql = "SELECT DISTINCT alps_release FROM image_data_mv WHERE slide_code=%s"
    try:
        DATABASE['sage']['cursor'].execute(sql, (slide_code,))
        row = DATABASE['sage']['cursor'].fetchone()
    except MySQLdb.Error as err:
        sql_error(err)
    if row and row['alps_release']:
        FAILURE[slide_code] = f"Slide code {slide_code} is published to {row['alps_release']} in SAGE"
    else:
        FAILURE[slide_code] = f"Slide code {slide_code} has no publishing release in SAGE"


def valid_row(row):
    ''' Determine if a row is valid
        Keyword arguments:
          row: single row from neuronMetadata
        Returns:
          True for valid, False for invalid
    '''
    if "publishedName" not in row or not row["publishedName"]:
        release = get_release(row['slideCode'])
        if row['slideCode'] not in FAILURE:
            get_release(sample)
        LOGGER.error("%s: %s", row['_id'], FAILURE[row['slideCode']])
        #LOGGER.error("Missing publishedName for %s (%s) in %s", row['_id'], row['slideCode'], row['libraryName'])
        COUNT["missing"] += 1
        return False
    if row["publishedName"].lower() == "no consensus":
        COUNT["consensus"] += 1
        return False
    return True


def build_bodyid_list(bodyids):
    blist = []
    for bid in bodyids:
        blist.append({bid: bid in KNOWN_PPP})
    return blist


def batch_row(name, keytype, matches, bodyids=None):
    ''' Create and save a payload for a single row
        Keyword arguments:
          name: publishedName
          keytype: key type for DynamoDB
          matches: CDM/PPP match dict
          bodyids: list of body IDs [optional]
        Returns:
          None
    '''
    payload = {"itemType": "searchString",
               "searchKey": name.lower(),
               "filterKey": name.lower(),
               "name": name,
               "keyType": keytype,
               "cdm": matches["cdm"],
               "ppp": matches["ppp"]}
    if bodyids:
        payload["bodyIDs"] = build_bodyid_list(bodyids)
    if name not in KEYS:
        ITEMS.append(payload)
        COUNT[keytype] += 1
        KEYS[name] = True


def primary_update(rlist, matches):
    ''' Run primary update to batch simple items (publishingName and bodyID)
        Keyword arguments:
          rlist: record list
          matches: match dict
        Returns:
          None
    '''
    for row in tqdm(rlist, desc="Primary update"):
        keytype = "publishingName"
        name = row["publishedName"]
        if row["libraryName"].startswith("flyem"):
            keytype = "bodyID"
        batch_row(name, keytype, matches[name])


def add_neuron(neuron, ntype):
    ''' Add a single neuron (Instance or Type) to the list of items to be stored in DynamoDB
        Keyword arguments:
          neuron: neuronInstance or neuronType
          ntype: "neuronInstance" or "neuronType"
        Returns:
          None
    '''
    nmatch = {"cdm": False, "ppp": False}
    coll = DATABASE["NB"]["neuronMetadata"]
    # Allow a body ID from any library
    payload = {ntype: neuron}
    #payload = {ntype: neuron, "libraryName": row["libraryName"]}
    results = coll.find(payload, {"publishedName": 1, "processedTags": 1,
                                  "libraryName": 1})
    bids = {}
    for brow in results:
        if brow["publishedName"] in bids or "processedTags" not in brow:
            continue
        bids[brow["publishedName"]] = True
        if "ColorDepthSearch" in brow["processedTags"] \
           and brow["processedTags"]["ColorDepthSearch"]:
            nmatch["cdm"] = True
        if "PPPMatch" in brow["processedTags"] \
           and brow["processedTags"]["PPPMatch"]:
            nmatch["ppp"] = True
    batch_row(neuron, ntype, nmatch, list(bids.keys()))


def match_count(matches):
    ''' Display ststs on found matches
        Keyword arguments:
          matches: match dict
        Returns:
          None
    '''
    mcount = {"em": 0, "lm": 0, "bcdm": 0, "bppp": 0, "pcdm": 0, "pppp": 0}
    for pname in matches:
        if pname.isdigit():
            mcount["em"] += 1
            for mtype in ["cdm", "ppp"]:
                if matches[pname][mtype]:
                    mcount["b" + mtype] += 1
        else:
            mcount["lm"] += 1
            for mtype in ["cdm", "ppp"]:
                if matches[pname][mtype]:
                    mcount["p" + mtype] += 1
    print(f"Matches:            {len(matches)}")
    print(f"  Body IDs:         {mcount['em']}")
    print(f"    CDM matches:    {mcount['bcdm']}")
    print(f"    PPP matches:    {mcount['bppp']}")
    print(f"  Publishing names: {mcount['lm']}")
    print(f"    CDM matches:    {mcount['pcdm']}")
    print(f"    PPP matches:    {mcount['pppp']}")


def update_neuron_matches(neurons):
    ''' Add neuronInstance and neuronType matches
        Keyword arguments:
          neuron: neuron instance/type dict
        Returns:
          None
    '''
    for ntype in NEURON_DATA:
        for neuron in tqdm(neurons[ntype], desc=ntype):
            add_neuron(neuron, ntype)


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
            if ARG.THROTTLE and (not COUNT["insertions"] % ARG.THROTTLE):
                time.sleep(2)
            writer.put_item(Item=item)
            COUNT["insertions"] += 1


def display_counts():
    ''' Display monitoring counts
        Keyword arguments:
          None
        Returns:
          None
    '''
    print(f"Images read:               {COUNT['images']}")
    if COUNT['missing']:
        print(f"Missing publishing name:   {COUNT['missing']}")
    if COUNT['consensus']:
        print(f"No consensus:              {COUNT['consensus']}")
    print(f"Items written to DynamoDB: {COUNT['insertions']}")
    print(f"  bodyID:                  {COUNT['bodyID']}")
    print(f"  neuronInstance:          {COUNT['neuronInstance']}")
    print(f"  neuronType:              {COUNT['neuronType']}")
    print(f"  publishingName:          {COUNT['publishingName']}")


def process_results(count, results):
    ''' Process results from neuronMetadata table
        Keyword arguments:
          count: document count
          results: documents from neuronMetadata
        Returns:
          None
    '''
    matches = {}
    rlist = []
    library = {}
    neurons = {"neuronInstance": {}, "neuronType": {}}
    for row in tqdm(results, desc="publishedName", total=count):
        if row["libraryName"] not in library:
            library[row["libraryName"]] = 0
        library[row["libraryName"]] += 1
        COUNT["images"] += 1
        if not valid_row(row):
            continue
        pname = row["publishedName"]
        if pname not in matches:
            matches[pname] = {"cdm": False, "ppp": False}
            rlist.append(row)
        if "ColorDepthSearch" in row["processedTags"] \
           and ARG.VERSION in row["processedTags"]["ColorDepthSearch"]:
            matches[pname]["cdm"] = True
        if "PPPMatch" in row["processedTags"] \
           and ARG.VERSION in row["processedTags"]["PPPMatch"]:
            matches[pname]["ppp"] = True
        # Accumulate neurons
        if pname.isdigit():
            for ntype in NEURON_DATA:
                if ntype in row and row[ntype]:
                    neurons[ntype][row[ntype]] = True
    if len(rlist) != len(matches):
        terminate_program(f"Unique primary list ({len(rlist)}) != match list({len(matches)})")
    print(f"Libraries:")
    liblen = cntlen = 0
    for lib in library:
        if len(lib) > liblen:
            liblen = len(lib)
        if len(str(library[lib])) > cntlen:
            cntlen = len(str(library[lib]))
    for lib in library:
        print(f"  {lib+':':<{liblen+1}} {library[lib]:>{cntlen}}")
    print(f"Neuron instances:   {len(neurons['neuronInstance'])}")
    print(f"Neuron types:       {len(neurons['neuronType'])}")
    match_count(matches)
    primary_update(rlist, matches)
    update_neuron_matches(neurons)
    if ARG.WRITE:
        write_dynamodb()
    else:
        COUNT["insertions"] = len(ITEMS)
    display_counts()


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
    results = coll.distinct("libraryName", payload)
    if not results:
        terminate_program(f"There are no processed tags for version {ARG.VERSION}")
    questions = [inquirer.Checkbox("to_include",
                                   message="Choose libraries to include",
                                   choices=results,
                                   default=results,
                                  )]
    answers = inquirer.prompt(questions)
    if answers["to_include"]:
        payload["libraryName"] = {"$in": answers["to_include"]}
    project = {"libraryName": 1, "publishedName": 1, "slideCode": 1,
               "processedTags": 1, "neuronInstance": 1, "neuronType": 1}
    count = coll.count_documents(payload)
    if not count:
        LOGGER.error("There are no processed tags for version %s", ARG.VERSION)
        results = {}
    else:
        results = coll.find(payload, project)
    LOGGER.info("Finding PPP matches")
    coll = DATABASE["NB"]["pppMatches"]
    pppresults = coll.distinct("sourceEmName")
    for row in pppresults:
        KNOWN_PPP[row.split("-")[0]] = True
    process_results(count, results)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update a janelia-neuronbridge-published-* table")
    PARSER.add_argument('--version', dest='VERSION', default='', help='NeuronBridge version')
    PARSER.add_argument('--ddbversion', dest='DDBVERSION', default='',
                        help='DynamoDB NeuronBridge version')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod', 'devpre', 'prodpre'],
                        help='DynamoDB manifold')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write to DynamoDB')
    PARSER.add_argument('--throttle', type=int, dest='THROTTLE',
                        default=0, help='DynamoDB batch write throttle (# items)')
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
