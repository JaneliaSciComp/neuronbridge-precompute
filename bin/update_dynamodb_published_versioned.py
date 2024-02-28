''' update_dynamodb_published_versioned.py
    Update a janelia-neuronbridge-published-* DynamoDB table.
'''

import argparse
from datetime import datetime
from operator import attrgetter
import re
import sys
import time
import boto3
import inquirer
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_lib as NB

# pylint: disable=broad-exception-caught,logging-fstring-interpolation
# Configuration
NEURON_DATA = ["neuronInstance", "neuronType"]
# Database
DATABASE = {}
DYNAMO = {}
ITEMS = []
# Counters
COUNT = {"bodyID": 0, "publishingName": 0, "neuronInstance": 0, "neuronType": 0,
        "images": 0, "missing": 0, "consensus": 0, "notreleased": 0,
         "insertions": 0}
FAILURE = {}
KEYS = {}
KNOWN_PPP = {}
DDB_NB = {}
NBODY = []


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


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    # MySQL
    dbo = attrgetter("sage.prod.read")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
    DATABASE['sage'] = JRC.connect_database(dbo)
    # MongoDB
    rwp = "write" if ARG.WRITE else "read"
    dbo = attrgetter(f"neuronbridge.{ARG.MONGO}.{rwp}")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
    DATABASE['NB'] = JRC.connect_database(dbo)
    # DynamoDB
    if not ARG.VERSION:
        ARG.VERSION = NB.get_neuronbridge_version(DATABASE["NB"]["publishedURL"])
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
        ARG.DDBVERSION = ver
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
    try:
        ddt = dynamodb_client.describe_table(TableName=table)
        DYNAMO['client'] = dynamodb_client
        DYNAMO['arn'] = ddt['Table']['TableArn']
    except dynamodb_client.exceptions.ResourceNotFoundException:
        LOGGER.warning("Table %s doesn't exist", table)


def get_release(slide_code):
    ''' Get ALPS release by slide code
        Keyword arguments:
          slide_code: slide code
        Returns:
          None
    '''
    sql = "SELECT DISTINCT alps_release FROM image_data_mv WHERE slide_code=%s"
    try:
        DATABASE['sage']['cursor'].execute(sql, (slide_code,))
        row = DATABASE['sage']['cursor'].fetchone()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    if row and row['alps_release']:
        FAILURE[slide_code] = f"Slide code {slide_code} is published to " \
                              + f"{row['alps_release']} in SAGE"
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
        get_release(row['slideCode'])
        if row['slideCode'] not in FAILURE:
            get_release(row['sourceRefId'])
        LOGGER.error("%s: %s", row['_id'], FAILURE[row['slideCode']])
        #LOGGER.error("Missing publishedName for %s (%s) in %s", row['_id'], row['slideCode'],
        #             row['libraryName'])
        COUNT["missing"] += 1
        return False
    if row["publishedName"].lower() == "no consensus":
        COUNT["consensus"] += 1
        return False
    return True


def build_bodyid_list(bodyids):
    ''' Build a list of body IDs with PPP results
        Keyword arguments:
          bodyids: list of body IDs
        Returns:
          List
    '''
    blist = []
    for bid in bodyids:
        blist.append({bid: bid in KNOWN_PPP})
    return blist


def batch_row(name, keytype, matches, bodyids=None):
    ''' Create and save a payload for a single row
        Keyword arguments:
          name: publishedName, bodyID, neuronInstance, or neuronType
          keytype: key type for DynamoDB (publishedName, bodyID, neuronInstance, or neuronType)
          matches: CDM/PPP match dict
          bodyids: list of body IDs [optional, used for neuronInstance or neuronType only]
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


def update_ddb_nb(library):
    ''' Update the DDB_NB dict
        Keyword arguments:
          library: library
        Returns:
          None
    '''
    if library not in DDB_NB:
        DDB_NB[library] = {"version": ARG.VERSION, "count": 0}
    DDB_NB[library]["count"] += 1


def primary_update(rlist, matches):
    ''' Run primary update to batch simple items (publishingName and bodyID)
        Keyword arguments:
          rlist: record list
          matches: match dict
        Returns:
          None
    '''
    for row in tqdm(rlist, desc="Primary update"):
        nmdcol = "publishedName"
        if nmdcol not in row:
            terminate_program(f"No {nmdcol} found:\n{row}")
        name = row[nmdcol]
        keytype = "publishingName"
        if row["libraryName"].startswith("flyem"):
            keytype = "bodyID"
        batch_row(name, keytype, matches[name])
        update_ddb_nb(row["libraryName"])


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
        # Set match flags
        if "ColorDepthSearch" in brow["processedTags"] \
           and brow["processedTags"]["ColorDepthSearch"]:
            nmatch["cdm"] = True
        if "PPPMatch" in brow["processedTags"] \
           and brow["processedTags"]["PPPMatch"]:
            nmatch["ppp"] = True
    NBODY.append(f"{ntype} {neuron} matches {','.join(list(bids.keys()))}")
    batch_row(neuron, ntype, nmatch, list(bids.keys()))


def match_count(matches):
    ''' Display stats on found matches
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
    print(f"Matches:            {len(matches):,}")
    print(f"  Body IDs:         {mcount['em']:,}")
    print(f"    CDM matches:    {mcount['bcdm']:,}")
    print(f"    PPP matches:    {mcount['bppp']:,}")
    print(f"  Publishing names: {mcount['lm']:,}")
    print(f"    CDM matches:    {mcount['pcdm']:,}")
    print(f"    PPP matches:    {mcount['pppp']:,}")


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
    print(f"Images read:               {COUNT['images']:,}")
    if COUNT['missing']:
        print(f"Missing publishing name:   {COUNT['missing']:,}")
    if COUNT['consensus']:
        print(f"No consensus:              {COUNT['consensus']:,}")
    if COUNT['notreleased']:
        print(f"Not released:              {COUNT['notreleased']:,}")
    print(f"Items written to DynamoDB: {COUNT['insertions']:,}")
    print(f"  bodyID:                  {COUNT['bodyID']:,}")
    print(f"  neuronInstance:          {COUNT['neuronInstance']:,}")
    print(f"  neuronType:              {COUNT['neuronType']:,}")
    print(f"  publishingName:          {COUNT['publishingName']:,}")


def process_results(count, results, publishedurl):
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
    not_released = {}
    neurons = {"neuronInstance": {}, "neuronType": {}}
    for row in tqdm(results, desc="publishedName", total=count):
        if row["libraryName"] not in library:
            library[row["libraryName"]] = 0
        library[row["libraryName"]] += 1
        COUNT["images"] += 1
        if not valid_row(row):
            continue
        pname = row["publishedName"]
        if pname not in publishedurl:
            not_released[pname] = True
            COUNT['notreleased'] += 1
            continue
        if pname not in matches:
            matches[pname] = {"cdm": False, "ppp": False}
            rlist.append(row)
        #if "ColorDepthSearch" in row["processedTags"] \
        #   and ARG.VERSION in row["processedTags"]["ColorDepthSearch"]:
        #    matches[pname]["cdm"] = True
        matches[pname]["cdm"] = True
        if "processedTags" in row \
           and "PPPMatch" in row["processedTags"] \
           and ARG.VERSION in row["processedTags"]["PPPMatch"]:
            matches[pname]["ppp"] = True
        # Accumulate neurons connected to a body id
        if pname.isdigit():
            for ntype in NEURON_DATA:
                if ntype in row and row[ntype]:
                    neurons[ntype][row[ntype]] = True
    if not_released:
        for pname in not_released:
            LOGGER.warning(f"Published name {pname} is not in publishedURL")
    # matches: key=publishing name, value={cdm: boolean, ppp: boolean}
    # rlist: list of rows from neuronMetadata (distinct publishing names)
    # neurons: key=data type, value={neuron name or instance: boolean}
    if len(rlist) != len(matches):
        terminate_program(f"Unique primary list ({len(rlist)}) != match list({len(matches)})")
    print("Libraries:")
    liblen = cntlen = 0
    for lib, val in library.items():
        if len(lib) > liblen:
            liblen = len(lib)
        if len(str(val)) > cntlen:
            cntlen = len(str(val))
    for lib, val in library.items():
        print(f"  {lib+':':<{liblen+1}} {val:>{cntlen},}")
    print(f"Neuron instances:   {len(neurons['neuronInstance']):,}")
    print(f"Neuron types:       {len(neurons['neuronType']):,}")
    match_count(matches)
    primary_update(rlist, matches)
    update_neuron_matches(neurons)
    LOGGER.info("Producing output files")
    for ntype in NEURON_DATA:
        with open(f"neuron_{ntype}.txt", 'w', encoding='ascii') as outstream:
            for row in neurons[ntype]:
                outstream.write(f"{row}\n")
    if NBODY:
        with open('neuron_body_matches.txt', 'w', encoding='ascii') as outstream:
            for row in NBODY:
                outstream.write(f"{row}\n")
    if ARG.WRITE:
        write_dynamodb()
        dts = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        for lib in library:
            key = " - ".join([lib, ARG.VERSION])
            resp = DYNAMO['client'].tag_resource(ResourceArn=DYNAMO['arn'],
                                                 Tags=[{'Key': key,
                                                        'Value': dts},])
            if 'HTTPStatusCode' not in resp['ResponseMetadata'] or \
               resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                LOGGER.warning("Could not write tag for %s", key)
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
    coll = DATABASE["NB"]["publishedURL"]
    results = coll.distinct("publishedName")
    publishedurl = {}
    for res in results:
        publishedurl[res] = True
    LOGGER.info(f"Published names in publishedURL: {len(publishedurl):,}")
    coll = DATABASE["NB"]["neuronMetadata"]
    #payload = {"$or": [{"processedTags.ColorDepthSearch": ARG.VERSION},
    #                   {"processedTags.PPPMatch": ARG.VERSION}]}
    payload = {"$or": [{"tags": ARG.VERSION},
                       {"processedTags.PPPMatch": ARG.VERSION}]}
    results = coll.aggregate([{"$match": payload}, {"$group": {"_id": "$libraryName",
                                                               "count": {"$sum":1}}}])
    lkeys = []
    lchoices = []
    for res in results:
        lkeys.append(res['_id'])
        lchoices.append((f"{res['_id']} ({res['count']:,})", res['_id']))
    if not lkeys:
        terminate_program(f"There are no processed tags for version {ARG.VERSION}")
    questions = [inquirer.Checkbox("to_include",
                                   message="Choose libraries to include",
                                   choices=lchoices,
                                   default=lkeys,
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
        LOGGER.info("Selecting images from neuronMetaData")
        results = coll.find(payload, project)
    LOGGER.info("Finding PPP matches in pppMatches")
    coll = DATABASE["NB"]["pppMatches"]
    pppresults = coll.distinct("sourceEmName")
    for row in pppresults:
        KNOWN_PPP[row.split("-")[0]] = True
    LOGGER.info(f"Processing neuronMetaData ({count:,} images)")
    process_results(count, results, publishedurl)
    if not ARG.WRITE:
        return
    coll = DATABASE["NB"]["ddb_published_versioned"]
    LOGGER.info("Updating ddb_published_versioned for version %s", ARG.DDBVERSION)
    payload = coll.find_one({"dynamodb_version": ARG.DDBVERSION})
    if not payload:
        payload = {"dynamodb_version": ARG.DDBVERSION,
                   "components": {}}
    for lib, val in DDB_NB.items():
        payload['components'][lib] = val
    results = coll.update_one({"dynamodb_version": ARG.DDBVERSION}, {"$set": payload}, upsert=True)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update a janelia-neuronbridge-published-* table")
    PARSER.add_argument('--version', dest='VERSION', default='', help='NeuronBridge tag version')
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
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    update_dynamo()
    terminate_program()
