''' upload_ppp.py
    Create upload orders for PPP matches.
'''

import argparse
import json
import os
import sys
from time import strftime
from types import SimpleNamespace
import colorlog
from pymongo import MongoClient
import requests
from tqdm import tqdm
import neuronbridge_lib as NB

# pylint: disable=W0703, E1101
# Database
MONGODB = "neuronbridge-mongo"
DATABASE = {}
BATCH_SIZE = 500000
# General use
NEURONBRIDGE_JSON_BASE = "/nrs/neuronbridge"
PPP_BASE = NEURONBRIDGE_JSON_BASE + "/ppp_imagery"
RENAME_COMPONENTS = ["maskPublishedName", "lmPublishedName", "lmSlideCode",
                     "lmObjective",
                     "inputAlignmentSpace"]
KEYS = {}
# Output files
COPY = S3CP = None
# Counters
COUNT = {"files": 0, "matches": 0, "lmPublishedName": 0,
         "mongo": 0, "processed": 0}


def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if COPY:
        COPY.close()
        S3CP.close()
        for fpath in [COPY_FILE, S3CP_FILE]:
            if os.path.exists(fpath) and not os.path.getsize(fpath):
                os.remove(fpath)
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


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    data = (call_responder('config', 'config/db_config'))["config"]
    # MongoDB
    LOGGER.info("Connecting to neuronbridge MongoDB on %s", ARG.MONGO)
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        mongo = data[MONGODB][ARG.MONGO][rwp]
        rset = 'rsProd' if ARG.MONGO == 'prod' else 'rsDev'
        client = MongoClient(mongo['host'], replicaSet=rset, username=mongo['user'],
                             password=mongo['password'])
        DATABASE["NB"] = client.neuronbridge
        DATABASE["NB_count"] = 0
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    if not ARG.NEURONBRIDGE:
        ARG.NEURONBRIDGE = NB.get_ppp_version(DATABASE["NB"].pppMatches)


def all_components_present(row):
    ''' Write a batch of records to Mongo
        Keyword arguments:
          row: row from pppMatches
        Returns:
          True or False
    '''
    good = True
    for key in RENAME_COMPONENTS:
        if key not in row:
            good = False
            LOGGER.error("No %s for %s", key, row["_id"])
    return good


def write_mongo():
    ''' Write a batch of records to Mongo
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DATABASE["NB"].pppmURL
    result = coll.insert_many(ITEMS)
    COUNT["mongo"] += len(result.inserted_ids)
    ITEMS.clear()
    DATABASE["NB_count"] = 0


def add_row_to_mongo(mid, copydict, filedict, thumbdict):
    ''' Create and save a payload for a single row
        Keyword arguments:
          mid: _id from pppMatch
          copydict: dict with /nrs file locations
          filedict: dict with S3 file locations
        Returns:
          None
    '''
    payload = {"_id": mid,
               "copiedFiles": copydict,
               "uploadedFiles": filedict,
               "uploadedThumbnails": thumbdict,
               }
    ITEMS.append(payload)
    DATABASE["NB_count"] += 1
    if DATABASE["NB_count"] >= BATCH_SIZE:
        write_mongo()


def handle_single_entry(row):
    ''' Write copy/s3cp commands for a single match
        Keyword arguments:
          row: row from pppMatches
        Returns:
          None
    '''
    COUNT["matches"] += 1
    if "lmPublishedName" not in row:
        COUNT["lmPublishedName"] += 1
        return
    COUNT["processed"] += 1
    bucket = AWS.s3_bucket.ppp
    bucket += '-' + ARG.MANIFOLD
    bid = (row["sourceEmName"].split("-"))[0]
    prefix = bid[0:2]
    lib = getattr(CDM, row["sourceEmLibrary"]).name
    row["maskPublishedName"] = bid
    if "lmObjective" not in row:
        row["lmObjective"] = "40x"
    if not all_components_present(row):
        terminate_program("Missing file components")
    template = row["inputAlignmentSpace"]
    files = row["sourceImageFiles"]
    fs_prefix = '/'.join([PPP_BASE, "v" + ARG.NEURONBRIDGE, lib, prefix, bid])
    s3_prefix = f"{template}/{lib}/{prefix}/{bid}"
    copydict = {}
    filedict = {}
    thumbdict = {}
    for file in files:
        newname = "-".join(row[key] for key in RENAME_COMPONENTS)
        newname += f"-{file.lower()}.png"
        if newname in KEYS:
            terminate_program(f"Duplicate file {newname} for {row['_id']}")
        KEYS[newname] = True
        copydict[file] = f"{fs_prefix}/{newname}"
        filedict[file] = f"{s3_prefix}/{newname}"
        if file == "CH":
            thumbdict[file] = filedict[file].replace(".png", ".jpg")
        COPY.write(f"cp \"{files[file]}\" {fs_prefix}/{newname}\n")
        S3CP.write(f"{files[file]}\t{bucket}/{s3_prefix}/{newname}\n")
        COUNT["files"] += 1
    if ARG.WRITE:
        add_row_to_mongo(row["_id"], copydict, filedict, thumbdict)


def handle_matches():
    ''' Main routine to update DynamoDB from MongoDB neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DATABASE["NB"].pppMatches
    payload = {"tags": ARG.NEURONBRIDGE,
               "sourceImageFiles": {"$ne": None}}
    if ARG.ID:
        payload["_id"] = int(ARG.ID)
    LOGGER.info("Getting match count for version %s", ARG.NEURONBRIDGE)
    count = coll.count_documents(payload)
    LOGGER.info("Getting matches for version %s", ARG.NEURONBRIDGE)
    results = coll.find(payload)
    for row in tqdm(results, total=count):
        handle_single_entry(row)
    if DATABASE["NB_count"]:
        write_mongo()
    print(f"Matches read:            {COUNT['matches']:10,}")
    print(f"Missing lmPublishedName: {COUNT['lmPublishedName']:10,}")
    print(f"Rows processed:          {COUNT['processed']:10,}")
    print(f"Rows written to Mongo:   {COUNT['mongo']:10,}")
    print(f"Files to transfer:       {COUNT['files']:10,}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload PPP matches")
    PARSER.add_argument('--library', dest='LIBRARY', action='store', help='Library')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', default='',
                        help='NeuronBridge data version')
    PARSER.add_argument('--id', dest='ID', default='',
                        help='Single MongoDB _id to process')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod', 'devpre', 'prodpre'],
                        help='AWS S3 manifold (prod)')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold [dev, prod] (prod)')
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
    AWS = create_config_object("aws")
    CDM = create_config_object("cdm_library")
    initialize_program()
    STAMP = strftime("%Y%m%dT%H%M%S")
    COPY_FILE = f"upload_ppp-{ARG.NEURONBRIDGE}-{ARG.MANIFOLD}-{STAMP}_copy.sh"
    COPY = open(COPY_FILE, 'w', encoding='ascii')
    S3CP_FILE = f"upload_ppp-{ARG.NEURONBRIDGE}-{ARG.MANIFOLD}-{STAMP}_s3cp.order"
    S3CP = open(S3CP_FILE, 'w', encoding='ascii')
    ITEMS = []
    handle_matches()
    terminate_program()
