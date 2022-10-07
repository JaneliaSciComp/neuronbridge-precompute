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
# Configuration

# Database
DATABASE = {}
# General use
NEURONBRIDGE_JSON_BASE = '/nrs/neuronbridge'
PPP_BASE = NEURONBRIDGE_JSON_BASE + '/ppp_imagery'
# Counters
COUNT = {"matches": 0, "Missing sourceImageFiles": 0}


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
    if not ARG.NEURONBRIDGE:
        ARG.NEURONBRIDGE = NB.get_ppp_version(DATABASE["NB"].pppMatches)


def handle_single_entry(row):
    ''' Write copy/s3cp commands for a single match
        Keyword arguments:
          row: row from pppMatches
        Returns:
          None
    '''
    COUNT["matches"] += 1
    if "sourceImageFiles" not in row:
        COUNT["Missing sourceImageFiles"] += 1
        return
    bucket = AWS.s3_bucket.ppp
    bucket += '-' + ARG.MANIFOLD
    bid = (row["sourceEmName"].split("-"))[0]
    prefix = bid[0:2]
    lib = getattr(CDM, row["sourceEmLibrary"]).name
    newdir = '/'.join([PPP_BASE, "v" + ARG.NEURONBRIDGE, lib, prefix, bid])
    files = row["sourceImageFiles"]
    template = "JRC2018_Unisex_20x_HR" #PLUG
    for file in files:
        target = files[file]
        COPY.write(f"cp {files[file]} {newdir}\n")
        S3CP.write(f"{files[file]}\t{bucket}/{template}/{lib}/{prefix}/{bid}/{target}\n")


def handle_matches():
    ''' Main routine to update DynamoDB from MongoDB neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DATABASE["NB"].pppMatches
    LOGGER.info("Getting match count for version %s", ARG.NEURONBRIDGE)
    count = coll.count_documents({"tags": ARG.NEURONBRIDGE})
    LOGGER.info("Getting matches for version %s", ARG.NEURONBRIDGE)
    results = coll.find({"tags": ARG.NEURONBRIDGE})
    for row in tqdm(results, total=count):
        handle_single_entry(row)
    print(f"Matches read:             {COUNT['matches']}")
    print(f"Missing sourceImageFiles: {COUNT['Missing sourceImageFiles']}" \
          + f" ({COUNT['Missing sourceImageFiles']/COUNT['matches']*100.0:.2f}%)")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload PPP matches")
    PARSER.add_argument('--library', dest='LIBRARY', action='store', help='Library')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', default='',
                        help='NeuronBridge data version')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod', 'devpre', 'prodpre'],
                        help='AWS S3 manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold [dev, prod]')
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
    handle_matches()
    terminate_program()
