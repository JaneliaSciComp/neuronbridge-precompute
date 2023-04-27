''' check_neuronmetadata.py
    This program will check entries in jacs:emBody with nueronbridge:neuron
'''

import argparse
import json
import os
import sys
from types import SimpleNamespace
import colorlog
import inquirer
from pymongo import errors, MongoClient
import requests
from tqdm import tqdm

# Configuration
EDEFAULT = ["fib19", "hemibrain", "hemibrain:v0.9",
            "hemibrain:v1.0.1", "hemibrain:v1.1"]
EXCLUDE = {}
# Database
NEURONBRIDGE = 'neuronbridge-mongo'
JACS = 'neuronbridge-mongo'
DATABASE = {}
# Counts
COUNT = {"missing": 0, "nb_bid": 0, "np_bid": 0, "nb": 0, "np": 0}


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
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
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # MongoDB
    dbconfig = create_config_object("db_config")
    LOGGER.info("Connecting to Mongo neuronbridge on %s", ARG.MANIFOLD)
    dbc = getattr(getattr(getattr(dbconfig, NEURONBRIDGE), ARG.MANIFOLD), "read")
    try:
        client = MongoClient(dbc.host, replicaSet=dbc.replicaset, username=dbc.user,
                             password=dbc.password)
        DATABASE["NB"] = client.neuronbridge
    except errors.ConnectionFailure as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    LOGGER.info("Connecting to Mongo jacs on %s", ARG.MANIFOLD)
    try:
        dbc = getattr(getattr(getattr(dbconfig, JACS), ARG.MANIFOLD), "read")
        if ARG.MANIFOLD == "prod":
            client = MongoClient(dbc.host, replicaSet=dbc.replicaset,
                                 username=dbc.user, password=dbc.password)
        else:
            client = MongoClient(dbc.host)
        DATABASE["JACS"] = client.jacs
    except errors.ConnectionFailure as err:
        terminate_program(f"Could not connect to Mongo: {err}")


def get_exclusions():
    ''' Get a list of data sets to exclude
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DATABASE["JACS"].emBody
    results = coll.distinct("dataSetIdentifier")
    questions = [inquirer.Checkbox("excl",
                                   message="Data sets to exclude",
                                   choices=results,
                                   default=EDEFAULT
                                  )]
    answers = inquirer.prompt(questions)
    for excl in answers["excl"]:
        EXCLUDE[excl] = True


def mongo_check():
    ''' Check entries in jacs:emBody with nueronbridge:neuron
        Keyword arguments:
          None
        Returns:
          None
    '''
    get_exclusions()
    # emBody
    coll = DATABASE["JACS"].emBody
    payload = {"status": "Traced", "neuronType": {"$ne": None},
               "dataSetIdentifier": {"$nin": list(EXCLUDE.keys())}}
    project = {"dataSetIdentifier": 1, "name": 1}
    count = coll.count_documents(payload)
    results = coll.find(payload, project)
    in_np = {}
    for row in tqdm(results, desc="emBody", total=count):
        bid = row["name"]
        COUNT["np"] += 1
        if bid not in in_np:
            in_np[bid] = []
            COUNT["np_bid"] += 1
        in_np[bid].append(row["dataSetIdentifier"])
    # neuronMetadata
    coll = DATABASE["NB"].neuronMetadata
    payload = {"libraryName": {"$regex": "flyem"}}
    project = {"libraryName": 1, "publishedName": 1}
    count = coll.count_documents(payload)
    results = coll.find(payload, project)
    in_nb = {}
    for row in tqdm(results, desc="neuronMetadata", total=count):
        COUNT["nb"] += 1
        bid = row["publishedName"]
        if bid not in in_nb:
            in_nb[bid] = []
            COUNT["nb_bid"] += 1
        in_nb[bid].append(row["libraryName"])
    # In NeuPrint, not in NeuronBridge
    missing = []
    for npbid in tqdm(in_np, desc="Check"):
        if npbid not in in_nb:
            COUNT["missing"] += 1
            missing.append(f"{npbid} {in_np[npbid]}")
    if missing:
        filename = "missing_bodyids.txt"
        with open(filename, "w", encoding="ascii") as output:
            for miss in missing:
                output.write(f"{miss}\n")
        LOGGER.info("Wrote %s", filename)
    print(f"Entries in NeuPrint:                {COUNT['np']:6}")
    print(f"Body IDs in NeuPrint:               {COUNT['np_bid']:6}")
    print(f"Entries in NeuronBridge:            {COUNT['nb']:6}")
    print(f"Body IDs in NeuronBridge:           {COUNT['nb_bid']:6}")
    print(f"Body IDs missing from NeuronBridge: {COUNT['missing']:6}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backcheck publishedURL to neuronMetadata")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=["dev", "prod"], default="prod", help='MongoDB manifold')
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
    mongo_check()
    terminate_program()
