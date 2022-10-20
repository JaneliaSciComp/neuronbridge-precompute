''' check_neuronmetadata.py
    This program will check entries in jacs:emBody with nueronbridge:neuron
'''

import argparse
import os
import sys
import colorlog
import inquirer
from pymongo import errors, MongoClient
import requests
from tqdm import tqdm

# Configuration
CONFIG = {'config': {'url': os.environ.get('CONFIG_SERVER_URL')}}
EDEFAULT = ["fib19", "hemibrain", "hemibrain:v0.9",
            "hemibrain:v1.0.1", "hemibrain:v1.1"]
EXCLUDE = {}
# Database
NEURONBRIDGE = 'neuronbridge-mongo'
JACS = 'jacs-mongo'
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
    if server == "config" and endpoint:
        endpoint = f"config/{endpoint}"
    url = (CONFIG[server]['url'] if server else '') + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Could not get response from {url}: {req.text}")
    return False


def initialize_program():
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    for key, val in call_responder('config', 'rest_services')['config'].items():
        CONFIG[key] = val
    # MongoDB
    data = (call_responder('config', 'db_config'))["config"]
    LOGGER.info("Connecting to Mongo neuronbridge on %s", ARG.MANIFOLD)
    try:
        rset = 'rsProd' if ARG.MANIFOLD == 'prod' else 'rsDev'
        mongo = data[NEURONBRIDGE][ARG.MANIFOLD]["read"]
        client = MongoClient(mongo['host'], replicaSet=rset)
        DATABASE["NB"] = client.admin
        DATABASE["NB"].authenticate(mongo['user'], mongo['password'])
        DATABASE["NB"] = client.neuronbridge
    except errors.ConnectionFailure as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    LOGGER.info("Connecting to Mongo jacs on %s", ARG.MANIFOLD)
    try:
        rset = "replWorkstation"
        mongo = data[JACS][ARG.MANIFOLD]["read"]
        client = MongoClient(mongo['host'], replicaSet=rset)
        DATABASE["JACS"] = client.jacs
        DATABASE["JACS"].authenticate(mongo['user'], mongo['password'])
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
    payload = {"libraryName": {"$regex": u"flyem"}}
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
    initialize_program()
    mongo_check()
    terminate_program()
