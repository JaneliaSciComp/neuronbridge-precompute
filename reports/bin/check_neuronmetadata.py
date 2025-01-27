''' check_neuronmetadata.py
    This program will check entries in jacs:emBody with nueronbridge:neuron
'''

import argparse
from operator import attrgetter
import sys
import inquirer
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# Configuration
EDEFAULT = ["fib19", "hemibrain", "hemibrain:v0.9",
            "hemibrain:v1.0.1", "hemibrain:v1.1"]
EXCLUDE = {}
# Database
DATABASE = {}
# Counts
COUNT = {"missing": 0, "nb_bid": 0, "np_bid": 0, "nb": 0, "np": 0}


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # MongoDB
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"neuronbridge.{ARG.MANIFOLD}.read")(dbconfig)
    for source in ("jacs", "neuronbridge"):
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DATABASE[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_exclusions():
    ''' Get a list of data sets to exclude
        Keyword arguments:
          None
        Returns:
          None
    '''
    coll = DATABASE["jacs"].emBody
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
    coll = DATABASE["jacs"].emBody
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
    coll = DATABASE["neuronbridge"].neuronMetadata
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
    print(f"Entries in NeuPrint:                {COUNT['np']:7,}")
    print(f"Body IDs in NeuPrint:               {COUNT['np_bid']:7,}")
    print(f"Entries in NeuronBridge:            {COUNT['nb']:7,}")
    print(f"Body IDs in NeuronBridge:           {COUNT['nb_bid']:7,}")
    print(f"Body IDs missing from NeuronBridge: {COUNT['missing']:7,}")


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
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    mongo_check()
    terminate_program()
