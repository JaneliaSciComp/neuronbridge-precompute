''' process_check.py
    This program will check the NeuronBridge backend process.
'''

import argparse
import json
import os
import re
import sys
from types import SimpleNamespace
from colorama import Fore, Style
import colorlog
from pymongo import errors, MongoClient
import requests
from aws_s3_lib import get_prefixes

# pylint: disable=R1710, W0703
# Database
DBM = {}

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


def call_responder(server, endpoint, authenticate=False):
    """ Call a REST API
        Keyword arguments:
          server: server name
          endpoint: endpoint
          authenticate: authenticate to server
        Returns:
          JSON
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        if authenticate:
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer " + os.environ["NEUPRINT_JWT"]}
            req = requests.get(url, headers=headers, timeout=10)
        else:
            req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Status: {str(req.status_code)}")


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
    LOGGER.info("Connecting to Mongo neuronbridge on %s", ARG.MANIFOLD)
    try:
        dbc = getattr(getattr(getattr(dbconfig, "neuronbridge-mongo"), ARG.MANIFOLD), "read")
        client = MongoClient(dbc.host, replicaSet=dbc.replicaset, username=dbc.user,
                             password=dbc.password)
        DBM["NB"] = client.neuronbridge
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    LOGGER.info("Connecting to Mongo jacs on %s", ARG.MANIFOLD)
    try:
        dbc = getattr(getattr(getattr(dbconfig, "jacs-mongo"), ARG.MANIFOLD), "read")
        if ARG.MANIFOLD == "prod":
            client = MongoClient(dbc.host, replicaSet=dbc.replicaset)
        else:
            client = MongoClient(dbc.host)
        DBM["JACS"] = client.jacs
        if ARG.MANIFOLD == "prod":
            DBM["JACS"].authenticate(dbc.user, dbc.password)
    except errors.ConnectionFailure as err:
        terminate_program(f"Could not connect to Mongo: {err}")


def process_em_neuprint():
    """ Process EM datasets from pre and prod
        Keyword arguments:
          None
        Returns:
          Dataset dictionary
    """
    dset = {}
    for server in ("neuprint-pre", "neuprint"):
        response = call_responder(server, 'dbmeta/datasets', True)
        datasets = list(response.keys())
        for dataset in datasets:
            if server == "neuprint-pre":
                dset[dataset] = Fore.YELLOW + f"{'pre':>4}"
            else:
                dset[dataset] = Fore.GREEN + "prod"
    return dset


def process_jacs_sync():
    """ Process libraries in JACS emDataSet
        Keyword arguments:
          None
        Returns:
          JACS dictionary
    """
    coll = DBM["JACS"].emDataSet
    results = coll.find({})
    dset = {}
    for row in results:
        if row["version"]:
            vlib = ":v".join([row["name"], row["version"]])
        else:
            vlib = row["name"]
        dset[vlib] = True
    return dset


def process_neuronbridge(nb_coll):
    """ Process libraries in NeuronBridge collection
        Keyword arguments:
          nb_coll: NeuronBridge collection
        Returns:
          NeuronBridge dictionary
    """
    if nb_coll == "neuronMetadata":
        coll = DBM["NB"].neuronMetadata
    else:
        coll = DBM["NB"].publishedURL
    dset = {}
    results = coll.distinct("libraryName")
    for row in results:
        if "flyem_" in row:
            regex = re.search(r"flyem_([^_]+)_(.+)", row)
            dset[":v".join([regex[1], regex[2].replace("_", ".")])] = True
        else:
            dset[row] = True
    return dset


def process_aws():
    """ Process library prefixes in AWS S3
        Keyword arguments:
          None
        Returns:
          AWS dictionary
    """
    awslib = {}
    for awsman in ("devpre", "prodpre", ""):
        bucket = "janelia-flylight-color-depth"
        if awsman:
            manifold = awsman
            bucket += f"-{awsman}"
        else:
            manifold = "prod"
        for prefix in get_prefixes(bucket):
            if not prefix.startswith("JRC"):
                continue
            libs =  get_prefixes(bucket, prefix)
            for lib in libs:
                newlib = lib.lower().replace("flyem_", "").replace("_v", ":v")
                if newlib.startswith("flylight_"):
                    newlib = newlib.replace("_drivers", "").replace("-", "_")
                    newlib += "_published"
                awslib[newlib] = f"{manifold:>7}"
    return awslib


def check_process():
    """ Report on NeuronBridge backend process
        Keyword arguments:
          None
        Returns:
          None
    """
    step = {"neuprint": process_em_neuprint(),
            "sync": process_jacs_sync(),
            "metadata": process_neuronbridge("neuronMetadata"),
            "published": process_neuronbridge("publishedURL"),
            "aws": process_aws()}
    master = {}
    width = 0
    for src in ("neuprint", "sync", "metadata", "published"):
        for dset in step[src]:
            if len(dset) > width:
                width = len(dset)
            master[dset] = True
    print(f"{'Data set':<{width}}  {'NeuPrint':<8}  {'JACS':<4}  {'Metadata':<8}  "
          + f"{'Published':<9}  {'AWS loc':<7}")
    first = "-"*width
    print(f"{first}  {'-'*8}  {'-'*4}  {'-'*8}  {'-'*9}  {'-'*7}")
    for dset in sorted(master):
        nprint = step["neuprint"][dset] if dset in step["neuprint"] else Fore.RED + f"{'No':>4}"
        sync = Fore.GREEN + "Yes" if dset in step["sync"] else Fore.RED + f"{'No':>3}"
        mdata = Fore.GREEN + "Yes" if dset in step["metadata"] else Fore.RED + f"{'No':>3}"
        pub = Fore.GREEN + "Yes" if dset in step["published"] else Fore.RED + f"{'No':>3}"
        aws = Fore.GREEN + step["aws"][dset] if dset in step["aws"] else ""
        print(f"{dset:<{width}}     {nprint:>8}    {sync:>4}     {mdata:>8}      "
              + f"{pub:>9}     {aws:>7}{Style.RESET_ALL}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Check NeuronBridge backend process')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod'], default='prod', help='Manifold')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
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
    check_process()
    terminate_program()
