''' process_check.py
    This program will check the NeuronBridge backend process.
'''

import argparse
from operator import attrgetter
import os
import re
import sys
from colorama import Fore, Style
import requests
import aws_s3_common.aws_s3_common as AW
import jrc_common.jrc_common as JRC


# pylint: disable=R1710, W0703
# Database
DBM = {}

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


def call_responder(server, endpoint, authenticate=False):
    """ Call a REST API
        Keyword arguments:
          server: server name
          endpoint: endpoint
          authenticate: authenticate to server
        Returns:
          JSON
    """
    url = attrgetter(f"{server}.url")(REST) + endpoint
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


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for source in ("jacs", "neuronbridge"):
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DBM[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


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
    coll = DBM["jacs"].emDataSet
    results = coll.find({})
    dset = {}
    for row in results:
        if row["version"]:
            vlib = ":v".join([row["name"].replace("flywire_", ""), row["version"]])
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
        coll = DBM["neuronbridge"].neuronMetadata
    else:
        coll = DBM["neuronbridge"].publishedURL
    dset = {}
    results = coll.distinct("libraryName")
    for row in results:
        if row.startswith('flylight'):
            dset[row] = True
        else:
            regex = re.search(r"(flyem|flywire)_([^_]+)_(.+)", row)
            dset[":v".join([regex[2], regex[3].replace("_", ".")])] = True
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
        for prefix in AW.get_prefixes(bucket):
            if not prefix.startswith("JRC"):
                continue
            libs =  AW.get_prefixes(bucket, prefix)
            for lib in libs:
                newlib = lib.lower()
                newlib = newlib.replace("flyem_", "").replace("_v", ":v")
                newlib = newlib.lower().replace("flywire_", "").replace("_v", ":v")
                if newlib.startswith("flylight"):
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
    print(f"{'Data set':<{width}}  {'NeuPrint':<8}  {'emData':<6}  {'Metadata':<8}  "
          + f"{'Published':<9}  {'AWS loc':<7}")
    first = "-"*width
    print(f"{first}  {'-'*8}  {'-'*6}  {'-'*8}  {'-'*9}  {'-'*7}")
    for dset in sorted(master):
        if dset.startswith('flylight'):
            nprint = sync = Fore.RED + f"{'N/A':>4}"
        else:
            nprint = step["neuprint"][dset] if dset in step["neuprint"] else Fore.RED + f"{'No':>4}"
            sync = Fore.GREEN + " Yes" if dset in step["sync"] else Fore.RED + f"{'No':>4}"
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
    LOGGER = JRC.setup_logging(ARG)
    REST = JRC.get_config("rest_services")
    initialize_program()
    check_process()
    terminate_program()
