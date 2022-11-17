''' backcheck_publishedurl.py
    This program will backcheck publishedURL to neuronMetadata
'''

import argparse
import json
import os
import sys
from types import SimpleNamespace
from colorama import Fore, Style
import colorlog
from pymongo import errors, MongoClient
import requests

# Database
MONGODB = 'neuronbridge-mongo'
DATABASE = {}


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
    dbconfig = create_config_object("db_config")
    # MongoDB
    LOGGER.info("Connecting to Mongo on %s", ARG.MANIFOLD)
    dbc = getattr(getattr(getattr(dbconfig, MONGODB), ARG.MANIFOLD), "read")
    try:
        client = MongoClient(dbc.host, replicaSet=dbc.replicaset, username=dbc.user,
                             password=dbc.password)
        DATABASE["MONGO"] = client.neuronbridge
    except errors.ConnectionFailure as err:
        terminate_program(f"Could not connect to Mongo: {err}")


def process_row(row, maxl, nonversion, libcount, primary):
    ''' Process a collection row
        Keyword arguments:
          row: single results row
          maxl: maximum value length dics
          noversion: non-version tag dict
          libcount: library counter dics
          primary: primary collection
        Returns:
          None
    '''
    secondary = "neuronMetadata" if primary == "publishedURL" else "publishedURL"
    lib = row['_id']['lib']
    if len(lib) > maxl["lib"]:
        maxl["lib"] = len(lib)
    tag = row['_id']['tag']
    if not tag[0].isdigit():
        nonversion[tag] = True
        return
    if len(tag) > maxl["tag"]:
        maxl["tag"] = len(tag)
    if lib not in libcount:
        libcount[lib] = {}
    #libcount[lib][tag] = {primary: lid['count'], secondary: "-"}
    if tag not in libcount[lib]:
        libcount[lib][tag] = {secondary: "-", primary: row['count']}
    else:
        libcount[lib][tag][primary] = row['count']


def process_collection(collection, maxl, nonversion, libcount):
    ''' Process a collection
        Keyword arguments:
          collection: collection to process
          maxl: maximum value length dics
          noversion: non-version tag dict
          libcount: library counter dics
        Returns:
          None
    '''
    payload = [{"$unwind": "$tags"},
               {"$project": {"_id": 0, "libraryName": 1, "tags": 1}},
               {"$group": {"_id": {"lib": "$libraryName", "tag": "$tags"}, "count":{"$sum": 1}}}]
    LOGGER.info("Getting counts from %s", collection)
    coll = DATABASE["MONGO"][collection]
    results = coll.aggregate(payload)
    for row in results:
        process_row(row, maxl, nonversion, libcount, collection)


def set_colors(nmd, pub):
    ''' Set colors for neuronMetadata and publishedURL counter values
        Keyword arguments:
          nmd: neuronMetadata counter value
          pub: publishedURL counter value
        Returns:
          color1: first color (neuronMetadata)
          color2: second color (publishedURL)
    '''
    color1 = color2 = ""
    if "-" in [nmd, pub]:
        if nmd == "-":
            color1 = Fore.RED
        if pub == "-":
            color2 = Fore.RED
    elif nmd != pub:
        if nmd > pub:
            color1 = Fore.RED
        else:
            color2 = Fore.RED
    return color1, color2


def mongo_check():
    ''' Backcheck publishedURL to neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    nonversion = {}
    libcount = {}
    maxl = {"lib": 0, "tag": 0}
    for collection in ["neuronMetadata", "publishedURL"]:
        process_collection(collection, maxl, nonversion, libcount)
    if libcount:
        print(f"{'Library':{maxl['lib']}} {'Tag':{maxl['tag']}} "
              + f"{'neuronMetadata':>14} {'publishedURL':>12}")
    for lib in sorted(libcount):
        for tag in sorted(libcount[lib]):
            nmd = libcount[lib][tag]['neuronMetadata']
            pub = libcount[lib][tag]['publishedURL']
            color1, color2 = set_colors(nmd, pub)
            print(f"{lib:{maxl['lib']}} {tag:{maxl['tag']}} "
                  + f"{color1}{nmd:>14}{Style.RESET_ALL} {color2}{pub:>12}{Style.RESET_ALL}")
    if nonversion:
        print("\nNon-version tags:")
        for tag in sorted(nonversion):
            print(f"  {tag}")


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
