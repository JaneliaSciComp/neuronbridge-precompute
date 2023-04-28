''' backcheck_publishedurl.py
    This program will backcheck publishedURL to neuronMetadata
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
from common_lib import setup_logging, get_config, connect_database

# Database
MONGODB = 'neuronbridge'
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


def initialize_program():
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"neuronbridge.{ARG.MANIFOLD}.read")(dbconfig)
    try:
        DATABASE["MONGO"] = connect_database(dbo)
    except Exception as err:
        terminate_program(err)


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
    LOGGER = setup_logging(ARG)
    initialize_program()
    mongo_check()
    terminate_program()
