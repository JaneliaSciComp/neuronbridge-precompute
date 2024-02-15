''' This program will update janelia-neuronbridge-published-stacks
'''

import argparse
from copy import deepcopy
from operator import attrgetter
import sys
import boto3
from colorama import Fore, Style
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC


# Configuration
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
KEY = "searchString"
INSERTED = {}
SLIDE_CODE = {}
# Database
MONGODB = 'neuronbridge-mongo'
DBASE = {}
ITEMS = []
# General
COUNT = {"write": 0}

# pylint: disable=W0703,E1101

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


def initialize_program():
    """ Initialize
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    # Database
    for source in ("neuronbridge",):
        manifold = "prod" if source == "sage" else ARG.MANIFOLD
        rwp = "write" if ARG.WRITE else "read"
        dbo = attrgetter(f"{source}.{manifold}.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DBASE[source] = JRC.connect_database(dbo)
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)

    # DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    ddt = "janelia-neuronbridge-published-stacks"
    LOGGER.info("Connecting to %s", ddt)
    DBASE["ddb"] = dynamodb.Table(ddt)


def set_payload(row):
    """ Set a DynamoDB item payload
        Keyword arguments:
          row: row from MongoDB publishedLMImage collection
        Returns:
          payload
    """
    key = row["slideCode"]
    skey = "-".join([row["objective"], row["alignmentSpace"]])
    ckey = "-".join([key, skey])
    if ckey in INSERTED:
        terminate_program(f"Key {ckey} is already in table")
    SLIDE_CODE[row["slideCode"]] = True
    INSERTED[ckey] = True
    payload = {"itemType": ckey.lower(),
              }
    for itm in ["name", "area", "tile", "releaseName", "slideCode", "objective", "alignmentSpace"]:
        payload[itm] = row[itm]
    payload["files"] = deepcopy(row["files"])
    return payload


def write_dynamodb():
    ''' Write rows from ITEMS to DynamoDB in batch
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Batch writing %s items to DynamoDB janelia-neuronbridge-published-stacks",
                len(ITEMS))
    with DBASE["ddb"].batch_writer() as writer:
        for item in tqdm(ITEMS, desc="DynamoDB"):
            writer.put_item(Item=item)
            COUNT["write"] += 1


def process_mongo():
    """ Use a JACS sample result to find the Unisex CDM
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info("Fetching records from publishedLMImage")
    try:
        coll = DBASE["neuronbridge"].publishedLMImage
        rows = coll.find()
        count = coll.count_documents({})
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))
    LOGGER.info(f"Records in Mongo publishedLMImage: {count:,}")
    LOGGER.info("Building payload list for DynamoDB update")
    for row in tqdm(rows, total=count):
        payload = set_payload(row)
        ITEMS.append(payload)
    if ARG.WRITE:
        write_dynamodb()
    else:
        COUNT["write"] = count
    tcolor = Fore.GREEN if count == COUNT["write"] else Fore.RED
    print(f"Items read:    {tcolor}{count:,}{Style.RESET_ALL}")
    print(f"Slide codes:   {len(SLIDE_CODE):,}")
    print(f"Items written: {tcolor}{COUNT['write']:,}{Style.RESET_ALL}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update janelia-neuronbridge-published-stacks")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='Manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to databases')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_mongo()
    sys.exit(0)
