''' dataset_report.py
    Report on datasets in MongoDB
'''

import argparse
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Globals
ARG = LOGGER = None
# Database
DB = {}

# -----------------------------------------------------------------------------

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
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for dbname in ('sage', 'neuronbridge'):
        man = 'prod' if dbname == 'sage' else ARG.MANIFOLD
        dbo = attrgetter(f"{dbname}.{man}.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        DB[dbname] = JRC.connect_database(dbo)


def processing():
    """ Process the data
        Keyword arguments:
          None
        Returns:
          None
    """
    payload = [{"$match": {"tags": "3.8.1"}},
               {"$match": {"tags": {"$nin": ["junk", "validationError"]}}},
               {"$project": {"libraryName": 1, "publishedName": 1, "mipId": 1, "sourceRefId": 1,
                             "alignmentSpace": 1}},
               {"$sort": {"libraryName": 1, "publishedName": 1}}]
    LOGGER.info("Getting datasets from NeuronBridge")
    try:
        rows = DB['neuronbridge'].neuronMetadata.aggregate(payload)
    except Exception as err:
        terminate_program(err)
    dataset = {}
    for row in rows:
        if row['libraryName'] not in dataset:
            dataset[row['libraryName']] = {'images': 0, 'publishedName': {'20x': {}, '40x': {}}, \
                                           'mipId': {'20x': {}, '40x': {}},
                                           'sourceRefId': {'20x': {}, '40x': {}}}
        dataset[row['libraryName']]['images'] += 1
        for field in ('publishedName', 'mipId', 'sourceRefId'):
            align = '20x' if '20x' in row['alignmentSpace'] else '40x'
            dataset[row['libraryName']][field][align][row[field]] = True
    for library, data in dataset.items():
        print(f"{library}: {data['images']:,} images")
        for field, value in data.items():
            if field == 'images':
                continue
            alignl = []
            for mag, align in value.items():
                if align:
                    alignl.append(f"{len(align):,} {mag}")
            alignstr = ', '.join(alignl)
            print(f"  {field}: {alignstr}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Report on datasets in MongoDB")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod', 'local'], default='prod', help='Manifold')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
