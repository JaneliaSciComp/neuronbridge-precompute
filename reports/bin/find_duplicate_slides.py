''' find_duplicate_slides.py
    Find slide codes with mutiple samples in neuronBridge
'''
__version__ = '1.0.0'

import argparse
import collections
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-not-lazy,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    # Database
    for source in ("neuronbridge", ):
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def processing():
    ''' Find and report on slide codes with multiple samples
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = [{"$match": {"slideCode": {"$exists": 1}, "objective": {"$exists": 1}}},
                          {"$group": {"_id": {"slide": "$slideCode",
                                              "objective": "$objective",
                                              "sample": "$sourceRefId"
                                              },
                                      "count": {"$sum": 1}
                                     }
                          }
              ]
    sample = {}
    sample_name = {}
    try:
        coll = DB['neuronbridge'].neuronMetadata
        rows = coll.aggregate(payload)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        slide = row['_id']['slide']
        if 'objective' not in row['_id']:
            row['_id']['objective'] = ''
            LOGGER.warning(f"Slide {slide} has no objective")
        obj = row['_id']['objective']
        smp = row['_id']['sample']
        sample_name[smp] = True
        if slide not in sample:
            sample[slide] = {}
            COUNT['slides'] += 1
        if obj not in sample[slide]:
            sample[slide][obj] = []
            COUNT['objectives'] += 1
        sample[slide][obj].append(smp)
    for slide, sval in sample.items():
        for obj in sval:
            if len(sval[obj]) > 1:
                print("Slide {slide} Objective {obj} has samples {', '.join(sval[obj])}")
    print(f"Slides:           {COUNT['slides']:,}")
    print(f"Slide/objectives: {COUNT['objectives']:,}")
    print(f"Samples:          {len(sample_name):,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Report on slide codes with multiple samples")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='Manifold [prod]')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
