''' find_changed_releases.py
    Find samples that havde different releases in a publishing database (or SAGE) versus
    NeuronBridge. The publishing database or SAGE will have the most recent release.
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
READ = {"MAIN": "SELECT workstation_sample_id,slide_code,line,alps_release FROM "
                + "image_data_mv WHERE alps_release=%s",
        "ALL": "SELECT workstation_sample_id,slide_code,line,alps_release FROM image_data_mv WHERE "
                + "alps_release IS NOT NULL",
    }
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Global variables
DIFF = []
MISSING = []

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
    for source in ("sage", ARG.DATABASE, "neuronbridge"):
        manifold = ARG.MANIFOLD if source in ['gen1mcfo', 'mbew', 'raw'] else 'prod'
        dbo = attrgetter(f"{source}.{manifold}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def processing():
    ''' Find differences in releases between SAGE/publishing and NeuronBridge
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.RELEASE:
        LOGGER.info(f"Finding {ARG.RELEASE} samples in {ARG.DATABASE}")
        DB['sage']['cursor'].execute(READ['MAIN'], (ARG.RELEASE,))
    else:
        LOGGER.info(f"Finding samples in {ARG.DATABASE}")
        DB['sage']['cursor'].execute(READ['ALL'])
    rows = DB['sage']['cursor'].fetchall()
    sample = {}
    for row in rows:
        sample[row['workstation_sample_id']] = row['alps_release']
    LOGGER.info(f"Found {len(sample):,} samples in {ARG.DATABASE}")
    LOGGER.info("Finding samples in neuronMetadata")
    payload = [{"$match": {"datasetLabels": {"$exists": True},
                           "libraryName": {"$regex": "flylight"}}},
                {"$group": {"_id": {"sample": "$sourceRefId", "release": "$datasetLabels"},
                                    "count": {"$sum": 1}}}]
    nbsample = {}
    try:
        rows = DB['neuronbridge']['neuronMetadata'].aggregate(payload)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        smp = row['_id']['sample'].replace('Sample#', '')
        if row['_id']['sample'] not in nbsample:
            nbsample[smp] = []
        for rel in row['_id']['release']:
            if rel not in nbsample[smp]:
                nbsample[smp].append(rel)
    LOGGER.info(f"Found {len(nbsample):,} samples in neuronMetadata")
    for smp, rel in sample.items():
        if smp not in nbsample:
            MISSING.append(f"{smp}")
            COUNT['missing'] += 1
            continue
        if rel not in nbsample[smp]:
            rls = ', '.join(nbsample[smp]) if len(nbsample[smp]) > 1 else nbsample[smp][0]
            DIFF.append(f"{smp}\t{rel}\t{rls}")
            COUNT['diff'] += 1
    if DIFF:
        DIFF.insert(0, "Sample\tSAGE release\tNeuronBridge release")
        with open('releases_to_change.txt', 'w', encoding='ascii') as out:
            out.write('\n'.join(DIFF))
    if MISSING:
        with open('missing_samples.txt', 'w', encoding='ascii') as out:
            out.write('\n'.join(MISSING))
    print(f"Samples in {ARG.DATABASE+':':20}      {len(sample):,}")
    print(f"Samples in neuronMetadata:           {len(nbsample):,}")
    print(f"Samples missing from neuronMetadata: {COUNT['missing']:,}")
    print(f"Samples with different releases:     {COUNT['diff']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Report on sample status")
    LOOKUP = PARSER.add_mutually_exclusive_group(required=True)
    LOOKUP.add_argument('--release', dest='RELEASE', action='store',
                        default='', help='ALPS release')
    LOOKUP.add_argument('--database', dest='DATABASE', action='store',
                        default='sage', help='Database [sage, gen1mcfo, mbew, raw]')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['staging', 'prod'], help='Manifold [prod]')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
