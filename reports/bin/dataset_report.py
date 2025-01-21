''' Report on EM datasets from Mongo
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught

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
    for dbname in ('jacs', 'neuronbridge'):
        man = 'prod' if dbname == 'neuronbridge' else ARG.MANIFOLD
        dbo = attrgetter(f"{dbname}.{man}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        DB[dbname] = JRC.connect_database(dbo)


def color(txt, warn=False):
    """ Color Yes/No text
        Keyword arguments:
          txt: text to color
        Returns:
          Colored text
    """
    if 'Yes' in txt:
        return Fore.GREEN + txt + Style.RESET_ALL
    if warn:
        return Fore.YELLOW + txt + Style.RESET_ALL
    return Fore.RED + txt + Style.RESET_ALL


def get_datasets():
    """ Show datasets
        Keyword arguments:
          None
        Returns:
          None
    """
    # Get data sets from NeuronBridge publishedURL
    LOGGER.info("Getting libraries from NeuronBridge")
    coll = DB['neuronbridge'].publishedURL
    payload = [{"$group": {"_id": {"lib": "$libraryName"},
                           "count": {"$sum": 1}}}]
    rows = coll.aggregate(payload)
    nblib = {}
    for row in rows:
        nblib[row['_id']['lib'].replace('flyem_', '')] = row['count']
    # Get data sets from jacs
    LOGGER.info("Getting datasets from JACS")
    coll = DB['jacs'].emDataSet
    rows = coll.find()
    dataset = {}
    for row in rows:
        dataset[str(row['_id'])] = {'active': 'Yes' if row['active'] else 'No',
                                    'published': 'Yes' if row['published'] else 'No'}
    # Get body count from jacs
    LOGGER.info("Getting bodies from JACS")
    coll = DB['jacs'].emBody
    payload = [{"$group": {"_id": {"ds": "$dataSetIdentifier", "dsr": "$dataSetRef"},
                           "count": {"$sum": 1}}}]
    rows = coll.aggregate(payload)
    rep = {}
    maxds = 8
    for row in rows:
        dsid = row['_id']['dsr'].split('#')[1]
        dset = row['_id']['ds']
        if dsid not in dataset:
            LOGGER.warning(f"Dataset ID {dsid} not found")
            continue
        if ':' in dset:
            dset, ver = dset.split(':')
            dslib = '_'.join([dset, ver.replace('v', '').replace('.', '_')])
        else:
            ver = ''
            dslib = dset
        if len(dset) > maxds:
            maxds = len(dset)
        if dslib in nblib:
            on_nb = color(f"{'Yes':^12}")
        elif dataset[dsid]['active'] == 'Yes' and dataset[dsid]['published'] == 'Yes' \
             and dset not in ('fib19', 'hemibrain'):
            on_nb = color(f"{'No':^12}")
        else:
            on_nb = 'No'
        rep[row['_id']['ds']] = {'ds': dset, 'ver': ver,
                                 'nb': on_nb,
                                 'cnt': row['count'],
                                 'act': color(f"{dataset[dsid]['active']:^6}", True),
                                 'pub': color(f"{dataset[dsid]['published']:^6}", True)}
    # Display
    print(f"{'Data set':<{maxds}}  Version  {'Active':6}  {'Public':6}  {'Bodies':7}  NeuronBridge")
    print(f"{'-'*maxds}  {'-'*7}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*12}")
    for dset, data in sorted(rep.items()):
        print(f"{data['ds']:<{maxds}}  {data['ver']:<7}  {data['act']}  " \
              + f"{data['pub']}  {data['cnt']:>7,}  {data['nb']:^12}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Report on EM datasets from Mongo")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod', 'local'], default='prod', help='Manifold')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    get_datasets()
    sys.exit(0)
