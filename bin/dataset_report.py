''' Report on EM datasets from Mongo
'''

import argparse
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught

DB = {}

# -----------------------------------------------------------------------------

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
    for dbname in ('jacs',):
        dbo = attrgetter(f"{dbname}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        DB[dbname] = JRC.connect_database(dbo)


def get_datasets():
    """ Show datasets
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['jacs'].emDataSet
    rows = coll.find()
    dataset = {}
    for row in rows:
        dataset[str(row['_id'])] = {'active': 'Yes' if row['active'] else 'No',
                                    'published': 'Yes' if row['published'] else 'No'}
    coll = DB['jacs'].emBody
    payload = [{"$group": {"_id": {"ds": "$dataSetIdentifier", "dsr": "$dataSetRef"},
                           "count": {"$sum": 1}}}]
    rows = coll.aggregate(payload)
    rep = {}
    maxds = 8
    for row in rows:
        dsid = row['_id']['dsr'].split('#')[1]
        dset = row['_id']['ds']
        if ':' in dset:
            dset, ver = dset.split(':')
        else:
            ver = ''
        if len(dset) > maxds:
            maxds = len(dset)
        rep[row['_id']['ds']] = {'ds': dset, 'ver': ver,
                                 'cnt': row['count'], 'act': dataset[dsid]['active'],
                                 'pub': dataset[dsid]['published']}
    print(f"{'Data set':<{maxds}}  Version  {'Active':6}  {'Public':6}  Bodies")
    print(f"{'-'*maxds}  {'-'*7}  {'-'*6}  {'-'*6}  {'-'*7}")
    for dset, data in sorted(rep.items()):
        print(f"{data['ds']:<{maxds}}  {data['ver']:<7}  {data['act']:^6}  " \
              + f"{data['pub']:^6}  {data['cnt']:>7,}")


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
