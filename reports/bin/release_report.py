''' release_report.py
    Compare the number of samples in SAGE to counts in MongoDB by release.
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
READ = {"releases": "SELECT alps_release,COUNT(DISTINCT workstation_sample_id) AS cnt FROM "
                    "image_data_mv WHERE alps_release IS NOT NULL GROUP BY 1",
}

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
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        DB[dbname] = JRC.connect_database(dbo)


def color(text, prev, colsize):
    """ Color Yes/No text
        Keyword arguments:
          None
        Returns:
          Colored text
    """
    if text == "0":
        return Fore.RED + f"{text:>{colsize}}" + Style.RESET_ALL
    return Fore.GREEN + f"{text:>{colsize}}" + Style.RESET_ALL if text == prev \
           else Fore.YELLOW + f"{text:>{colsize}}" + Style.RESET_ALL


def process():
    """ Process the data
        Keyword arguments:
          None
        Returns:
          None
    """
    DB['sage']['cursor'].execute(READ['releases'])
    releases = DB['sage']['cursor'].fetchall()
    LOGGER.info(f"Found {len(releases)} releases in SAGE")

    mongo = {}
    for coll in ("neuronMetadata", "publishedURL"):
        mongo[coll] = {}
        field = "datasetLabels" if coll == "neuronMetadata" else "alpsRelease"
        field2 = "sourceRefId" if coll == "neuronMetadata" else "sampleRef"
        payload = [{"$match": {field: {"$exists": True},
                    "libraryName": {"$regex": "^flylight_"}}},
                   {"$unwind": f"${field}"},
                   {"$group": {"_id": {"release": f"${field}", "sample": f"${field2}"}}},
                  ]
        rows = DB['neuronbridge'][coll].aggregate(payload)
        for row in rows:
            if row['_id']['release'] not in mongo[coll]:
                mongo[coll][row['_id']['release']] = 0
            mongo[coll][row['_id']['release']] += 1
        LOGGER.info(f"Found {len(mongo[coll])} datasets in {coll}")
    colsize = {'nmd': len('neuronMetadata'), 'purl': len('publishedURL'), 'rel': 0, 'sage': 11}
    for rel in releases:
        if len(rel['alps_release']) > colsize['rel']:
            colsize['rel'] = len(rel['alps_release'])
        if len(str(rel['cnt'])) > colsize['sage']:
            colsize['sage'] = len(f"{rel['cnt']:,}")
    print(f"{'Release':<{colsize['rel']}}  {'Slide codes':>{colsize['sage']}}  " \
          + f"{'neuronMetadata':>{colsize['nmd']}}  {'publishedURL':>{colsize['purl']}}")
    for row in releases:
        rel = row['alps_release']
        sage = f"{row['cnt']:,}"
        nmd = f"{mongo['neuronMetadata'][rel]:,}" if rel in mongo['neuronMetadata'] else '0'
        purl = f"{mongo['publishedURL'][rel]:,}" if rel in mongo['publishedURL'] else '0'
        if ARG.SKIP and sage == nmd and nmd == purl:
            continue
        nmd = color(nmd, sage, colsize['nmd'])
        purl = color(purl, sage, colsize['purl'])
        print(f"{rel:<{colsize['rel']}}  {sage:>{colsize['sage']}}  {nmd}  {purl}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Report on LM releases in MongoDB")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod', 'local'], default='prod', help='Manifold')
    PARSER.add_argument('--skip', dest='SKIP', action='store_true',
                        default=False, help='Skip display of releases with matching counts')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process()
    terminate_program()
