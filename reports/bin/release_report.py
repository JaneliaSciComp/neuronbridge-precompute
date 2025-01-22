''' release_report.py
    Compare the number of samples in SAGE to counts in MongoDB by release.
    A file (releases_missing_samples.txt) is created with the list of missing samples.
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
READ = {"releases": "SELECT DISTINCT alps_release,workstation_sample_id FROM "
                    "image_data_mv WHERE alps_release IS NOT NULL",
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
    # Get samples from SAGE
    DB['sage']['cursor'].execute(READ['releases'])
    rows = DB['sage']['cursor'].fetchall()
    releases = {}
    samples = {"sage": {}, "neuronMetadata": {}, "publishedURL": {}}
    for row in rows:
        if row['alps_release'] not in releases:
            releases[row['alps_release']] = 0
        releases[row['alps_release']] += 1
        if row['alps_release'] not in samples['sage']:
            samples['sage'][row['alps_release']] = {}
        if row['workstation_sample_id'] not in samples['sage'][row['alps_release']]:
            samples['sage'][row['alps_release']][row['workstation_sample_id']] = True
    LOGGER.info(f"Found {len(releases)} releases in SAGE")
    # Get samples from MongoDB
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
            if row['_id']['release'] not in samples[coll]:
                samples[coll][row['_id']['release']] = {}
            smp = row['_id']['sample'].replace("Sample#", "")
            if smp not in samples[coll][row['_id']['release']]:
                samples[coll][row['_id']['release']][smp] = True
        LOGGER.info(f"Found {len(mongo[coll])} datasets in {coll}")
    colsize = {'nmd': len('neuronMetadata'), 'purl': len('publishedURL'), 'rel': 0, 'sage': 11}
    for rel, val in releases.items():
        if len(rel) > colsize['rel']:
            colsize['rel'] = len(rel)
        if len(str(val)) > colsize['sage']:
            colsize['sage'] = len(f"{val:,}")
    if ARG.VERBOSE:
        for rel, val in samples.items():
            print(f"{rel}: {len(val)}")
    print(f"{'Release':<{colsize['rel']}}  {'Samples':>{colsize['sage']}}  " \
          + f"{'neuronMetadata':>{colsize['nmd']}}  {'publishedURL':>{colsize['purl']}}")
    # Compare sample counts
    missing = {}
    for rel, val in releases.items():
        sage = f"{val:,}"
        nmd = f"{mongo['neuronMetadata'][rel]:,}" if rel in mongo['neuronMetadata'] else '0'
        if nmd != sage and rel in samples['neuronMetadata']:
            if rel not in missing:
                missing[rel] = {}
            for smp in samples['sage'][rel]:
                if smp not in samples['neuronMetadata'][rel]:
                    missing[rel][smp] = True
        purl = f"{mongo['publishedURL'][rel]:,}" if rel in mongo['publishedURL'] else '0'
        if purl != sage and rel in samples['publishedURL']:
            if rel not in missing:
                missing[rel] = {}
            for smp in samples['sage'][rel]:
                if smp not in samples['publishedURL'][rel]:
                    missing[rel][smp] = True
        if ARG.SKIP and sage == nmd and nmd == purl:
            continue
        nmd = color(nmd, sage, colsize['nmd'])
        purl = color(purl, sage, colsize['purl'])
        print(f"{rel:<{colsize['rel']}}  {sage:>{colsize['sage']}}  {nmd}  {purl}")
    if missing:
        LOGGER.info(f"Found {len(missing)} releases with missing samples")
        with open("releases_missing_samples.txt", "w", encoding="ascii") as file:
            for rel, smps in missing.items():
                for smp in smps:
                    file.write(f"{rel}\t{smp}\n")

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
