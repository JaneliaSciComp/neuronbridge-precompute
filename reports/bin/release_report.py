''' release_report.py
    Compare the number of samples in SAGE to counts in MongoDB by release.
    A file (releases_missing_samples.txt) is created with the list of missing samples.
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
READ = {"releases": "SELECT DISTINCT alps_release,slide_code,workstation_sample_id FROM "
                    "image_data_mv WHERE alps_release IS NOT NULL",
        "single": "SELECT DISTINCT alps_release,slide_code,workstation_sample_id FROM "
                  "image_data_mv WHERE alps_release=%s",
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


def process_sage_samples(releases, samples, slides):
    ''' Process the samples from SAGE
        Keyword arguments:
          releases: dictionary of releases
          samples: dictionary of samples
          slides: dictionary of slide codes
        Returns:
          None
    '''
    LOGGER.info("Getting samples from SAGE")
    if ARG.RELEASE:
        DB['sage']['cursor'].execute(READ['single'], (ARG.RELEASE,))
    else:
        DB['sage']['cursor'].execute(READ['releases'])
    rows = DB['sage']['cursor'].fetchall()
    scnt = 0
    for row in rows:
        if row['alps_release'] not in releases:
            releases[row['alps_release']] = 0
        releases[row['alps_release']] += 1
        if row['alps_release'] not in samples['sage']:
            samples['sage'][row['alps_release']] = {}
        if row['workstation_sample_id'] not in samples['sage'][row['alps_release']]:
            samples['sage'][row['alps_release']][row['workstation_sample_id']] = True
        if row['workstation_sample_id'] not in slides:
            slides[row['workstation_sample_id']] = row['slide_code']
        scnt += 1
    LOGGER.info(f"Found {len(releases)} release{'' if len(releases) ==1 else 's'} " \
                + f"with {scnt:,} samples in SAGE")


def get_mongo_payload(coll):
    ''' Get the payload for MongoDB query
        Keyword arguments:
            coll: collection name
        Returns:
            payload
    '''
    field = "datasetLabels" if coll == "neuronMetadata" else "alpsRelease"
    field2 = "sourceRefId" if coll == "neuronMetadata" else "sampleRef"
    # {"$group": {"_id": {"release": f"${field}", "sample": f"${field2}"}}},
    # {"$group": {"_id": {"release": f"${field}", "sample": f"${field2}", "libraryName": "$libraryName"}}},
    return [{"$match": {field: {"$exists": True},
             "libraryName": {"$regex": "^flylight_"}}},
            {"$unwind": f"${field}"},
            {"$group": {"_id": {"release": f"${field}", "sample": f"${field2}", "libraryName": "$libraryName"}}},
           ]


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


def compare_sample_counts(releases, samples, mongo, colsize):
    ''' Compare the sample counts
        Keyword arguments:
          releases: dictionary of releases
          samples: dictionary of samples
          mongo: dictionary of MongoDB data
          colsize: dictionary of column sizes
        Returns:
          Dictionary of missing samples by release
    '''
    missing = {}
    for rel, val in sorted(releases.items()):
        sage = f"{val:,}"
        purl = f"{mongo['publishedURL'][rel]:,}" if rel in mongo['publishedURL'] else '0'
        if purl != sage and rel in samples['publishedURL']:
            if rel not in missing:
                missing[rel] = {}
            for smp in samples['sage'][rel]:
                if smp not in samples['publishedURL'][rel]:
                    missing[rel][smp] = "publishedURL"
        nmd = f"{mongo['neuronMetadata'][rel]:,}" if rel in mongo['neuronMetadata'] else '0'
        if nmd != sage and rel in samples['neuronMetadata']:
            if rel not in missing:
                missing[rel] = {}
            for smp in samples['sage'][rel]:
                if smp not in samples['neuronMetadata'][rel]:
                    missing[rel][smp] = "neuronMetadata"
        if ARG.SKIP and sage == nmd and nmd == purl:
            continue
        nmd = color(nmd, sage, colsize['nmd'])
        purl = color(purl, sage, colsize['purl'])
        print(f"{rel:<{colsize['rel']}}  {sage:>{colsize['sage']}}  {nmd}  {purl}")
    return missing


def produce_output_file(missing, slides):
    ''' Produce the output file
        Keyword arguments:
          missing: dictionary of missing samples
          slides: dictionary of slide codes
        Returns:
          None
    '''
    if missing:
        LOGGER.info(f"Found {len(missing)} release{'' if len(missing) == 1 else 's'} " \
                    + "with missing samples")
        with open("releases_missing_samples.txt", "w", encoding="ascii") as file:
            file.write("Release\tSample\tSlide code\tMissing from\n")
            for rel, smps in missing.items():
                for smp, where in smps.items():
                    if smp not in slides:
                        terminate_program(f"Slide code not found for sample {smp}")
                    slide = slides[smp]
                    file.write(f"{rel}\t{smp}\t{slide}\t{where}\n")


def process():
    """ Process the data
        Keyword arguments:
          None
        Returns:
          None
    """
    # Get samples from SAGE
    releases = {}
    slides = {}
    samples = {"sage": {}, "neuronMetadata": {}, "publishedURL": {}}
    process_sage_samples(releases, samples, slides)
    # Get samples from MongoDB
    mongo = {}
    for coll in ("neuronMetadata", "publishedURL"):
        LOGGER.info(f"Getting samples from {coll}")
        mongo[coll] = {}
        payload = get_mongo_payload(coll)
        rows = DB['neuronbridge'][coll].aggregate(payload)
        scnt = 0
        for row in rows:
            if row['_id']['libraryName'] in ('flylight_gen1_mcfo_published',
                                             'flylight_annotator_gen1_mcfo_published'):
                continue
            if row['_id']['release'] not in mongo[coll]:
                mongo[coll][row['_id']['release']] = 0
            mongo[coll][row['_id']['release']] += 1
            if row['_id']['release'] not in samples[coll]:
                samples[coll][row['_id']['release']] = {}
            smp = row['_id']['sample'].replace("Sample#", "")
            if smp not in samples[coll][row['_id']['release']]:
                samples[coll][row['_id']['release']][smp] = True
            scnt += 1
        LOGGER.info(f"Found {len(mongo[coll])} releases with {scnt:,} samples in {coll}")
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
    missing = compare_sample_counts(releases, samples, mongo, colsize)
    # Produce output file
    produce_output_file(missing, slides)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Report on LM releases in MongoDB")
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        default='', help='ALPS release')
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
