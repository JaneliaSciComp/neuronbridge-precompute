''' This program will check neuronMetadata for a NeuronBridge release for
    sample issues, optionally retagging documents. Documents will be tagged
    with the word "unreleased" as well as the ALPS release.
'''
__version__ = '0.0.4'

import argparse
from operator import attrgetter
import sys
from time import strftime
import boto3
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB

#pylint: disable=broad-exception-caught,logging-fstring-interpolation

# AWS
S3 = {}
S3_SECONDS = 60 * 60 * 12
# Database
DB = {}
COLL = {}
# Counters
COUNT = {"images": 0, "found": 0, "published": 1, "updated": 0, "unreleased": 0}
RELEASE = {}
SLIDES = []

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


def initialize_s3():
    ''' Intialize S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        aws = JRC.get_config("aws")
    except Exception as err:
        terminate_program(err)
    LOGGER.info("Opening S3 client and resource")
    if "dev" in ARG.MANIFOLD:
        S3['client'] = boto3.client('s3')
        S3['resource'] = boto3.resource('s3')
    else:
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=aws.role_arn,
                                     RoleSessionName="AssumeRoleSession1",
                                     DurationSeconds=S3_SECONDS)
        credentials = aro['Credentials']
        S3['client'] = boto3.client('s3',
                                    aws_access_key_id=credentials['AccessKeyId'],
                                    aws_secret_access_key=credentials['SecretAccessKey'],
                                    aws_session_token=credentials['SessionToken'])
        S3['resource'] = boto3.resource('s3',
                                        aws_access_key_id=credentials['AccessKeyId'],
                                        aws_secret_access_key=credentials['SecretAccessKey'],
                                        aws_session_token=credentials['SessionToken'])


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    # Database
    for source in ("sage", "neuronbridge"):
        manifold = "prod" if source == "sage" else ARG.MANIFOLD
        rwp = "write" if ARG.WRITE else "read"
        dbo = attrgetter(f"{source}.{manifold}.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            if source == "sage":
                DB[source] = {}
            DB[source] = JRC.connect_database(dbo)
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)
    # S3
    initialize_s3()
    # Parms
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(S3["client"], 'janelia-flylight-color-depth')
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(source='mongo', coll=DB['neuronbridge'].neuronMetadata,
                                     exclude="flyem")
    if not ARG.VERSION:
        ARG.VERSION = NB.get_neuronbridge_version(DB['neuronbridge'].neuronMetadata)


def tag_release(row, release):
    ''' Tag the image with the ALPs release and the word "unreleased"
        Keyword arguments:
          row: neuronMetadata row
          release: ALPS release
        Returns:
          None
    '''
    COUNT['found'] += 1
    listsize = len(row['tags'])
    if release:
        if release not in RELEASE:
            RELEASE[release] = 0
        RELEASE[release] += 1
    if release and (release not in row['tags']):
        row['tags'].append(release)
    if "unreleased" not in row['tags']:
        row['tags'].append("unreleased")
        COUNT["unreleased"] += 1
    if len(row['tags']) == listsize:
        return
    LOGGER.debug(row)
    if ARG.WRITE:
        payload = { "$set": { 'tags': row['tags']} }
        result = COLL['neuronMetadata'].update_one({"_id": row['_id']}, payload)
        if result.modified_count:
            COUNT["updated"] += 1
        else:
            LOGGER.error("Could not update %s in neuronMetadata", row['_id'])



def check_image(row, non_public):
    ''' Check an image to see if it's ready for uploading
        Keyword arguments:
          row: neuronMetadata row
          non_public: hash of non-public slide codes
        Returns:
          None
    '''
    if row['slideCode'] in non_public:
        release = non_public[row['slideCode']]
        if release:
            LOGGER.debug("Sample %s (%s) is in non-public release %s", row['_id'],
                         row['slideCode'], release)
        else:
            LOGGER.warning(f"Sample {row['_id']} ({row['sourceRefId']} " \
                           + f"{row['slideCode']}) is not prestaged")
            SLIDES.append(row['slideCode'])
        tag_release(row, release)
    elif not row['publishedName']:
        LOGGER.error("No publishing name for %s", row['_id'])


def process_slide_codes():
    ''' Generate a report of slide codes with unreleased images
        Keyword arguments:
          None
        Returns:
          None
    '''
    fname = strftime("%Y%m%dT%H%M%S") + "_precheck.tsv"
    print(f"Some slide codes have images that may need to be retagged.\nCheck {fname}")
    sql = "SELECT id,slide_code,workstation_sample_id,data_set,name,alps_release " \
          + "FROM image_data_mv WHERE slide_code=%s ORDER BY 2,3,4,5"
    with open(fname, "w", encoding="ascii") as outf:
        outf.write("ID\tSlide code\tSample\tData set\tALPS release\tImage name\n")
        for scode in SLIDES:
            DB['sage']['cursor'].execute(sql, (scode,))
            rows =  DB['sage']['cursor'].fetchall()
            for row in rows:
                outf.write(f"{row['id']}\t{row['slide_code']}\t{row['workstation_sample_id']}\t" \
                           + f"{row['data_set']}\t{row['alps_release']}\t{row['name']}\n")


def perform_checks():
    ''' Check all images for a given library/version
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Find non-public releases
    COLL['lmRelease'] = DB['neuronbridge'].lmRelease
    results = COLL['lmRelease'].find({"public": False})
    non_public = [row['release'] for row in results]
    # Get count of images with correct library, alignment space, and version from neuronMetadata
    COLL['neuronMetadata'] = DB['neuronbridge'].neuronMetadata
    payload = {"libraryName": ARG.LIBRARY,
               "alignmentSpace": ARG.TEMPLATE,
               "$and": [{"tags": ARG.VERSION},
                        {"tags": {"$nin": ["unreleased"]}}]}
    count = COLL['neuronMetadata'].count_documents(payload)
    if not count:
        terminate_program(f"There are no processed tags for version {ARG.VERSION} in {ARG.LIBRARY}")
    print(f"Images in {ARG.LIBRARY} {ARG.VERSION}: {count:,}")
    # Get images from SAGE with no release or a release in the non-public list
    sql = "SELECT DISTINCT slide_code,alps_release FROM image_data_mv WHERE display=1 AND " \
          + "alignment_space_cdm=%s AND (alps_release IS NULL OR alps_release IN (%s))"
    if ARG.RAW:
        non_public.append('Split-GAL4 Omnibus Broad')
    sql = sql % (f"'{ARG.TEMPLATE}'", '"' + '","'.join(non_public) + '"',)
    LOGGER.info(f"Finding non-public images ({','.join(non_public)}) in SAGE")
    DB['sage']['cursor'].execute(sql)
    rows =  DB['sage']['cursor'].fetchall()
    non_public = {row['slide_code']: row['alps_release']  for row in rows}
    # Get images from publishedURL
    LOGGER.info("Finding images in publishedURL")
    COLL['publishedURL'] = DB['neuronbridge'].publishedURL
    published = {}
    results = COLL['publishedURL'].find({"libraryName": ARG.LIBRARY,
                                         "alignmentSpace": ARG.TEMPLATE}, {"_id": 1})
    for row in results:
        published[row['_id']] = True
    # Get images with correct library, alignment space, and version from neuronMetadata
    project = {"libraryName": 1, "publishedName": 1, "slideCode": 1,
               "tags": 1, "neuronInstance": 1, "neuronType": 1, "sourceRefId": 1}
    results = COLL['neuronMetadata'].find(payload, project)
    # Process neuronMetadata images
    for row in tqdm(results, desc="publishedName", total=count):
        if row['_id'] in published:
            COUNT['published'] += 1
            continue
        COUNT['images'] += 1
        check_image(row, non_public)
    print(f"Images found:        {COUNT['images']:,}")
    print(f"Images published:    {COUNT['published']:,}")
    print(f"Images to retag:     {COUNT['found']:,}")
    if COUNT['found']:
        print(f"Images retagged:     {COUNT['updated']:,}")
        print(f"Unreleased:          {COUNT['unreleased']:,}")
        for key, val in RELEASE.items():
            print(f"{key}: {val:,}")
    if SLIDES:
        process_slide_codes()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload prechecks")
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='Template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        default='', help='NeuronBridge data version')
    PARSER.add_argument('--tag', dest='TAG', action='store',
                        default='', help='MongoDB neuronMetadata tag')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='S3 manifold')
    PARSER.add_argument('--raw', dest='RAW', action='store_true',
                        default=False, help='Do not consider RAW as public')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually write to neuronMetadata')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    perform_checks()
    terminate_program()
