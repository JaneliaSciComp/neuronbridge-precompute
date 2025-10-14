'''
'''

import argparse
import collections
from copy import deepcopy
import json
from operator import attrgetter
import sys
import boto3
from botocore.exceptions import ClientError
import inquirer
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB

__version__ = '0.0.1'
# Configuration
AWSS3 = {"client": None, "resource": None}
MANIFEST = {}
# Database
DB = {}
# Known NeuronBridge slide codes
SLIDE_CODES = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
CHECKED = collections.defaultdict(lambda: 0, {})
FOUND = collections.defaultdict(lambda: 0, {})
# Output
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


def initialize_s3():
    """ Initialize S3 client and resource
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.MANIFOLD == 'prod':
        try:
            aws = JRC.get_config("aws")
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=aws.role_arn,
                                     RoleSessionName="AssumeRoleSession1")
        credentials = aro['Credentials']
        AWSS3["client"] = boto3.client('s3',
                                       aws_access_key_id=credentials['AccessKeyId'],
                                       aws_secret_access_key=credentials['SecretAccessKey'],
                                       aws_session_token=credentials['SessionToken'])
        AWSS3["resource"] = boto3.resource('s3',
                                           aws_access_key_id=credentials['AccessKeyId'],
                                           aws_secret_access_key=credentials['SecretAccessKey'],
                                           aws_session_token=credentials['SessionToken'])
    else:
        ARG.BUCKET = '-'.join([ARG.BUCKET, ARG.MANIFOLD])
        AWSS3["client"] = boto3.client('s3')
        AWSS3["resource"] = boto3.resource('s3')


def get_parms():
    """ Query the user for the CDM library and manifold
        Keyword arguments:
            None
        Returns:
            None
    """
    if ARG.MANIFEST:
        with open(ARG.MANIFEST, 'r', encoding='ascii') as instream:
            for line in instream:
                MANIFEST['/'.join([ARG.BUCKET, line.strip()])] = True
        print(f"Manifest entries:          {len(MANIFEST):,}")
    if ARG.COLLECTION == 'publishedLMImage':
        return
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(AWSS3["client"], 'janelia-flylight-color-depth')
        if not ARG.TEMPLATE:
            terminate_program("No alignment template selected")
    coll = DB['neuronbridge'].publishedURL
    if not ARG.LIBRARY:
        bucket = 'janelia-flylight-color-depth'
        if ARG.MANIFOLD != 'prod':
            bucket += f"-{ARG.MANIFOLD}"
        ARG.LIBRARY = NB.get_library(source='mongo', coll=coll, template=ARG.TEMPLATE)
        if not ARG.LIBRARY:
            terminate_program("No library selected")
    if not ARG.VERSION:
        payload = [{"$match": {"alignmentSpace": ARG.TEMPLATE,
                               "libraryName": ARG.LIBRARY}},
                   {"$unwind": "$tags"},
                   {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
                  ]
        rows = coll.aggregate(payload)
        versions = []
        for row in rows:
            versions.append(row['_id'])
        quest = [inquirer.List('version',
                 message='Select version',
                 choices=versions)]
        ans = inquirer.prompt(quest)
        if not ans or not ans['version']:
            terminate_program("No version selected")
        ARG.VERSION = ans['version']
    if not ARG.VERSION:
        terminate_program("No NeuronBridge data version was found")
    print(f"Template:                  {ARG.TEMPLATE}")
    print(f"Library:                   {ARG.LIBRARY}")
    print(f"NeuronBridge data version: {ARG.VERSION}")        


def initialize_program():
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['neuronbridge']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['neuronbridge'].publishedURL.distinct('slideCode')
    except Exception as err:
        terminate_program(err)
    for row in rows:
        SLIDE_CODES[row] = True
    LOGGER.info(f"Known NeuronBridge slide codes: {len(SLIDE_CODES):,}")
    initialize_s3()
    get_parms()


def compare_manifest(bucket, obj, objtype, row):
    LOGGER.debug(f"Comparing {objtype} {bucket}/{obj} for {row['_id']}")
    if '/'.join([bucket, obj]) in MANIFEST:
        FOUND[objtype] += 1
    else:
        LOGGER.warning(f"Missing {objtype} {bucket}/{obj} for {row['_id']}")
        MISSING.append(f"{row['slideCode']}\t{row['releaseName']}\t{bucket}/{obj}\n")
        if ARG.DELETE and ARG.COLLECTION == 'publishedLMImage':
            coll = DB['neuronbridge'].publishedLMImage
            payload = deepcopy(row['files'])
            del payload[objtype]
            if not payload:
                coll.delete_one({"_id": row['_id']})
            else:
                coll.update_one({"_id": row['_id']}, {"$set": {"files": payload}})


def compare_s3(bucket, obj, objtype, row):
    try:
        AWSS3['resource'].Object(bucket, obj).load()
    except ClientError as err:
        if err.response['Error']['Code'] == "404":
            LOGGER.warning(f"Missing {objtype} {bucket}/{obj} for {row['_id']}")
            return
        else:
            terminate_program(err)
    except Exception as err:
        terminate_program(err)
    FOUND[objtype] += 1


def processing_publishedurl():
    """ Process publishedURL
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['neuronbridge'].publishedURL
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "libraryName": ARG.LIBRARY,
               "tags": ARG.VERSION}
    try:
        cnt = coll.count_documents(payload)
        rows = coll.find(payload)
    except Exception as err:     
        terminate_program(err)
    print(f"Processing {cnt:,} documents from {ARG.COLLECTION}")
    for row in tqdm(rows, total=cnt):
        for objtype, objkey in row['uploaded'].items():
            objkey = objkey.replace("https://s3.amazonaws.com/", "")
            bucket, obj = objkey.split('/', 1)
            if ARG.MANIFEST:
                if bucket != ARG.BUCKET:
                    continue
                compare_manifest(bucket, obj, objtype, row)
            else:
                compare_s3(bucket, obj, objtype, row)
            CHECKED[objtype] += 1


def processing_publishedlmimage():
    """ Process publishedLMImage
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['neuronbridge'].publishedLMImage
    payload = {}
    if ARG.RELEASE:
        payload['releaseName'] = ARG.RELEASE
    if ARG.SLIDE:
        payload['slideCode'] = ARG.SLIDE
    try:
        cnt = coll.count_documents(payload)
        rows = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    print(f"Processing {cnt:,} documents from {ARG.COLLECTION}")
    for row in tqdm(rows, total=cnt):
        if row['slideCode'] not in SLIDE_CODES:
            continue
        for objtype, objkey in row['files'].items():
            objkey = objkey.replace("https://s3.amazonaws.com/", "")
            bucket, obj = objkey.split('/', 1)
            obj = obj.replace("+", "%20")
            if ARG.MANIFEST:
                if bucket != ARG.BUCKET:
                    continue
                compare_manifest(bucket, obj, objtype, row)
            else:
                compare_s3(bucket, obj, objtype, row)
            CHECKED[objtype] += 1

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add ORCID information to MongoDB:orcid")
    PARSER.add_argument('--collection', dest='COLLECTION', action='store',
                        default='publishedURL', choices=['publishedURL', 'publishedLMImage'],
                        help='MongoDB collection')
    PARSER.add_argument('--bucket', dest='BUCKET', action='store',
                        default='janelia-flylight-color-depth', help='AWS S3 bucket')
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='Template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='Library')
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        help='Release name')
    PARSER.add_argument('--slide', dest='SLIDE', action='store',
                        help='Slide code')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'devpre', 'prod', 'prodpre'],
                        help='S3 manifold')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        help='NeuronBridge data version')
    PARSER.add_argument('--manifest', dest='MANIFEST', action='store',
                        help='Search manifest instead of AWS S3')
    PARSER.add_argument('--delete', dest='DELETE', action='store_true',
                        default=False, help='Delete missing file pointers from publishedLMImage')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing_publishedurl() if ARG.COLLECTION == 'publishedURL' else processing_publishedlmimage()
    print(f"Checked {sum(CHECKED.values()):,} images")
    for objtype, cnt in CHECKED.items():
        print(f"{objtype + ':':22} {cnt:,}")
    print(f"Found {sum(FOUND.values()):,} objects")
    for objtype, cnt in FOUND.items():
        print(f"{objtype + ':':22} {cnt:,} ({cnt/CHECKED[objtype]*100:.2f}%)")
    if MISSING:
        with open(f"{ARG.COLLECTION}_missing.txt", 'w', encoding='ascii') as outfile:
            outfile.writelines(MISSING)
    terminate_program()
