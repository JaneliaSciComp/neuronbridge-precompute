'''
'''

import argparse
import collections
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
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
FOUND = collections.defaultdict(lambda: 0, {})

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
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(AWSS3["client"], ARG.BUCKET)
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
    if ARG.MANIFEST:
        with open(ARG.MANIFEST, 'r', encoding='ascii') as instream:
            for line in instream:
                MANIFEST['/'.join([ARG.BUCKET, line.strip()])] = True
        print(f"Manifest entries:          {len(MANIFEST):,}")        


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
    initialize_s3()
    get_parms()


def compare_manifest(bucket, obj, objtype, row):
    if '/'.join([bucket, obj]) in MANIFEST:
        FOUND[objtype] += 1
    else:
        LOGGER.warning(f"Missing {objtype} {bucket}/{obj} for {row['_id']}")


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


def processing():
    coll = DB['neuronbridge'].publishedURL
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "libraryName": ARG.LIBRARY,
               "tags": ARG.VERSION}
    try:
        cnt = coll.count_documents(payload)
        rows = coll.find(payload)
    except Exception as err:     
        terminate_program(err)
    print(f"Processing {cnt:,} documents")
    checked = collections.defaultdict(lambda: 0, {})
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
            checked[objtype] += 1
    print(f"Checked {sum(checked.values()):,} images")
    for objtype, cnt in checked.items():
        print(f"{objtype + ':':19} {cnt:,}")
    print(f"Found {sum(FOUND.values()):,} objects")
    for objtype, cnt in FOUND.items():
        print(f"{objtype + ':':19} {cnt:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add ORCID information to MongoDB:orcid")
    PARSER.add_argument('--bucket', dest='BUCKET', action='store',
                        default='janelia-flylight-color-depth', help='AWS S3 bucket')
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='Template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='Library')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'devpre', 'prod', 'prodpre'],
                        help='S3 manifold')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        help='NeuronBridge data version')
    PARSER.add_argument('--manifest', dest='MANIFEST', action='store',
                        help='Search manifest instead of AWS S3')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
