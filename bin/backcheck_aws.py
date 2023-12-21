''' backcheck_aws.py
    This program will backcheck AWS S3 contents to neuronbridge.neuronMetadata
'''

import argparse
from operator import attrgetter
import sys
from types import SimpleNamespace
import boto3
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_lib as NB

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# AWS
S3 = {}
S3_SECONDS = 60 * 60 * 12
# Database
DB = {}
# Configuration
MANIFOLDS = ['dev', 'prod', 'devpre', 'prodpre']

def terminate_program(msg=None):
    """ Log an optional error to output and exit
        Keyword arguments:
          err: error message
        Returns:
          None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_s3():
    """ Initialize S3 connections
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info("Opening S3 client and resource")
    try:
        aws = JRC.get_config("aws")
    except Exception as err:
        terminate_program(err)
    if ARG.MANIFOLD != 'prod':
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
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    dbo = attrgetter(f"neuronbridge.{ARG.MONGO}.read")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
    DB['NB'] = JRC.connect_database(dbo)
    initialize_s3()


def get_published_names():
    """ Get published names from neuronbridgee.neuronMetadata
        Keyword arguments:
          None
        Returns:
          pname: dict of publishing names
    """
    try:
        libraries = simplenamespace_to_dict(JRC.get_config("cdm_library"))
    except Exception as err:
        terminate_program(err)
    complib = ARG.LIBRARY.replace("_", " ")
    libname = ''
    for lib, mdata in libraries.items():
        if mdata['name'] == ARG.LIBRARY or mdata['name'] == complib:
            libname = lib
            break
    if not libname:
        terminate_program(f"Could not find library for {ARG.LIBRARY}")
    coll = DB['NB'].neuronMetadata
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "libraryName": libname}
    pname = {}
    LOGGER.info(f"Searching neuronMetadata for {ARG.TEMPLATE}/{libname}")
    try:
        results = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in results:
        if row['publishedName']:
            pname[row['publishedName']] = True
    print(f"Found {len(pname):,} publishing names in neuronMetadata")
    return pname


def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return f"{num:.1f}{unit}{suffix}"
        num /= 1024.0
    return "{num:.1f}P{suffix}"


def simplenamespace_to_dict(nspace):
    """ Convert a simplenamespace to a dict recursively
        Keyword arguments:
          nspace: simplenamespace to convert
        Returns:
          The converted dict
    """
    result = {}
    for key, value in nspace.__dict__.items():
        if isinstance(value, SimpleNamespace):
            result[key] = simplenamespace_to_dict(value)
        else:
            result[key] = value
    return result


def run_backcheck():
    """ Check publishing names in S3 vs. neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    """
    bucket = 'janelia-flylight-color-depth'
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(S3['client'], bucket)
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library_from_aws(S3['client'], bucket, ARG.TEMPLATE)
    mpname = get_published_names()
    prefix = '/'.join([ARG.TEMPLATE, ARG.LIBRARY]) + '/'
    objs = NB.get_all_s3_objects(S3['client'], Bucket=bucket, Prefix=prefix)
    total_objs = total_size = 0
    files = []
    for obj in tqdm(objs, desc='Finding files on S3'):
        total_objs += 1
        if '/searchable_neurons/' not in obj['Key'] or not obj['Key'].endswith('.tif'):
            continue
        total_size += obj['Size']
        files.append(obj['Key'])
    LOGGER.info(f"Checked {total_objs:,} objects on S3")
    print(f"Found {len(files):,} objects ({humansize(total_size)})")
    apname = {}
    for file in files:
        apname[file.split('/')[-1].split('-')[0]] = True
    print(f"Found {len(apname):,} distinct publishing names in S3")
    good = True
    for cpn in tqdm(apname, desc='AWS S3'):
        if cpn not in mpname:
            good = False
            LOGGER.warning(f"{cpn} is in S3 but not in neuronMetadata")
    for cpn in tqdm(mpname, desc='neuronMetadata'):
        if cpn not in apname:
            good = False
            LOGGER.warning(f"{cpn} is in neuronMetadata but not in S3")
    if good:
        print("All publishing names matched")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backcheck AWS S3 to neuronMetadata")
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='alignment template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=MANIFOLDS, help='S3 manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    run_backcheck()
    terminate_program()
