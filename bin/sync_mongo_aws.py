''' backcheck_aws.py
    This program will backcheck AWS S3 contents to neuronbridge.neuronMetadata
'''

import argparse
from operator import attrgetter
import sys
from types import SimpleNamespace
import boto3
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# AWS
S3 = {}
S3_SECONDS = 60 * 60 * 12
# Database
DB = {}
READ = {"RELEASES": "SELECT publishing_name,slide_code,GROUP_CONCAT(DISTINCT alps_release) "
                    + "AS rels FROM image_data_mv WHERE alps_release IS NOT NULL GROUP BY 1,2",
       }
RELEASE = {}
SLIDE = {}
# Configuration
MANIFOLDS = ['dev', 'prod', 'devpre', 'prodpre']

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
    for dbname in ('sage', 'neuronbridge'):
        mfd = 'prod' if dbname == 'sage' else ARG.MONGO
        dbo = attrgetter(f"{dbname}.{mfd}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, mfd, dbo.host, dbo.user)
        if dbname == 'sage':
            DB[dbname] = JRC.connect_database(dbo)
        else:
            DB['NB'] = JRC.connect_database(dbo)
    initialize_s3()
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(S3['client'], ARG.BUCKET)
    if not ARG.TEMPLATE:
        terminate_program("No template was selected")
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(source='aws', client=S3['client'], bucket=ARG.BUCKET,
                                     template=ARG.TEMPLATE)
    if not ARG.LIBRARY:
        terminate_program("No library was selected")


def populate_releases():
    """ Populate the RELEASE dict with releases for publishing names and slide codes
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info("Fetching releases for publishing names and slide codes")
    try:
        DB['sage']['cursor'].execute(READ['RELEASES'])
        rows = DB['sage']['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    for row in rows:
        RELEASE[row['publishing_name']] = row['rels']
        RELEASE[row['slide_code']] = row['rels']
        SLIDE[row['slide_code']] = row['publishing_name']


def get_releases(key):
    """ Get ALPS releases for a given publishing name/slide code
        Keyword arguments:
          key: publishing name/slide code
        Returns:
          String containing releases
    """
    return f" ({RELEASE[key]})" if key in RELEASE else ''


def get_mongo_data():
    """ Get published names from neuronbridge.neuronMetadata
        Keyword arguments:
          None
        Returns:
          pname: dict of publishing names
          scode: dict of slide codes
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
    coll = DB['NB'][ARG.SOURCE]
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "libraryName": libname}
    pname = {}
    scode = {}
    LOGGER.info(f"Searching {ARG.SOURCE} for {ARG.TEMPLATE}/{libname}")
    try:
        results = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in results:
        if row['publishedName']:
            if ARG.SOURCE == 'publishedURL':
                row['publishedName'] = row['publishedName'].split(':')[-1]
            pname[row['publishedName']] = True
            if 'slideCode' in row:
                scode[row['slideCode']] = row['publishedName']
                if row['slideCode'] not in SLIDE:
                    SLIDE[row['slideCode']] = row['publishedName']
                elif SLIDE[row['slideCode']] != row['publishedName']:
                    msg = f"Mismatched {ARG.SOURCE} publishing name {row['publishedName']} " \
                          + f"{SLIDE[row['slideCode']]} for {row['slideCode']}"
                    LOGGER.warning(msg)
    print(f"Found {len(pname):,} publishing names and {len(scode):,} slide codes in {ARG.SOURCE}")
    return pname, scode


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


def get_aws_data():
    """ Get published names and slide codes from AWS
        Keyword arguments:
          None
        Returns:
          pname: dict of publishing names
          scode: dict of slide codes
    """
    total_objs = total_size = 0
    files = []
    if ARG.MANIFEST:
        LOGGER.info(f"Searching manifest for {ARG.TEMPLATE}/{ARG.LIBRARY}")
        prefix = '/'.join([ARG.TEMPLATE, ARG.LIBRARY]) + '/searchable_neurons/'
        with open(ARG.MANIFEST, 'r', encoding='ascii') as instream:
            rows = instream.read().splitlines()
            for row in rows:
                total_objs += 1
                if not row.startswith(prefix) or not row.endswith('.tif'):
                    continue
                files.append(row.split('/')[-1])
    else:
        LOGGER.info(f"Searching AWS for {ARG.TEMPLATE}/{ARG.LIBRARY}")
        prefix = '/'.join([ARG.TEMPLATE, ARG.LIBRARY]) + '/'
        objs = NB.get_all_s3_objects(S3['client'], Bucket=ARG.BUCKET, Prefix=prefix)
        for obj in tqdm(objs, desc='Finding files on S3'):
            total_objs += 1
            if '/searchable_neurons/' not in obj['Key'] or not obj['Key'].endswith('.tif'):
                continue
            total_size += obj['Size']
            files.append(obj['Key'])
    LOGGER.info(f"Checked {total_objs:,} objects on S3")
    print(f"Found {len(files):,} objects ({humansize(total_size)})")
    pname = {}
    scode = {}
    for file in files:
        fname = file.split('/')[-1]
        pname[fname.split('-')[0]] = True
        if library_type() != 'flyem':
            scode[fname.split('-')[1]] = fname.split('-')[0]
        if fname.split('-')[1] not in SLIDE:
            SLIDE[fname.split('-')[1]] = fname.split('-')[0]
        #elif SLIDE[fname.split('-')[1]] != fname.split('-')[0]:
        #    terminate_program(f"Mismatched AWS publishing name for {fname.split('-')[1]} " \
        #                      + f"{fname.split('-')[0]} {SLIDE[fname.split('-')[1]]}")
    print(f"Found {len(pname):,} publishing names and {len(scode):,} slide codes in S3")
    return pname, scode


def library_type():
    """ Get the library type from the library name
        Keyword arguments:
          None
        Returns:
          "flyem" or "flylight"
    """
    return 'flyem' if 'flyem' in ARG.LIBRARY.lower() else 'flylight'


def report_errors(mpname, mscode, apname, ascode):
    """ Report on publishing name/slide code errors
        Keyword arguments:
          mpname: dict of publishing names from Mongo
          mscode: dict of slide codes from Mongo
          apname: dict of publishing names from AWS
          ascode: dict of slide codes from AWS
        Returns:
          errors: list of errors
    """
    errors = []
    checked = {}
    print(f"{ARG.SOURCE} --> {'S3 manifest' if ARG.MANIFEST else 'AWS S3'}")
    for key in tqdm(mscode, desc=f"{ARG.SOURCE} slide codes"):
        if key not in ascode:
            rel = '' if library_type() == 'flyem' else get_releases(key)
            errors.append(f"{key}{rel} is in {ARG.SOURCE} but not in S3")
    for key in tqdm(mpname, desc=f"{ARG.SOURCE} publishing names"):
        if key not in apname and key not in checked:
            rel = '' if library_type() == 'flyem' else get_releases(key)
            errors.append(f"{key}{rel} is in {ARG.SOURCE} but not in S3")
    print(f"{ARG.SOURCE} <-- {'S3 manifest' if ARG.MANIFEST else 'AWS S3'}")
    for key in tqdm(ascode, desc='AWS S3 slide codes'):
        if key not in mscode:
            rel = '' if library_type() == 'flyem' else get_releases(key)
            errors.append(f"{key}{rel} ({SLIDE[key]}) is in S3 but not in {ARG.SOURCE}")
            checked[SLIDE[key]] = True
    for key in tqdm(apname, desc='AWS S3 publishing names'):
        if key not in mpname and key not in checked:
            rel = '' if library_type() == 'flyem' else get_releases(key)
            errors.append(f"{key}{rel} is in S3 but not in {ARG.SOURCE}")

    return errors


def run_backcheck():
    """ Check publishing names in S3 vs. publishedURL or neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    """
    if library_type() != 'flyem':
        populate_releases()
    mpname, mscode = get_mongo_data()
    apname, ascode = get_aws_data()
    errors = report_errors(mpname, mscode, apname, ascode)
    if errors:
        LOGGER.error("There are discrepancies in publishing names/slide codes")
        with open('mongo_aws_mismatches.txt','w', encoding='ascii') as outstream:
            outstream.write(f"{ARG.TEMPLATE}/{ARG.LIBRARY}\n")
            for err in errors:
                outstream.write(f"{err}\n")
    else:
        print("All publishing names/slide codes matched")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Backcheck AWS S3 to neuronMetadata")
    PARSER.add_argument('--bucket', dest='BUCKET', action='store',
                        default='janelia-flylight-color-depth', help='AWS S3 bucket')
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='Alignment template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='Color depth library')
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        default='publishedURL', choices=['neuronMetadata', 'publishedURL'],
                        help='Source connection (neuronMetadata, publishedURL)')
    PARSER.add_argument('--manifest', dest='MANIFEST', action='store',
                        help='AWS S3 bucket manifest')
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
