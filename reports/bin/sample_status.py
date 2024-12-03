''' sample_status.py
    This script is used to generate a report of the status of a sample in NeuronBridge
    precompute database tables.
'''
__version__ = '1.1.0'

import argparse
import collections
from operator import attrgetter
import re
import sys
import boto3
import botocore
from colorama import Fore, Back, Style
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-not-lazy,logging-fstring-interpolation

# Database
DB = {}
READ = {"SC": "SELECT workstation_sample_id,slide_code,publishing_name,area,tile,objective,"
              + "alps_release,s.parent FROM image_data_mv i LEFT OUTER JOIN secondary_image_vw s "
              + "ON (i.id=s.image_id AND s.product='aligned_jrc2018_unisex_hr_stack') "
              + "WHERE slide_code=%s AND alps_release IS NOT NULL"
    }
# AWS S3
AWS = {}
S3_SECONDS = 60 * 60 * 12

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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
        aws = JRC.get_config("aws")
    except Exception as err:
        terminate_program(err)
    # Database
    for source in ("sage", "neuronbridge"):
        manifold = 'prod' if source == 'sage' else ARG.MANIFOLD
        dbo = attrgetter(f"{source}.{manifold}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    # AWS S3
    try:
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=attrgetter("role_arn")(aws),
                                     RoleSessionName="AssumeRoleSession1",
                                     DurationSeconds=S3_SECONDS)
        credentials = aro['Credentials']
        AWS['client'] = boto3.client('s3',
                                     aws_access_key_id=credentials['AccessKeyId'],
                                     aws_secret_access_key=credentials['SecretAccessKey'],
                                     aws_session_token=credentials['SessionToken'])
    except Exception as err:
        terminate_program(err)


def show_sage():
    ''' Show data from SAGE
        Keyword arguments:
          None
        Returns:
          None
    '''
    sql = READ['SC']
    if ARG.SAMPLE:
        sql = sql.replace('WHERE slide_code', 'WHERE workstation_sample_id')
    try:
        DB['sage']['cursor'].execute(sql, (ARG.SAMPLE if ARG.SAMPLE else ARG.SLIDE,))
        rows = DB['sage']['cursor'].fetchall()
    except Exception as err:
        terminate_program(JRC.sql_error(err))
    if not rows:
        LOGGER.warning(f"{'Sample '+ARG.SAMPLE if ARG.SAMPLE else 'Slide code '+ARG.SLIDE} " \
                       + "was not found in SAGE")
        return
    colsize = collections.defaultdict(lambda: 0, {})
    colsize['publishing_name'] = 14
    colsize['objective'] = 9
    out = []
    pnames = {}
    samples = {}
    for row in rows:
        pnames[row['publishing_name']] = True
        samples[row['workstation_sample_id']] = True
        osearch = re.search(r' (\d+[Xx])/', row['objective'], re.IGNORECASE)
        if osearch:
            row['objective'] = osearch.group(1)
        for col in ('workstation_sample_id', 'slide_code', 'publishing_name', 'area', 'tile',
                    'objective', 'alps_release', 'parent'):
            if row[col] is None:
                row[col] = ''
            if len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
        out.append(row)
    print(f"---------- SAGE ({len(rows)}) ----------")
    print(f"{'Sample':{colsize['workstation_sample_id']}}  " \
          + f"{'Slide code':{colsize['slide_code']}}  " \
          + f"{'Published name':{colsize['publishing_name']}}  " \
          + f"{'Area':{colsize['area']}}  {'Tile':{colsize['tile']}}  " \
          + f"{'Objective':{colsize['objective']}}  {'Release':{colsize['alps_release']}}  " \
          + f"{'Alignment':{colsize['parent']}}")
    for row in out:
        print(f"{row['workstation_sample_id']:{colsize['workstation_sample_id']}}  " \
              + f"{row['slide_code']:{colsize['slide_code']}}  " \
              + f"{row['publishing_name']:{colsize['publishing_name']}}  " \
              + f"{row['area']:{colsize['area']}}  {row['tile']:{colsize['tile']}}  " \
              + f"{row['objective']:{colsize['objective']}}  " \
              + f"{row['alps_release']:{colsize['alps_release']}}  " \
              + f"{row['parent']:{colsize['parent']}}")
    if len(pnames) > 1:
        print(Fore.RED + f"Multiple published names found: {', '.join(pnames.keys())}" \
              + Style.RESET_ALL)
    if len(samples) > 1:
        print(Fore.YELLOW + f"Multiple samples found: {', '.join(samples.keys())}" \
              + Style.RESET_ALL)


def show_nmd():
    ''' Show data from neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"sourceRefId": "Sample#" + ARG.SAMPLE}
    else:
        payload = {"slideCode": ARG.SLIDE}
    rows = None
    try:
        cnt =  DB['neuronbridge']['neuronMetadata'].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge']['neuronMetadata'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW \
              + f"\n{'Sample '+ARG.SAMPLE if ARG.SAMPLE else 'Slide code '+ARG.SLIDE} " \
              + "was not found in neuronMetadata" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    colsize['publishedName'] = 14
    colsize['objective'] = 9
    out = []
    for row in rows:
        row['sourceRefId'] = row['sourceRefId'].replace('Sample#','')
        row['datasetLabels'] = ', '.join(row['datasetLabels'])
        for col in ('sourceRefId', 'slideCode', 'publishedName', 'anatomicalArea', 'objective',
                    'datasetLabels'):
            if len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
        out.append(row)
    print(f"\n---------- neuronMetadata ({cnt}) ----------")
    print(f"{'Sample':{colsize['sourceRefId']}}  {'Slide code':{colsize['slideCode']}}  " \
          + f"{'Published name':{colsize['publishedName']}}  " \
          + f"{'Area':{colsize['anatomicalArea']}}  " \
          + f"{'Objective':{colsize['objective']}}  {'Release':{colsize['datasetLabels']}}")
    for row in out:
        print(f"{row['sourceRefId']:{colsize['sourceRefId']}}  " \
              + f"{row['slideCode']:{colsize['slideCode']}}  " \
              + f"{row['publishedName']:{colsize['publishedName']}}  " \
              + f"{row['anatomicalArea']:{colsize['anatomicalArea']}}  " \
              + f"{row['objective']:{colsize['objective']}}  " \
              + f"{row['datasetLabels']:{colsize['datasetLabels']}}")


def check_s3(uploaded, s3files, outs3, colsize, errtype):
    ''' Look for files on AWS S3
        Keyword arguments:
          uploaded: dictionary of files (key: file type, value: full path)
          s3files: files already checked on S3
          outs3: output list
          colsize: dictionary of column sizes
          errtype: dictionary of error types
        Returns:
          None
    '''
    for ftype, full in uploaded.items():
        if len(ftype) > colsize['ftype']:
            colsize['ftype'] = len(ftype)
        floc = full.replace('https://s3.amazonaws.com/', '')
        bucket, key = floc.split('/', 1)
        if ftype in s3files and key in s3files[ftype]:
            continue
        if ftype not in s3files:
            s3files[ftype] = {}
        s3files[ftype][key] = True
        if len(key) > colsize['key']:
            colsize['key'] = len(key)
        try:
            AWS['client'].head_object(Bucket=bucket, Key=key.replace('+', ' '))
            outs3.append([ftype, key])
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":
                errtype['notfound'] = True
                outs3.append([ftype, Fore.RED+key+Style.RESET_ALL])
            else:
                errtype['other'] = True
                outs3.append([ftype, Back.RED+key+Style.RESET_ALL])


def show_purl():
    ''' Show data from publishedURL and AWS S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"sample": "Sample#" + ARG.SAMPLE}
    else:
        payload = {"slideCode": ARG.SLIDE}
    rows = None
    try:
        cnt =  DB['neuronbridge']['publishedURL'].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge']['publishedURL'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW \
              + f"\n{'Sample '+ARG.SAMPLE if ARG.SAMPLE else 'Slide code '+ARG.SLIDE} " \
              + "was not found in publishedURL" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    colsize['publishedName'] = 14
    colsize['objective'] = 9
    out = []
    s3files = {}
    outs3 = []
    colsize['ftype'] = 9
    colsize['key'] = 0
    errtype = {}
    for row in rows:
        row['sampleRef'] = row['sampleRef'].replace('Sample#','')
        for col in ('sampleRef', 'slideCode', 'publishedName', 'anatomicalArea', 'objective',
                    'alpsRelease'):
            if len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
        out.append(row)
        # AWS S3 uploads
        if 'uploaded' in row and row['uploaded']:
            check_s3(row['uploaded'], s3files, outs3, colsize, errtype)
    print(f"\n---------- publishedURL ({cnt}) ----------")
    print(f"{'Sample':{colsize['sampleRef']}}  {'Slide code':{colsize['slideCode']}}  " \
          + f"{'Published name':{colsize['publishedName']}}  " \
          + f"{'Area':{colsize['anatomicalArea']}}  " \
          + f"{'Objective':{colsize['objective']}}  {'Release':{colsize['alpsRelease']}}")
    for row in out:
        print(f"{row['sampleRef']:{colsize['sampleRef']}}  " \
              + f"{row['slideCode']:{colsize['slideCode']}}  " \
              + f"{row['publishedName']:{colsize['publishedName']}}  " \
              + f"{row['anatomicalArea']:{colsize['anatomicalArea']}}  " \
              + f"{row['objective']:{colsize['objective']}}  " \
              + f"{row['alpsRelease']:{colsize['alpsRelease']}}")
    print(f"\n---------- AWS S3 for publishedURL ({len(outs3)}) ----------")
    print(f"{'File type':{colsize['ftype']}}  {'Key':{colsize['key']}}")
    for row in outs3:
        print(f"{row[0]:{colsize['ftype']}}  {row[1]:{colsize['key']}}")
    if 'notfound' in errtype:
        print(f"Some S3 keys were not found and are shown in {Fore.RED+'red'+Style.RESET_ALL}")
    if 'other' in errtype:
        print(f"Some S3 keys are in error and are shown in {Back.RED+'red'+Style.RESET_ALL}")


def show_pli():
    ''' Show data from publishedLMImage and AWS S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"sampleRef": "Sample#" + ARG.SAMPLE}
    else:
        payload = {"slideCode": ARG.SLIDE}
    rows = None
    try:
        cnt =  DB['neuronbridge']['publishedLMImage'].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge']['publishedLMImage'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW + \
              f"\n{'Sample '+ARG.SAMPLE if ARG.SAMPLE else 'Slide code '+ARG.SLIDE} " \
              + "was not found in publishedLMImage" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    colsize['name'] = 14
    colsize['tile'] = 4
    colsize['objective'] = 9
    colsize['alignment'] = 9
    out = []
    s3files = {}
    outs3 = []
    colsize['ftype'] = 9
    colsize['key'] = 0
    errtype = {}
    for row in rows:
        row['alignment'] = Fore.YELLOW + 'No' + Style.RESET_ALL
        if 'files' in row and 'VisuallyLosslessStack' in row['files']:
            row['alignment'] = 'Yes'
        row['sampleRef'] = row['sampleRef'].replace('Sample#','')
        for col in ('sampleRef', 'slideCode', 'name', 'area', 'tile', 'objective', 'releaseName',
                    'alignment'):
            if col not in row or row[col] is None:
                row[col] = ''
            if len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
        out.append(row)
        # AWS S3 uploads
        if 'files' in row and row['files']:
            check_s3(row['files'], s3files, outs3, colsize, errtype)
    print(f"\n---------- publishedLMImage ({cnt}) ----------")
    print(f"{'Sample':{colsize['sampleRef']}}  {'Slide code':{colsize['slideCode']}}  " \
          + f"{'Published name':{colsize['name']}}  {'Area':{colsize['area']}}  " \
          + f"{'Tile':{colsize['tile']}}  {'Objective':{colsize['objective']}}  " \
          + f"{'Release':{colsize['releaseName']}}  {'Alignment':{colsize['alignment']}}")
    for row in out:
        print(f"{row['sampleRef']:{colsize['sampleRef']}}  " \
              + f"{row['slideCode']:{colsize['slideCode']}}  " \
              + f"{row['name']:{colsize['name']}}  {row['area']:{colsize['area']}}  " \
              + f"{row['tile']:{colsize['tile']}}  {row['objective']:{colsize['objective']}}  " \
              + f"{row['releaseName']:{colsize['releaseName']}}  " \
              + f"{row['alignment']:{colsize['alignment']}}")
    print(f"\n---------- AWS S3 for publishedLMImage ({len(outs3)}) ----------")
    print(f"{'File type':{colsize['ftype']}}  {'Key':{colsize['key']}}")
    for row in outs3:
        print(f"{row[0]:{colsize['ftype']}}  {row[1]:{colsize['key']}}")
    if 'notfound' in errtype:
        print(f"Some S3 keys were not found and are shown in {Fore.RED+'red'+Style.RESET_ALL}")
    if 'other' in errtype:
        print(f"Some S3 keys are in error and are shown in {Back.RED+'red'+Style.RESET_ALL}")


def sample_status():
    ''' Report on the status of a sample in NeuronBridge precompute database tables
        Keyword arguments:
          None
        Returns:
          None
    '''
    show_sage()
    show_nmd()
    show_purl()
    show_pli()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Report on sample status")
    LOOKUP = PARSER.add_mutually_exclusive_group(required=True)
    LOOKUP.add_argument('--sample', dest='SAMPLE', action='store',
                        default='', help='Sample')
    LOOKUP.add_argument('--slide', dest='SLIDE', action='store',
                        default='', help='Slide code')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold [prod]')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    sample_status()
    terminate_program()
