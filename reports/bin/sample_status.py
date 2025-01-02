''' sample_status.py
    This script is used to generate a report of the status of a sample, slide code, or body ID in
    NeuronBridge precompute database tables (and AWS S3 and DynamoDB).
    Examples:
        python3 sample_status.py --sample 2489243463367786594
        python3 sample_status.py --slide 20190816_62_G9
        python3 sample_status.py --body 1537331894
        python3 sample_status.py --body 720575940596125868
        python3 sample_status.py --slide 20160617_24_C1
'''
__version__ = '3.0.0'

import argparse
import collections
from operator import attrgetter
import re
import sys
import boto3
from boto3.dynamodb.conditions import Key
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
# General
PNAME = {}

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
    for source in ("sage", "gen1mcfo", "raw", "mbew", "jacs", "neuronbridge"):
        if source in ("sage","jacs", "neuronbridge"):
            manifold = 'prod'
        else:
            manifold = ARG.MANIFOLD
        dbo = attrgetter(f"{source}.{manifold}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    # AWS DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        DB['DYNAMO'] = dynamodb
        DB['DYNAMOCLIENT'] = boto3.client('dynamodb', region_name='us-east-1')
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


def set_colsize(colsize):
    ''' Set default column sizes
        Keyword arguments:
          colsize: dictionary of column sizes
        Returns:
          None
    '''
    colsize['alignment'] = 9
    colsize['anatomicalArea'] = 4
    colsize['area'] = 4
    colsize['citation'] = 8
    colsize['gender'] = 6
    colsize['link'] = 4
    colsize['name'] = 15
    colsize['neuronType'] = 11
    colsize['objective'] = 9
    colsize['publishedName'] = 14
    colsize['publishing_name'] = 14
    colsize['publishingName'] = 15
    colsize['releaseLabel'] = 7
    colsize['tile'] = 4


def show_sage(dbn='sage'):
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
        DB[dbn]['cursor'].execute(sql, (ARG.SAMPLE if ARG.SAMPLE else ARG.SLIDE,))
        rows = DB[dbn]['cursor'].fetchall()
    except Exception as err:
        terminate_program(JRC.sql_error(err))
    if not rows:
        LOGGER.warning(f"{'Sample '+ARG.SAMPLE if ARG.SAMPLE else 'Slide code '+ARG.SLIDE} " \
                       + "was not found in SAGE")
        return
    colsize = collections.defaultdict(lambda: 0, {})
    set_colsize(colsize)
    out = []
    fields = ['workstation_sample_id', 'slide_code', 'publishing_name', 'area', 'tile',
              'objective', 'alps_release', 'parent']
    pnames = {}
    samples = {}
    databases = []
    for row in rows:
        pnames[row['publishing_name']] = True
        samples[row['workstation_sample_id']] = True
        osearch = re.search(r' (\d+[Xx])/', row['objective'], re.IGNORECASE)
        if osearch:
            row['objective'] = osearch.group(1)
        for col in fields:
            if row[col] is None:
                row[col] = ''
            if len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
        out.append(row)
        if row['alps_release']:
            if 'Omnibus Broad' in row['alps_release']:
                ndb = 'raw'
            elif 'Gen1 MCFO' in row['alps_release']:
                ndb = 'gen1mcfo'
            else:
                ndb = 'mbew'
            if ndb not in databases:
                databases.append(ndb)
    print(f"---------- {dbn} ({len(rows)}) ----------")
    print(f"{'Sample':{colsize['workstation_sample_id']}}  " \
          + f"{'Slide code':{colsize['slide_code']}}  " \
          + f"{'Published name':{colsize['publishing_name']}}  " \
          + f"{'Area':{colsize['area']}}  {'Tile':{colsize['tile']}}  " \
          + f"{'Objective':{colsize['objective']}}  {'Release':{colsize['alps_release']}}  " \
          + f"{'Alignment':{colsize['parent']}}")
    for row in out:
        print('  '.join([f"{row[fld]:{colsize[fld]}}" for fld in fields]))
    if len(pnames) > 1:
        print(Fore.RED + f"Multiple published names found: {', '.join(pnames.keys())}" \
              + Style.RESET_ALL)
    if len(samples) > 1:
        print(Fore.YELLOW + f"Multiple samples found: {', '.join(samples.keys())}" \
              + Style.RESET_ALL)
    if dbn == 'sage':
        for next_dbn in databases:
            print()
            show_sage(next_dbn)


def show_sample():
    ''' Show data from jacs:sample
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"_id": int(ARG.SAMPLE)}
        itype = 'Sample'
        ival = ARG.SAMPLE
    else:
        payload = {"slideCode": ARG.SLIDE}
        itype = 'Slide code'
        ival = ARG.SLIDE
    rows = None
    try:
        cnt =  DB['jacs']['sample'].count_documents(payload)
        if cnt:
            rows = DB['jacs']['sample'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW + f"\n{itype} {ival} " \
              + "was not found in sample" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    set_colsize(colsize)
    out = []
    fields = ['_id', 'slideCode', 'line', 'publishingName', 'gender', 'dataSet',
              'releaseLabel', 'status']
    for row in rows:
        row['_id'] = str(row['_id'])
        for col in fields:
            if col in row and row[col] is None:
                row[col] = ''
            if col in row and row[col] and len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
            elif col not in row:
                row[col] = ''
        out.append(row)
    print(f"\n---------- sample ({cnt}) ----------")
    print(f"{'Sample':{colsize['_id']}}  {'Slide code':{colsize['slideCode']}}  " \
              + f"{'Line':{colsize['line']}}  " \
              + f"{'Publishing name':{colsize['publishingName']}}  " \
              + f"{'Gender':{colsize['gender']}}  {'Data set':{colsize['dataSet']}}  "\
              + f"{'Release':{colsize['releaseLabel']}}  " \
              + f"{'Status':{colsize['status']}}")
    for row in out:
        print('  '.join([f"{row[fld]:{colsize[fld]}}" for fld in fields]))


def show_image():
    ''' Show data from jacs:image
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"sampleRef": 'Sample#' + ARG.SAMPLE}
        itype = 'Sample'
        ival = ARG.SAMPLE
    else:
        payload = {"slideCode": ARG.SLIDE}
        itype = 'Slide code'
        ival = ARG.SLIDE
    rows = None
    try:
        cnt =  DB['jacs']['image'].count_documents(payload)
        if cnt:
            rows = DB['jacs']['image'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW + f"\n{itype} {ival} " \
              + "was not found in image" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    set_colsize(colsize)
    out = []
    fields = ['sampleRef', 'slideCode', 'line', 'anatomicalArea', 'tile', 'objective',
              'gender', 'dataSet', 'name']
    for row in rows:
        row['_id'] = str(row['_id'])
        for col in fields:
            if col in row and len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
            elif col not in row:
                row[col] = ''
        out.append(row)
    print(f"\n---------- image ({cnt}) ----------")
    print(f"{'Sample':{colsize['sampleRef']}}  {'Slide code':{colsize['slideCode']}}  " \
              + f"{'Line':{colsize['line']}}  " \
              + f"{'Area':{colsize['anatomicalArea']}}  {'Tile':{colsize['tile']}}  " \
              + f"{'Objective':{colsize['objective']}}  "
              + f"{'Gender':{colsize['gender']}}  {'Data set':{colsize['dataSet']}}  " \
              + f"{'Name':{colsize['name']}}")
    for row in out:
        print('  '.join([f"{row[fld]:{colsize[fld]}}" for fld in fields]))


def show_nmd():
    ''' Show data from neuronMetadata
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"sourceRefId": "Sample#" + ARG.SAMPLE}
        itype = 'Sample'
        ival = ARG.SAMPLE
    elif ARG.SLIDE:
        payload = {"slideCode": ARG.SLIDE}
        itype = 'Slide code'
        ival = ARG.SLIDE
    else:
        payload = {"publishedName": ARG.BODY}
        itype = 'Body ID'
        ival = ARG.BODY
    rows = None
    try:
        cnt =  DB['neuronbridge']['neuronMetadata'].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge']['neuronMetadata'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW + f"\n{itype} {ival} " \
              + "was not found in neuronMetadata" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    set_colsize(colsize)
    out = []
    if ARG.BODY:
        fields = ['publishedName', 'neuronType', 'neuronInstance']
    else:
        fields = ['sourceRefId', 'mipId', 'alignmentSpace', 'slideCode', 'publishedName',
                  'anatomicalArea', 'objective', 'gender', 'datasetLabels']
    for row in rows:
        row['sourceRefId'] = row['sourceRefId'].replace('Sample#','')
        if 'datasetLabels' in row:
            row['datasetLabels'] = ', '.join(row['datasetLabels'])
        else:
            row['datasetLabels'] = ''
        for col in fields:
            if col in row and len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
            elif col not in row:
                row[col] = ''
        out.append(row)
    print(f"\n---------- neuronMetadata ({cnt}) ----------")
    if ARG.BODY:
        print(f"{'Published name':{colsize['publishedName']}}  " \
              + f"{'Neuron type':{colsize['neuronType']}}  " \
              + f"{'Neuron instance':{colsize['neuronInstance']}}")
    else:
        print(f"{'Sample':{colsize['sourceRefId']}}  {'MIP':{colsize['mipId']}}  " \
              + f"{'Alignment':{colsize['alignmentSpace']}}  " \
              + f"{'Slide code':{colsize['slideCode']}}  "
              + f"{'Published name':{colsize['publishedName']}}  " \
              + f"{'Area':{colsize['anatomicalArea']}}  " \
              + f"{'Objective':{colsize['objective']}}  {'Gender':{colsize['gender']}}  " \
              + f"{'Release':{colsize['datasetLabels']}}")
    for row in out:
        print('  '.join([f"{row[fld]:{colsize[fld]}}" for fld in fields]))


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
                outs3.append([ftype, f"{Back.RED}{key} ({err}){Style.RESET_ALL}"])


def show_purl():
    ''' Show data from publishedURL and AWS S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.SAMPLE:
        payload = {"sampleRef": "Sample#" + ARG.SAMPLE}
        itype = 'Sample'
        ival = ARG.SAMPLE
    elif ARG.SLIDE:
        payload = {"slideCode": ARG.SLIDE}
        itype = 'Slide code'
        ival = ARG.SLIDE
    else:
        payload = {"publishedName": {"$regex": ":" + ARG.BODY + "$"}}
        itype = 'Body ID'
        ival = ARG.BODY
    rows = None
    try:
        cnt =  DB['neuronbridge']['publishedURL'].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge']['publishedURL'].find(payload)
    except Exception as err:
        terminate_program(err)
    if not cnt:
        print(Fore.YELLOW \
              + f"\n{itype} {ival} " \
              + "was not found in publishedURL" + Style.RESET_ALL)
        return
    colsize = collections.defaultdict(lambda: 0, {})
    set_colsize(colsize)
    out = []
    if ARG.BODY:
        fields = ['publishedName', 'name']
    else:
        fields = ['sampleRef', 'mipId', 'alignmentSpace', 'slideCode', 'publishedName',
                  'anatomicalArea', 'objective', 'gender', 'alpsRelease']
    s3files = {}
    outs3 = []
    colsize['ftype'] = 9
    colsize['key'] = 0
    errtype = {}
    for row in rows:
        row['sampleRef'] = row['sampleRef'].replace('Sample#','')
        for col in fields:
            if col in row and len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
        out.append(row)
        # AWS S3 uploads
        if 'uploaded' in row and row['uploaded']:
            check_s3(row['uploaded'], s3files, outs3, colsize, errtype)
    print(f"\n---------- publishedURL ({cnt}) ----------")
    if ARG.BODY:
        print(f"{'Published name':{colsize['publishedName']}}  {'Name':{colsize['name']}}")
    else:
        print(f"{'Sample':{colsize['sampleRef']}}  {'MIP':{colsize['mipId']}}  " \
              + f"{'Alignment':{colsize['alignmentSpace']}}  " \
              + f"{'Slide code':{colsize['slideCode']}}  "
              + f"{'Published name':{colsize['publishedName']}}  " \
              + f"{'Area':{colsize['anatomicalArea']}}  " \
              + f"{'Objective':{colsize['objective']}}  {'Gender':{colsize['gender']}}  " \
              + f"{'Release':{colsize['alpsRelease']}}")
    for row in out:
        PNAME[row['publishedName']] = True
        print('  '.join([f"{row[fld]:{colsize[fld]}}" for fld in fields]))
    print(f"\n---------- AWS S3 for publishedURL ({len(outs3)}) ----------")
    print(f"{'File type':{colsize['ftype']}}  {'Key':{colsize['key']}}")
    for row in outs3:
        print(f"{row[0]:{colsize['ftype']}}  {row[1]:{colsize['key']}}")
    if 'notfound' in errtype:
        print(f"Some S3 keys were not found and are shown in {Fore.RED+'red'+Style.RESET_ALL}")
    if 'other' in errtype:
        print(f"Some S3 keys are in error and are shown in {Back.RED+'red'+Style.RESET_ALL}")


def get_stacks(key):
    ''' Show entries in janelia-neuronbridge-published-stacks
        Keyword arguments:
          key: partition key
        Returns:
          Release name
    '''
    tbl = 'janelia-neuronbridge-published-stacks'
    DB[tbl] = DB['DYNAMO'].Table(tbl)
    try:
        response = DB[tbl].query(KeyConditionExpression= \
                                 Key('itemType').eq(key))
    except DB['DYNAMOCLIENT'].exceptions.ResourceNotFoundException:
        terminate_program(f"DynamoDB table {tbl} does not exist")
    if not response or ('Items' not in response) or not response['Items']:
        return None
    return response['Items'][0]['releaseName']


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
    set_colsize(colsize)
    out = []
    fields = ['sampleRef', 'slideCode', 'name', 'area', 'tile', 'objective', 'releaseName',
              'alignment']
    s3files = {}
    outs3 = []
    colsize['ftype'] = 9
    colsize['key'] = 0
    colsize['release'] = 0
    errtype = {}
    ddb = {}
    for row in rows:
        row['alignment'] = Fore.YELLOW + 'No' + Style.RESET_ALL
        if 'files' in row and 'VisuallyLosslessStack' in row['files']:
            row['alignment'] = 'Yes'
        row['sampleRef'] = row['sampleRef'].replace('Sample#','')
        for col in fields:
            if col not in row or row[col] is None:
                row[col] = ''
            if len(row[col]) > colsize[col]:
                colsize[col] = len(row[col])
            ddb_key = '-'.join([row['slideCode'], row['objective'], row['alignmentSpace']]).lower()
            if ddb_key not in ddb:
                ret = get_stacks(ddb_key)
                if ret:
                    ddb[ddb_key] = ret
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
        print('  '.join([f"{row[fld]:{colsize[fld]}}" for fld in fields]))
    print(f"\n---------- AWS S3 for publishedLMImage ({len(outs3)}) ----------")
    print(f"{'File type':{colsize['ftype']}}  {'Key':{colsize['key']}}")
    for row in outs3:
        print(f"{row[0]:{colsize['ftype']}}  {row[1]:{colsize['key']}}")
    if 'notfound' in errtype:
        print(f"Some S3 keys were not found and are shown in {Fore.RED+'red'+Style.RESET_ALL}")
    if 'other' in errtype:
        print(f"Some S3 keys are in error and are shown in {Back.RED+'red'+Style.RESET_ALL}")
    if ddb:
        print("\n---------- DynamoDB janelia-neuronbridge-published-stacks " \
              + f"({len(ddb)}) ----------")
        colsize['key'] = 0
        for key, val in ddb.items():
            if len(key) > colsize['key']:
                colsize['key'] = len(key)
            if len(val) > colsize['release']:
                colsize['release'] = len(val)
        print(f"{'itemType':{colsize['key']}}  {'Release':{colsize['release']}}")
        for key, val in ddb.items():
            print(f"{key:{colsize['key']}}  {val:{colsize['release']}}")


def show_dois():
    ''' Show entries in janelia-neuronbridge-publishing-doi
        Keyword arguments:
          key: partition key
        Returns:
          None
    '''
    tbl = 'janelia-neuronbridge-publishing-doi'
    DB[tbl] = DB['DYNAMO'].Table(tbl)
    colsize = collections.defaultdict(lambda: 0, {})
    set_colsize(colsize)
    out = []
    for pname in PNAME:
        try:
            response = DB[tbl].query(KeyConditionExpression= \
                                     Key('name').eq(pname))
        except DB['DYNAMOCLIENT'].exceptions.ResourceNotFoundException:
            terminate_program(f"DynamoDB table {tbl} does not exist")
        if not response or ('Items' not in response) or not response['Items']:
            continue
        dois = response['Items'][0]
        if len(dois['name']) > colsize['name']:
            colsize['name'] = len(dois['name'])
        for doi in dois['doi']:
            if 'link' not in doi:
                doi['link'] = ''
            for col in ('link', 'citation'):
                if len(doi[col]) > colsize[col]:
                    colsize[col] = len(doi[col])
            out.append({'name': dois['name'], 'link': doi['link'], 'citation': doi['citation']})
    if not out:
        return
    print(f"\n---------- DynamoDB {tbl} ({len(out)}) ----------")
    print(f"{'Publishing name':{colsize['name']}}  {'Citation':{colsize['citation']}}  " \
          + f"{'Link':{colsize['link']}}")
    for row in out:
        print(f"{row['name']:{colsize['name']}}  {row['citation']:{colsize['citation']}}  " \
              + f"{row['link']:{colsize['link']}}")


def sample_status():
    ''' Report on the status of a sample in NeuronBridge precompute database tables
        Keyword arguments:
          None
        Returns:
          None
    '''
    if not ARG.BODY:
        show_sage()
        show_sample()
        show_image()
    show_nmd()
    show_purl()
    if not ARG.BODY:
        show_pli()
    show_dois()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Report on sample status")
    LOOKUP = PARSER.add_mutually_exclusive_group(required=True)
    LOOKUP.add_argument('--body', dest='BODY', action='store',
                        default='', help='Body ID or FlyWire Root ID')
    LOOKUP.add_argument('--sample', dest='SAMPLE', action='store',
                        default='', help='Sample')
    LOOKUP.add_argument('--slide', dest='SLIDE', action='store',
                        default='', help='Slide code')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='staging', choices=['staging', 'prod'],
                        help='Publishing manifold [staging]')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    sample_status()
    terminate_program()
