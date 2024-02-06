''' This program downloads an AWS bucket inventory as a CSV file. We only use the first column.
'''

import argparse
import csv
from datetime import datetime
import gzip
import json
import re
import sys
import boto3
import jrc_common.jrc_common as JRC
from aws_s3_lib import get_prefixes

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# AWS
S3 = {}


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
    ''' Initialize S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Opening S3 client")
    if "dev" in ARG.MANIFOLD:
        S3['client'] = boto3.client('s3')
    else:
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=AWS.role_arn,
                                     RoleSessionName="AssumeRoleSession1",
                                     DurationSeconds=3600)
        credentials = aro['Credentials']
        S3['client'] = boto3.client('s3',
                                    aws_access_key_id=credentials['AccessKeyId'],
                                    aws_secret_access_key=credentials['SecretAccessKey'],
                                    aws_session_token=credentials['SessionToken'])


def get_object(bucket, key):
    ''' Return the contents of an object
        Keyword arguments:
          bucket: bucket
          key: object key
        Returns:
          Undecoded object contents
    '''
    LOGGER.info(f"Fetching {key} from {bucket}")
    try:
        s3_response_object = S3['client'].get_object(Bucket=bucket, Key=key)
    except Exception as err:
        terminate_program(err)
    object_content = s3_response_object['Body'].read()
    return object_content


def get_manifest():
    ''' Get the latest manifest
        Keyword arguments:
          None
        Returns:
          data: latest manifest as JSON
    '''
    ibase = f"{ARG.BUCKET}/BasicInventory"
    prefixes = get_prefixes(ARG.INVENTORY, prefix=ibase, client=S3['client'])
    prefix = ''
    for pfx in prefixes:
        if not re.match(r"^\d{4}-", pfx):
            continue
        if pfx > prefix:
            prefix = pfx
    LOGGER.info(f"Downloading manifest from {prefix}")
    try:
        contents = get_object(ARG.INVENTORY, f"{ibase}/{prefix}/manifest.json").decode("utf-8")
        data = json.loads(contents)
    except Exception as err:
        terminate_program(err)
    return data


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


def get_csv():
    ''' Get the latest bucket inventory as a CSV file
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.MANIFEST:
        with open(ARG.MANIFEST, 'r', encoding='ascii') as instream:
            data = json.load(instream)
    else:
        data = get_manifest()
    source = data['sourceBucket']
    bucket = data['destinationBucket'].split(':')[-1]
    LOGGER.info(f"Getting manifest for {source}")
    lines = 0
    filename = f"{source}_manifest.{datetime.today().strftime('%Y%m%d')}.txt"
    total = 0
    with open(filename, 'w', encoding='ascii') as outstream:
        for file in data['files']:
            cdata = get_object(bucket, file['key'])
            udata = str(gzip.decompress(cdata), 'UTF-8')
            reader = csv.reader(udata.split('\n'), delimiter=',')
            for row in reader:
                if not row:
                    continue
                try:
                    outstream.write(f"{row[1]}\n")
                except Exception as err:
                    terminate_program(err)
                total += int(row[2])
                lines += 1
    print(f"Wrote {lines:,} keys to {filename}")
    print(f"Bucket size: {humansize(total)}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Download AWS S3 manifest")
    PARSER.add_argument('--bucket', dest='BUCKET', action='store',
                        default='janelia-flylight-color-depth', help='AWS S3 bucket')
    PARSER.add_argument('--inventory', dest='INVENTORY', action='store',
                        default='janelia-flylight-inventory', help='Inventory bucket')
    PARSER.add_argument('--manifest', dest='MANIFEST', action='store',
                        help='Manifest file')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='Manifold')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    AWS = JRC.get_config("aws")
    initialize_s3()
    get_csv()
    terminate_program()
