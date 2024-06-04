''' delete_duplicate_neurons.py
    Find searchable_neurons files in multiple partitions, and delete the duplicates
'''

import argparse
import json
import sys
import boto3
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB

# pylint: disable=broad-exception-caught,logging-fstring-interpolation
AWSS3 = {"client": None}
COUNT = {"keys": 0, "files": 0, "deleted": 0}
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


def get_parms():
    """ Query the user for the template and CDM library
        Keyword arguments:
            None
        Returns:
            None
    """
    bucket = "janelia-flylight-color-depth"
    if ARG.MANIFOLD != "prod":
        bucket = bucket + f"-{ARG.MANIFOLD}"
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(AWSS3["client"], bucket)
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(client=AWSS3["client"], bucket=bucket, template=ARG.TEMPLATE)


def initialize_program():
    """ Initialize S3 client and set parameters
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.MANIFOLD == 'prod':
        try:
            aws = JRC.get_config("aws")
        except Exception as err:
            terminate_program(err)
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=aws.role_arn,
                                     RoleSessionName="AssumeRoleSession1")
        credentials = aro['Credentials']
        AWSS3["client"] = boto3.client('s3',
                                       aws_access_key_id=credentials['AccessKeyId'],
                                       aws_secret_access_key=credentials['SecretAccessKey'],
                                       aws_session_token=credentials['SessionToken'])
    else:
        AWSS3["client"] = boto3.client('s3')
    get_parms()


def read_object(bucket, key):
    ''' Read a JSON file and return the contents
        Keyword arguments:
          bucket: bucket name
          key: object key
        Returns:
          JSON
    '''
    LOGGER.info(f"Reading {bucket}/{key}")
    try:
        data = AWSS3["client"].get_object(Bucket=bucket, Key=key)
        contents = data['Body'].read().decode("utf-8")
    except Exception as err:
        terminate_program(err)
    return json.loads(contents)


def process_file():
    """ Find files to delete
        Keyword arguments:
            None
        Returns:
            None
    """
    bucket = "janelia-flylight-color-depth"
    if ARG.MANIFOLD != "prod":
        bucket = bucket + f"-{ARG.MANIFOLD}"
    data = read_object(bucket, f"{ARG.TEMPLATE}/{ARG.LIBRARY}/" \
                               + "searchable_neurons/keys_denormalized.json")
    COUNT['files'] = len(data)
    keys = {}
    for file in data:
        key = file.split("/")[-1]
        if key not in keys:
            keys[key] = []
        keys[key].append(file)
    COUNT['keys'] = len(keys)
    if len(data) == len(keys):
        print("No updates needed")
        return
    for key in tqdm(keys, desc="Processing keys"):
        if len(keys[key]) > 1:
            LOGGER.warning(f"Delete {keys[key][-1]}")
            COUNT['deleted'] += 1
            if not ARG.WRITE:
                continue
            try:
                AWSS3["client"].delete_object(Bucket=bucket, Key=keys[key][-1])
            except Exception as err:
                terminate_program(err)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Delete duplicate searchable_neurons files")
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='Alignment space')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        help='Color depth library')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='AWS S3 manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually delete files')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_file()
    print(f"Files found:   {COUNT['files']:,}")
    print(f"Keys found:    {COUNT['keys']:,}")
    print(f"Files deleted: {COUNT['deleted']:,}")
    terminate_program()
