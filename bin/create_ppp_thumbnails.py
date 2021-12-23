''' create_ppp_thumbnails.py
    Create/update PPP thumbnails
'''

import argparse
import json
import pprint
import sys
import boto3
import colorlog
from tqdm import tqdm

# AWS
LAMBDA_CLIENT = None
S3_CLIENT = None
# S3 locations
BASE_BUCKET = "janelia-ppp-match-"
PREFIX = "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.2.1"
# General
COUNT = {"Prefixes": 0, "Pages": 0, "Body IDs": 0, "Submitted": 0, "Submit error": 0}

# -------------------------------------------------------------------------------

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    global LAMBDA_CLIENT, S3_CLIENT # pylint: disable=W0603
    LAMBDA_CLIENT = boto3.client("lambda")
    S3_CLIENT = boto3.client('s3')


def invoke_lambda(bucket, body_id):
    """ Invoke the Lambda function for a single body ID
        Keyword arguments:
          bucket: bucket [janelia-ppp-match-prod]
          body_id: body ID
        Returns:
           None
    """
    payload = {"body_id": body_id}
    if "prod" not in bucket:
        payload["bucket"] = bucket
    response = LAMBDA_CLIENT.invoke(FunctionName="create_ppp_body_thumbnails",
                                    InvocationType="Event",
                                    Payload=json.dumps(payload))
    if ARG.DEBUG:
        pprint.pprint(response, indent=4)
    COUNT["Submitted" if response["StatusCode"] == 202 else "Submit error"] += 1
    if "Payload" in response:
        LOGGER.debug(response['Payload'].read().decode("utf-8"))


def create_thumbnails():
    """ Create thumbnails for a prefic in a bucket
        Keyword arguments:
          None
        Returns:
          None
    """
    bucket = BASE_BUCKET + ARG.MANIFOLD
    result = S3_CLIENT.list_objects(Bucket=bucket, Prefix=PREFIX + "/", Delimiter="/")
    lev1 = result.get('CommonPrefixes')
    for lev1pre in tqdm(lev1, desc="Prefixes"):
        bpre = lev1pre.get('Prefix').split("/")[-2]
        COUNT["Prefixes"] += 1
        #result2 = S3_CLIENT.list_objects(Bucket=bucket, Prefix="/".join([PREFIX, bpre]) + "/",
        #                                 Delimiter="/")
        paginator = S3_CLIENT.get_paginator("list_objects")
        pages = paginator.paginate(Bucket=bucket, Prefix="/".join([PREFIX, bpre]) + "/",
                                   Delimiter="/")
        for page in pages:
            COUNT["Pages"] += 1
            lev2 = page.get('CommonPrefixes')
            for lev2pre in lev2:
                body = lev2pre.get('Prefix').split("/")[-2]
                COUNT["Body IDs"] += 1
                if ARG.WRITE:
                    invoke_lambda(bucket, body)
                else:
                    LOGGER.debug("/".join([bucket, bpre, body]))
    print(COUNT)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Create/recreate PPP thumbnails")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=["dev", "devpre", "prod"], default='prod', help='Manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually invoke Lambdas')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    create_thumbnails()
    terminate_program()
