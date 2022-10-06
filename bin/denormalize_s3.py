''' denormalize_s3.py
    Denormalize an S3 bucket (with a Template/Library prefix).
    Two files are written to the Template/Library prefix:
      keys_denormalized.json: list of image files in Template/Library
      counts_denormalized.json: count of image files in Template/Library
    Two files are created in each Template/Library/<variant> prefix:
      keys_denormalized.json: list of image files in Template/Library/<variant>
      counts_denormalized.json: count of image files in Template/Library/<variant>
    For variants in DISTRIBUTE_FILES, an order file for use with s3cp will be created
    which will copy keys_denormalized.json to Template/Library/<variant>/KEYS/<num>
    where <num> is a number from 0-99.
'''

import argparse
import json
import os
import random
import sys
import tempfile
from types import SimpleNamespace
import colorlog
import boto3
from botocore.exceptions import ClientError
import requests
from tqdm import tqdm
import neuronbridge_lib as NB

__version__ = '1.1.1'
# Configuration
KEYFILE = "keys_denormalized.json"
COUNTFILE = "counts_denormalized.json"
DISTRIBUTE_FILES = ['searchable_neurons']
TAGS = 'PROJECT=CDCS&STAGE=prod&DEVELOPER=svirskasr&VERSION=%s' % (__version__)


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


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


def create_config_object(config):
    """ Convert the JSON received from a configuration to an object
        Keyword arguments:
          config: configuration name
        Returns:
          Configuration object
    """
    data = (call_responder("config", f"config/{config}"))["config"]
    return json.loads(json.dumps(data), object_hook=lambda dat: SimpleNamespace(**dat))


def initialize_program():
    """ Initialize
    """
    random.seed()


def upload_to_aws(s3r, body, object_name):
    """ Upload a file to AWS S3
        Keyword arguments:
          s3r: S3 resource
          body: JSON
          object_name: object
        Returns:
          None
    """
    if not ARG.WRITE:
        LOGGER.warning("Would have uploaded %s", object_name)
        return
    LOGGER.info("Uploading %s", object_name)
    try:
        bucket = s3r.Bucket(ARG.BUCKET)
        bucket.put_object(Body=body,
                          Key=object_name,
                          #ACL='public-read',
                          ContentType='application/json',
                          Tagging=TAGS)
    except ClientError as err:
        LOGGER.error("Could not upload %s", object_name)
        LOGGER.error(str(err))


def write_order_file(which, body, prefix):
    """ Write an order file for use with s3cp
        Keyword arguments:
            which: first prefix (e.g. "searchable_neurons")
            body: JSON
            prefix: partial key prefix
        Returns:
            order file name
    """
    fname = tempfile.mktemp()
    source_file = "%s_%s.txt" % (fname, which)
    LOGGER.info("Writing temporary file %s", source_file)
    tfile = open(source_file, "w")
    tfile.write(body)
    tfile.close()
    order_file = source_file.replace('.txt', '.order')
    LOGGER.info("Writing order file %s", order_file)
    ofile = open(order_file, "w")
    for chunk in range(100):
        ofile.write("%s\t%s\n" % (source_file, '/'.join([ARG.BUCKET, prefix, 'KEYS',
                                                         str(chunk), 'keys_denormalized.json'])))
    ofile.close()
    return order_file


def get_parms(s3_client):
    """ Query the user for the CDM library and manifold
        Keyword arguments:
            s3_client: S3 client
        Returns:
            None
    """
    cdm = (call_responder('config', 'config/cdm_library'))["config"]
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library_from_aws(cdm)
        if not ARG.LIBRARY:
            LOGGER.error("No library selected")
            sys.exit(0)
        print(ARG.LIBRARY)
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(s3_client, ARG.BUCKET)
        if not ARG.TEMPLATE:
            LOGGER.error("No alignment template selected")
            sys.exit(0)
        print(ARG.TEMPLATE)
    for cdmlib in cdm:
        if cdm[cdmlib]['name'].replace(' ', '_') == ARG.LIBRARY:
            for jsonfile in cdm[cdmlib][ARG.MANIFOLD]:
                rec = cdm[cdmlib][ARG.MANIFOLD][jsonfile]
                if rec is dict and "updated" in rec and rec["updated"]:
                    print("Library %s was last modified on %s on %s"
                          % (cdm[cdmlib]['name'], ARG.MANIFOLD,
                             rec['updated']))
            break


def initialize_s3():
    """ Initialize S3 client and resource
        Keyword arguments:
          None
        Returns:
          S3 client and resource
    """
    if ARG.MANIFOLD == 'prod':
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=AWS.role_arn,
                                     RoleSessionName="AssumeRoleSession1")
        credentials = aro['Credentials']
        s3_client = boto3.client('s3',
                                 aws_access_key_id=credentials['AccessKeyId'],
                                 aws_secret_access_key=credentials['SecretAccessKey'],
                                 aws_session_token=credentials['SessionToken'])
        s3_resource = boto3.resource('s3',
                                     aws_access_key_id=credentials['AccessKeyId'],
                                     aws_secret_access_key=credentials['SecretAccessKey'],
                                     aws_session_token=credentials['SessionToken'])
    else:
        ARG.BUCKET = '-'.join([ARG.BUCKET, ARG.MANIFOLD])
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
    return s3_client, s3_resource


def populate_batch_dict(s3_client, prefix):
    """ Produce a dict with key/batch information
        Keyword arguments:
          s3_client: S3 client
          prefix: top-level prefix
        Returns:
          batch dictionary
    """
    LOGGER.info("Batching keys for %s", prefix)
    total_objects = dict()
    key_list = dict()
    max_batch = dict()
    first_batch = dict()
    skipped_objects = 0;
    for which in DISTRIBUTE_FILES:
        max_batch[which] = 0
        first_batch[which] = 0
    keys = []
    for obj in NB.get_all_s3_objects(s3_client, Bucket=ARG.BUCKET, Prefix=prefix):
        if KEYFILE in obj['Key'] or COUNTFILE in obj['Key'] or "pngs" in obj['Key'] \
           or obj['Key'].endswith("/"):
            continue
        keys.append( obj['Key'])
    for key in tqdm(keys, desc="Keys"):
        which = 'default'
        LOGGER.debug(key)
        splitkey = key.split('/')
        if len(splitkey) >= 4:
            which = splitkey[2]
        if which not in key_list:
            LOGGER.info("Adding key %s", which)
            key_list[which] = list()
            total_objects[which] = 0
        # Skip here
        if which == 'searchable_neurons':
            fname = key.split("/")[-1]
            if fname in EXCLUSION:
                skipped_objects += 1
                continue
        total_objects[which] += 1
        key_list[which].append(key)
        if which in DISTRIBUTE_FILES:
            num = int(key.split("/")[3])
            if not first_batch[which]:
                first_batch[which] = num
            if num > max_batch[which]:
                max_batch[which] = num
    batch_size = dict()
    for which in DISTRIBUTE_FILES:
        batch_size[which] = 0
        objs = NB.get_all_s3_objects(s3_client, Bucket=ARG.BUCKET, Prefix=prefix + which + "/"
                                     + str(first_batch[which]) + "/")
        for obj in objs:
            batch_size[which] += 1
    batch_dict = {'count': total_objects,
                  'keys': key_list,
                  'size': batch_size,
                  'max_batch': max_batch}
    for which in key_list:
        print(which)
        print("  Total objects: %d" % (total_objects[which]))
        print("  Total keys:    %d" % (len(key_list[which])))
        if which in batch_size:
            print("  Batch size:    %d" % (batch_size[which]))
            print("  Max batch:     %d" % (max_batch[which]))
    print("Skipped objects: %d" % (skipped_objects))
    return batch_dict


def denormalize():
    """ Denormalize a bucket into a JSON file
        Keyword arguments:
          None
        Returns:
          None
    """
    #pylint: disable=no-member
    s3_client, s3_resource = initialize_s3()
    get_parms(s3_client)
    prefix = '/'.join([ARG.TEMPLATE, ARG.LIBRARY]) + '/'
    print("Processing %s on %s manifold" % (ARG.LIBRARY, ARG.MANIFOLD))
    batch_dict = populate_batch_dict(s3_client, prefix)
    if not batch_dict['count'] or not batch_dict['count']['default']:
        LOGGER.error("%s/%s was not found in the %s bucket", ARG.TEMPLATE, ARG.LIBRARY, ARG.BUCKET)
        sys.exit(-1)
    # Write files
    prefix_template = 'https://%s.s3.amazonaws.com/%s'
    payload = {'keyname': ARG.LIBRARY, 'count': 0, 'prefix': '',
               'subprefixes': dict()}
    order_file = list()
    for which in batch_dict['keys']:
        LOGGER.info("Processing %s imagery", which)
        prefix = '/'.join([ARG.TEMPLATE, ARG.LIBRARY])
        if which != 'default':
            prefix += '/' + which
            payload['subprefixes'][which] = {'count': batch_dict['count'][which],
                                             'prefix': prefix_template % (ARG.BUCKET, prefix)}
            if which in DISTRIBUTE_FILES:
                payload['subprefixes'][which]['batch_size'] = batch_dict['size'][which]
                payload['subprefixes'][which]['num_batches'] = batch_dict['max_batch'][which]
                print(which)
                print("  Batch size: %d" % (batch_dict['size'][which]))
                print("  Batches:    %d" % (batch_dict['max_batch'][which]))
        else:
            payload['count'] = batch_dict['count'][which]
            payload['prefix'] = prefix_template % (ARG.BUCKET, prefix)
        object_name = '/'.join([prefix, KEYFILE])
        print("%s objects: %d" % (which, batch_dict['count'][which]))
        random.shuffle(batch_dict['keys'][which])
        if which in DISTRIBUTE_FILES:
            order_file.append(write_order_file(which, json.dumps(batch_dict['keys'][which],
                                                                 indent=4), prefix))
        upload_to_aws(s3_resource, json.dumps(batch_dict['keys'][which], indent=4), object_name)
        object_name = '/'.join([prefix, COUNTFILE])
        LOGGER.info("Uploading %s count file (%d)", which, batch_dict['count'][which])
        upload_to_aws(s3_resource, json.dumps({"objectCount": batch_dict['count'][which]},
                                              indent=4), object_name)
    if ARG.WRITE:
        LOGGER.info("Updating DynamoDB")
        dynamodb = boto3.resource('dynamodb')
        table = 'janelia-neuronbridge-denormalization-%s' % (ARG.MANIFOLD)
        table = dynamodb.Table(table)
        table.put_item(Item=payload)
    if order_file and ARG.WRITE:
        print("Order files must be processed with s3cp to upload the key file to S3:")
        for order in order_file:
            print("  python3 s3cp.py --order " + order)
        print("s3cp is available at https://github.com/JaneliaSciComp/freight")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Produce denormalization files")
    PARSER.add_argument('--bucket', dest='BUCKET', action='store',
                        default='janelia-flylight-color-depth', help='AWS S3 bucket')
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        help='Template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='Library')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='S3 manifold')
    PARSER.add_argument('--exclusion', dest='EXCLUSION', action='store',
                        help='Exclusion file')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write mode (write to bucket)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    REST = create_config_object("rest_services")
    AWS = create_config_object("aws")
    initialize_program()
    # Exclusions
    EXCLUSION = dict()
    if ARG.EXCLUSION:
        with open(ARG.EXCLUSION) as inf:
            lines = inf.read().splitlines()
        EXCLUSION = {key: True for key in lines}
        print("Loaded %d exclusions" % (len(EXCLUSION)))
    denormalize()
