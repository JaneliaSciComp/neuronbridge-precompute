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
    The DynamoDB table janelia-neuronbridge-denormalization-<manifold> is also
    populated with counts and prefixes.
'''

import argparse
import json
from operator import attrgetter
import random
import sys
import tempfile
import boto3
from botocore.exceptions import ClientError
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB

__version__ = '2.0.0'
# Configuration
KEYFILE = "keys_denormalized.json"
COUNTFILE = "counts_denormalized.json"
DISTRIBUTE_FILES = ['searchable_neurons']
TAGS = f"PROJECT=CDCS&STAGE=prod&DEVELOPER=svirskasr&VERSION={(__version__)}"
AWSS3 = {"client": None, "resource": 0}
COUNT = {"skipped": 0}
# Database
DBM = {}

# pylint: disable=W0718

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
    if not ARG.LIBRARY:
        bucket = 'janelia-flylight-color-depth'
        if ARG.MANIFOLD != 'prod':
            bucket += f"-{ARG.MANIFOLD}"
        ARG.LIBRARY = NB.get_library(source='aws', client=AWSS3["client"], bucket=bucket,
                                     template=ARG.TEMPLATE)
        if not ARG.LIBRARY:
            terminate_program("No library selected")
    if not ARG.VERSION:
        coll = DBM['neuronbridge'].cdmLibraryStatus
        results = coll.distinct("neuronbridge", {"manifold": ARG.MANIFOLD})
        ARG.VERSION = results[-1].replace(".", "_")
    if not ARG.VERSION:
        terminate_program("No NeuronBridge data version was found")
    print(f"Template:                  {ARG.TEMPLATE}")
    print(f"Library:                   {ARG.LIBRARY}")
    print(f"NeuronBridge data version: {ARG.VERSION}")
    print("Do you want to proceed?")
    allowed = ['No', 'Yes']
    terminal_menu = TerminalMenu(allowed)
    chosen = terminal_menu.show()
    if chosen is None or allowed[chosen] != "Yes":
        terminate_program()


def initialize_program():
    """ Initialize
    """
    random.seed()
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    dbo = attrgetter("neuronbridge.prod.read")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DBM['neuronbridge'] = JRC.connect_database(dbo)
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    initialize_s3()
    get_parms()


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
    source_file = f"{fname}_{which}.txt"
    LOGGER.info("Writing temporary file %s", source_file)
    with open(source_file, "w", encoding="ascii") as tfile:
        tfile.write(body)
    order_file = source_file.replace('.txt', '.order')
    LOGGER.info("Writing order file %s", order_file)
    with open(order_file, "w", encoding="ascii") as ofile:
        for chunk in range(100):
            dest = '/'.join([ARG.BUCKET, prefix, 'KEYS', str(chunk), 'keys_denormalized.json'])
            ofile.write(f"{source_file}\t{dest}\n")
            if which == "searchable_neurons":
                dest = dest.replace("json", ARG.VERSION)
                ofile.write(f"{source_file}\t{dest}\n")
    return order_file


def get_batch_dict(prefix, first_batch, key_list, max_batch, total_objects):
    """ Return a batch dict for a prefix
        Keyword arguments:
          prefix: prefix
          first_batch: first batch number
          key_list: list of S3 keys
          max_batch: maximum batch number
          total_pbjects: object count
        Returns:
          batch_dict
    """
    batch_size = {}
    for which in DISTRIBUTE_FILES:
        batch_size[which] = 0
        objs = NB.get_all_s3_objects(AWSS3["client"], Bucket=ARG.BUCKET, Prefix=prefix + which + "/"
                                     + str(first_batch[which]) + "/")
        for _ in objs:
            batch_size[which] += 1
    batch_dict = {'count': total_objects,
                  'keys': key_list,
                  'size': batch_size,
                  'max_batch': max_batch}
    for which in key_list:
        print(which)
        print(f"  Total objects: {total_objects[which]:,}")
        print(f"  Total keys:    {len(key_list[which]):,}")
        if which in batch_size:
            print(f"  Batch size:    {batch_size[which]:,}")
            print(f"  Max batch:     {max_batch[which]}")
    print(f"Skipped objects: {COUNT['skipped']:,}")
    return batch_dict


def populate_batch_dict():
    """ Produce a dict with key/batch information
        Keyword arguments:
          None
        Returns:
          batch dictionary
    """
    prefix = '/'.join([ARG.TEMPLATE, ARG.LIBRARY]) + '/'
    LOGGER.info("Batching keys for %s", prefix)
    total_objects = {}
    key_list = {}
    max_batch = {}
    first_batch = {}
    for which in DISTRIBUTE_FILES:
        max_batch[which] = 0
        first_batch[which] = 0
    keys = []
    for obj in NB.get_all_s3_objects(AWSS3["client"], Bucket=ARG.BUCKET, Prefix=prefix):
        if KEYFILE in obj['Key'] or COUNTFILE in obj['Key'] or "pngs" in obj['Key'] \
           or obj['Key'].endswith("/"):
            continue
        keys.append(obj['Key'])
    for key in tqdm(keys, desc="Keys"):
        if "/KEYS/" in key:
            continue
        which = 'default'
        LOGGER.debug(key)
        splitkey = key.split('/')
        if len(splitkey) >= 4:
            which = splitkey[2]
        if which not in key_list:
            LOGGER.info("Adding key %s", which)
            key_list[which] = []
            total_objects[which] = 0
        # Skip here
        if which == 'searchable_neurons':
            fname = key.split("/")[-1]
            if fname in EXCLUSION:
                COUNT["skipped"] += 1
                continue
        total_objects[which] += 1
        key_list[which].append(key)
        if which in DISTRIBUTE_FILES:
            try:
                num = int(key.split("/")[3])
            except Exception as err:
                LOGGER.error(f"Could not get number from {key}")
                terminate_program(err)
            if not first_batch[which]:
                first_batch[which] = num
            if num > max_batch[which]:
                max_batch[which] = num
    return get_batch_dict(prefix, first_batch, key_list, max_batch, total_objects)


def denormalize():
    """ Denormalize a bucket into a JSON file
        Keyword arguments:
          None
        Returns:
          None
    """
    #pylint: disable=no-member
    print(f"Processing {ARG.LIBRARY}/{ARG.TEMPLATE} on {ARG.MANIFOLD} manifold")
    batch_dict = populate_batch_dict()
    if not batch_dict['count'] or not batch_dict['count']['default']:
        terminate_program(f"{ARG.TEMPLATE}/{ARG.LIBRARY} was not found in the {ARG.BUCKET} bucket")
    # Write files
    prefix_template = 'https://%s.s3.amazonaws.com/%s'
    payload = {'keyname': ARG.LIBRARY, 'count': 0, 'prefix': '',
               'subprefixes': {}}
    order_file = []
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
                print(f"  Batch size: {batch_dict['size'][which]:,}")
                print(f"  Batches:    {batch_dict['max_batch'][which]}")
        else:
            payload['count'] = batch_dict['count'][which]
            payload['prefix'] = prefix_template % (ARG.BUCKET, prefix)
        object_name = '/'.join([prefix, KEYFILE])
        print(f"{which} objects: {batch_dict['count'][which]:,}")
        random.shuffle(batch_dict['keys'][which])
        if which in DISTRIBUTE_FILES:
            order_file.append(write_order_file(which, json.dumps(batch_dict['keys'][which],
                                                                 indent=4), prefix))
        upload_to_aws(AWSS3["resource"], json.dumps(batch_dict['keys'][which],
                                                    indent=4), object_name)
        object_name = '/'.join([prefix, COUNTFILE])
        LOGGER.info(f"Uploading {which} count file ({batch_dict['count'][which]:,})")
        upload_to_aws(AWSS3["resource"], json.dumps({"objectCount": batch_dict['count'][which]},
                                                    indent=4), object_name)
    if ARG.WRITE:
        LOGGER.info("Updating DynamoDB")
        dynamodb = boto3.resource('dynamodb')
        table = f"janelia-neuronbridge-denormalization-{ARG.MANIFOLD}"
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
                        default='prod', choices=['dev', 'devpre', 'prod', 'prodpre'],
                        help='S3 manifold')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        help='NeuronBridge data version')
    PARSER.add_argument('--exclusion', dest='EXCLUSION', action='store',
                        help='Exclusion file')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write mode (write to bucket)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    # Exclusions
    EXCLUSION = {}
    if ARG.EXCLUSION:
        with open(ARG.EXCLUSION, encoding="ascii") as inf:
            LINES = inf.read().splitlines()
        EXCLUSION = {key: True for key in LINES}
        print(f"Loaded {len(EXCLUSION)} exclusions")
    denormalize()
