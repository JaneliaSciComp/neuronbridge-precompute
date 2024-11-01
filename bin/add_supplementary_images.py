""" add_supplementary_images.py
    Add supplementary images to publishedURL and create an order file
    *****************************************************************
                                WARNING
    This program was written for one specific use - to add skeletons
    for FlyWire. It is generalized to a point where it should work
    with other libraries and image products, but code will need to be
    changed to handle these cases.
    *****************************************************************
"""

__version__ = '1.0.0'

import argparse
import collections
import json
from operator import attrgetter
import pathlib
import sys
import boto3
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# AWS
S3 = {}
S3_SECONDS = 60 * 60 * 12
# Prefix/key mapping
IGNORE = ('InputColorDepthImage', 'SourceColorDepthImage')
MAP = {'GradientImage': {'key': 'GradientImage',
                         'prefix': 'GradientImage'},
       'SkeletonOBJ': {'key': 'skeletonobj',
                       'prefix': 'OBJ'},
       'SkeletonSWC': {'key': 'skeletonswc',
                       'prefix': 'SWC'},
       'ZGapImage': {'key': 'ZGapImage',
                     'prefix': 'ZGapImage'},
      }
# Counters
COUNT = collections.defaultdict(lambda: 0, {})


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
    ''' Intialize S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        aws = JRC.get_config("aws")
    except Exception as err:
        terminate_program(err)
    LOGGER.info("Opening S3 client and resource")
    if "dev" in ARG.MANIFOLD:
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
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
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
    # Get the alignment space and library
    if not ARG.ALIGNMENT:
        ARG.ALIGNMENT = NB.get_template(S3["client"], 'janelia-flylight-color-depth')
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(source='mongo', coll=DB['neuronbridge'].publishedURL,
                                     template=ARG.ALIGNMENT)
    if not ARG.LIBSUB:
        ARG.LIBSUB = NB.get_library(source='aws', bucket='janelia-flylight-color-depth',
                                    client=S3["client"], template=ARG.ALIGNMENT)


def get_product_list():
    ''' Get image product list
        Keyword arguments:
          None
        Returns:
          List of image products
    '''
    payload = [{"$match": {"libraryName": ARG.LIBRARY}},
               {"$project":{"arrayofkeyvalue":{"$objectToArray":"$computeFiles"}}},
               {"$project":{"keys":"$arrayofkeyvalue.k"}}]
    try:
        rows = DB['neuronbridge'].neuronMetadata.aggregate(payload)
    except Exception as err:
        terminate_program(err)
    products = {}
    for row in rows:
        for prod in row['keys']:
            if prod not in IGNORE and prod not in products:
                products[prod] = True
    return list(sorted(products.keys()))


def get_images_to_add():
    ''' Get images to add
        Keyword arguments:
          None
        Returns:
          dict of images (key=_id, value=file path)
    '''
    product = get_product_list()
    print("Select an image product to add")
    terminal_menu = TerminalMenu(product)
    chosen = terminal_menu.show()
    if chosen is None:
        terminate_program("No product chosen")
    LOGGER.info(f"Selecting files for {product[chosen]}")
    payload = {"libraryName": ARG.LIBRARY,
               "alignmentSpace": ARG.ALIGNMENT,
               f"computeFiles.{product[chosen]}": {"$exists": True}
              }
    try:
        rows = DB['neuronbridge'].neuronMetadata.find(payload,
                                                      {"publishedName": 1, "computeFiles": 1})
    except Exception as err:
        terminate_program(err)
    image = {}
    for row in rows:
        COUNT['images'] += 1
        image[row['_id']] = {"name": row['publishedName'],
                             "files": row['computeFiles'][product[chosen]]}
    return product[chosen], image


def get_new_path(fpath, product, name):
    ''' Get new path
        Keyword arguments:
          fpath: file path from neuronMetadata
          product: image product
        Returns:
          New file path
    '''
    ext = pathlib.Path(fpath).suffix
    newpath = '/'.join(["janelia-flylight-color-depth",
                        ARG.ALIGNMENT, ARG.LIBSUB, MAP[product]['prefix'], (name + ext)])
    return newpath


def add_images(product, image, existing):
    ''' Get new path
        Keyword arguments:
          product: image product
          image: dictionary of images (key=_id, value=file path)
          existing: dictionary of existing records (key=_id, value=computeFiles)
        Returns:
          None
    '''
    order = []
    coll = DB['neuronbridge'].publishedURL
    for iid, rec in tqdm(image.items()):
        fpath = rec['files']
        if iid not in existing:
            terminate_program(f"Image {iid} not found in publishedURL")
        pay = existing[iid]
        newpath = get_new_path(fpath, product, rec['name'])
        pay[MAP[product]['key']] = f"https://s3.amazonaws.com/{newpath}"
        order.append(f"{fpath}\t{newpath}\n")
        LOGGER.debug(json.dumps(pay, indent=2))
        payload = {"$set": {"uploaded": pay}}
        if ARG.WRITE:
            try:
                result = coll.update_one({"_id": iid}, payload)
            except Exception as err:
                terminate_program(err)
            if result.modified_count:
                COUNT['updated'] += result.modified_count
    COUNT['order'] = len(order)
    with open("order.txt", "w", encoding="ascii") as ofile:
        ofile.writelines(order)


def processing():
    ''' Main processing routine
        Keyword arguments:
          None
        Returns:
          None
    '''
    product, image = get_images_to_add()
    LOGGER.info(f"Found {len(image):,} images to add")
    payload = {"libraryName": ARG.LIBRARY,
               "alignmentSpace": ARG.ALIGNMENT
              }
    LOGGER.info("Getting existing records from publishedURL")
    existing = {}
    try:
        rows = DB['neuronbridge'].publishedURL.find(payload, {"uploaded": 1})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        COUNT['existing'] += 1
        existing[row['_id']] = row['uploaded']
    LOGGER.info(f"Found {len(existing):,} records in publishedURL")
    add_images(product, image, existing)
    print(f"Records found in neuronMetadata: {COUNT['images']:,}")
    print(f"Records found in publishedURL:   {COUNT['existing']:,}")
    print(f"Images written to order file   : {COUNT['order']:,}")
    print(f"Records updated in publishedURL: {COUNT['updated']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Template program")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--subprefix', dest='LIBSUB', action='store',
                        default='', help='AWS S3 library sub prefix')
    PARSER.add_argument('--ALIGNMENT', dest='ALIGNMENT', action='store',
                        help='alignment space')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
