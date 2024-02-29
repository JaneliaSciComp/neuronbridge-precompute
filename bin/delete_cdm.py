''' delete_cdm.py
    This program will delete a slide code from NeuronBridge
'''

import argparse
from operator import attrgetter
import sys
from types import SimpleNamespace
import boto3
from boto3.dynamodb.conditions import Key
import botocore
from botocore.exceptions import ClientError
import inquirer
import MySQLdb
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import jrc_common.jrc_common as JRC
import neuronbridge_lib as NB

# pylint: disable=broad-exception-caught,eval-used,logging-fstring-interpolation

# AWS
S3 = {}
S3_SECONDS = 60 * 60 * 12
# Database
DB = {}
DDBASE = "janelia-neuronbridge"
READ = {"LINE": "SELECT DISTINCT line FROM image_data_mv WHERE slide_code=%s",
        "PFALLBACK": "SELECT publishing_name FROM publishing_name_vw WHERE line=%s "
                     + "AND display_genotype=0 AND preferred_name=1",
        "PNAME": "SELECT publishing_name FROM publishing_name_vw WHERE "
                 + "display_genotype=0 AND line=%s",
        "RELEASES": "SELECT DISTINCT published_to,alps_release FROM image_data_mv WHERE "
                    + "published_to IS NOT NULL AND publishing_name=%s AND slide_code!=%s",
       }
# Configuration
MANIFOLDS = ['dev', 'prod', 'devpre', 'prodpre']
# Targets
OBJECTIVE = ['20x', '40x', '63x']
TARGET = {"s3-cdm": [], "s3-sn-tif": [], "s3-sn-png": [], "s3-thumbnail": [],
          "neuronMetadata": [], "publishedLMImage": [],
          "publishedURL": [], "published-stacks": [], "publishing-doi": []}
AREA = {"s3-cdm": "AWS S3 color depth", "s3-sn-tif": "AWS S3 searchable neuron TIFFs",
        "s3-sn-png": "AWS S3 searchable neuron PNGs",
        "s3-thumbnail": "AWS S3 color depth thumbnails",
        "neuronMetadata": "MongoDB NeuronBridge neuronmetadata",
        "publishedLMImage": "MongoDB NeuronBridge publishedLMImage",
        "publishedURL": "MongoDB NeuronBridge publishedURL",
        "published-stacks": f"DynamoDB {DDBASE}-published-stacks",
        "publishing-doi": f"DynamoDB {DDBASE}-publishing-doi"}
COUNT = {}
NEURON_TO_DELETE = ('neuronType', 'neuronInstance')
OTHER = {}
# Output file
LINES = []


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


def initialize_aws():
    """ Initialize S3 and DynamoDB connections
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
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        DB['DYNAMOCLIENT'] = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    for tname in ('published-stacks', 'publishing-doi'):
        fullname = f"{DDBASE}-{tname}"
        LOGGER.info(f"Connecting to {fullname}")
        DB[tname] = dynamodb.Table(fullname)
    DB['DYNAMO'] = dynamodb


def is_light(pname):
    """ Determine if the provided publishing name is for a light image
        Keyword arguments:
          pname: publishing name
        Returns:
          True if light, False otherwise
    """
    return not bool(pname.isnumeric())


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for dbname in ('sage', 'neuronbridge'):
        rwp = 'write' if dbname == 'neuronbridge' and ARG.WRITE else 'read'
        dbo = attrgetter(f"{dbname}.prod.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
        DB[dbname if dbname == 'sage' else 'NB'] = JRC.connect_database(dbo)
    initialize_aws()
    if not ARG.TEMPLATE:
        ARG.TEMPLATE = NB.get_template(S3['client'], ARG.BUCKET)
    if not ARG.TEMPLATE:
        terminate_program("No template was selected")
    if not ARG.LIBRARY:
        ARG.LIBRARY = NB.get_library(source='aws', client=S3['client'], bucket=ARG.BUCKET,
                                     template=ARG.TEMPLATE, exclude='FlyEM' \
                                     if is_light(ARG.ITEM) else 'FlyLight')

    if not ARG.LIBRARY:
        terminate_program("No library was selected")


def get_sage_info():
    """ Get the line and publishing name from the slide code. Also determine if any
        images from this line (but not this slide code) are published.
        Keyword arguments:
          None
        Returns:
          line: line
          pname: publishing name
    """
    try:
        DB['sage']['cursor'].execute(READ['LINE'], (ARG.ITEM,))
        row = DB['sage']['cursor'].fetchone()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    line = row['line']
    try:
        DB['sage']['cursor'].execute(READ['PNAME'], (line,))
        rows = DB['sage']['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    pname = rows[0]['publishing_name']
    if not pname:
        try:
            DB['sage']['cursor'].execute(READ['PFALLBACK'], (line,))
            rows = DB['sage']['cursor'].fetchall()
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        if not rows:
            terminate_program(f"Not publishing name found for {line}")
        if len(rows) > 1:
            terminate_program(f"Multiple publishing names found for {line}")
        pname = rows[0]['publishing_name']
    print(f"Slide code {ARG.ITEM} is in line {line} ({pname})")
    try:
        DB['sage']['cursor'].execute(READ['RELEASES'], (pname, ARG.ITEM))
        rows = DB['sage']['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    TARGET['sage'] = bool(rows)
    if TARGET['sage']:
        LOGGER.warning(f"{pname} is still published for other slide codes")
    return line, pname


def check_for_thumbnail(obj):
    """ Find thumbnails to delete.
        Keyword arguments:
          obj: object name
        Returns:
          None
    """
    fname = obj.replace(".png", ".jpg")
    try:
        S3['client'].head_object(Bucket=ARG.BUCKET + '-thumbnails', Key=fname)
        TARGET['s3-thumbnail'].append(fname)
        return
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            return
        terminate_program(err)
    except Exception as err:
        terminate_program(err)


def check_manifest():
    """ Find files to delete from manifest
        Keyword arguments:
          None
        Returns:
          None
    """
    fname = f"{ARG.BUCKET}_manifest.txt"
    LOGGER.info(f"Loading manifest for {ARG.BUCKET}")
    with open(fname, 'r', encoding='ascii') as infile:
        manifest = [x.strip() for x in infile.readlines()]
    LOGGER.info(f"Found {len(manifest):,} entries in manifest")
    base = f"{ARG.TEMPLATE}/{ARG.LIBRARY}"
    flylight = is_light(ARG.ITEM)
    searchkey = f"-{ARG.ITEM}-" if flylight else f"{ARG.ITEM}-{ARG.TEMPLATE}-"
    obj_cnt = 0
    for obj in tqdm(manifest, desc='Checking manifest'):
        if obj.startswith(base) and (searchkey in obj or \
                                     ((not flylight) and obj.endswith(f"{ARG.ITEM}.swc"))):
            tname = 's3-cdm'
            if 'searchable' in obj:
                tname = 's3-sn-png' if obj.endswith('.png') else 's3-sn-tif'
            TARGET[tname].append(obj)
            obj_cnt += 1
            check_for_thumbnail(obj)
    LOGGER.info(f"Objects found: {obj_cnt:,}")
    LOGGER.info(f"Thumbnail objects found: {len(TARGET['s3-thumbnail']):,}")


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


def get_library_name():
    """ Given a AWS library name, return the MongoDB library name
        Keyword arguments:
          None
        Returns:
          MongoDB library name
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
    return libname

# ********************************************************************************
# * Routines for checking areas                                                  *
# ********************************************************************************

def get_version():
    """ Allow the user to select a NeuronBridge data version
        Keyword arguments:
          None
        Returns:
          None
    """
    tbl = 'published'
    ddbr = boto3.resource("dynamodb")
    dtables = list(ddbr.tables.all())
    choices = []
    for ddbtbl in dtables:
        if ddbtbl.name.startswith(f"{DDBASE}-{tbl}-v"):
            choices.append(ddbtbl.name.replace(f"{DDBASE}-{tbl}-", ""))
    if not choices:
        terminate_program(f"No {DDBASE}-{tbl} versions found")
    print("Select the NeuronBridge data version:")
    terminal_menu = TerminalMenu(choices)
    answer = terminal_menu.show()
    if not answer:
        terminate_program("No version was selected")
    ARG.VERSION = choices[answer]


def check_neuronmetadata_lm():
    """ Check for images with a template/slide code in the neuronMetadata collection
        Keyword arguments:
          None
        Returns:
          None
    """
    if not ARG.VERSION:
        get_version()
    tag = ARG.VERSION.replace("v", "")
    coll = DB['NB']['neuronMetadata']
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "slideCode": ARG.ITEM,
               "tags": tag}
    LOGGER.info(f"Searching neuronMetadata for {ARG.TEMPLATE} {ARG.ITEM} {tag}")
    try:
        results = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in results:
        TARGET['neuronMetadata'].append(row['_id'])
    LOGGER.info(f"neuronMetadata records found: {len(TARGET['neuronMetadata']):,}")


def check_publishedlmimage():
    """ Check for images with a template/slide code in the publishedLMImage collection
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DB['NB']['publishedLMImage']
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "slideCode": ARG.ITEM}
    LOGGER.info(f"Searching publishedLMImage for {ARG.TEMPLATE} {ARG.ITEM}")
    try:
        results = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    for row in results:
        TARGET['publishedLMImage'].append(row['_id'])
    LOGGER.info(f"publishedLMImage records found: {len(TARGET['publishedLMImage']):,}")


def full_body_id(libname=None):
    """ Return a fully-qualified body ID
        Keyword arguments:
          libname: library (optional)
        Returns:
          None
    """
    if not libname:
        libname = get_library_name()
    prefix = libname.replace('flyem_', '')
    prefix, version = prefix.split('_', 1)
    return ":".join([prefix, 'v' + version.replace('_', '.'), ARG.ITEM])


def check_publishedurl():
    """ Check for images with a template/library/slide code in the publishedURL collection
        Keyword arguments:
          None
        Returns:
          None
    """
    libname = get_library_name()
    coll = DB['NB']['publishedURL']
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "libraryName": libname}
    if is_light(ARG.ITEM):
        payload['slideCode'] = searchkey = ARG.ITEM
    else:
        searchkey = full_body_id(libname)
        payload['publishedName'] = searchkey
    LOGGER.info(f"Searching publishedURL for {ARG.TEMPLATE}/{libname} {searchkey}")
    try:
        results = coll.find(payload)
    except Exception as err:
        terminate_program(err)
    uploaded = []
    for row in results:
        TARGET['publishedURL'].append(row['_id'])
        for ufile in row['uploaded'].values():
            uploaded.append(ufile)
    on_s3 = len(TARGET['s3-cdm']) + len(TARGET['s3-sn-tif']) + len(TARGET['s3-sn-png']) \
            + len(TARGET['s3-thumbnail'])
    if uploaded and (len(uploaded)+1 != on_s3):
        LOGGER.warning(f"Mismatch between uploaded files ({len(uploaded)}) " \
                       + f"and files found on AWS S3 ({on_s3})")
    LOGGER.info(f"publishedURL records found: {len(TARGET['publishedURL']):,}")


def check_neuronmetadata_em():
    """ Check for images with a template/library/body ID in the neuronMetadata collection
        Keyword arguments:
          None
        Returns:
          None
    """
    libname = get_library_name()
    coll = DB['NB']['neuronMetadata']
    payload = {"alignmentSpace": ARG.TEMPLATE,
               "libraryName": libname}
    payload['publishedName'] = ARG.ITEM
    LOGGER.info(f"Searching neuronMetadata for {ARG.TEMPLATE}/{libname} {ARG.ITEM}")
    try:
        row = coll.find_one(payload)
    except Exception as err:
        terminate_program(err)
    for col in NEURON_TO_DELETE:
        if row[col]:
            OTHER[col] = row[col]


def check_published_stacks():
    """ Check for records in the janelia-neuronbridge-published-stacks table. These
        are keyed by slide code/objective/template.
        Keyword arguments:
          None
        Returns:
          None
    """
    tbl = 'published-stacks'
    for obj in OBJECTIVE:
        key = f"{ARG.ITEM.lower()}-{obj}-{ARG.TEMPLATE.lower()}"
        LOGGER.info(f"Searching {DDBASE}-{tbl} for {key}")
        response = DB[tbl].query(KeyConditionExpression=Key('itemType').eq(key))
        if 'Count' not in response or not response['Count']:
            continue
        if response['Items'][0]['alignmentSpace'] != ARG.TEMPLATE:
            print(ARG.TEMPLATE, response['alignmentSpace'])
            continue
        TARGET[tbl].append(key)
    LOGGER.info(f"{tbl} records found: {len(TARGET[tbl]):,}")


def check_published(pname):
    """ Check for records in the janelia-neuronbridge-published-[version] table. These
        are keyed by publishing name.
        Keyword arguments:
          pname: publishing name
        Returns:
          None
    """
    tbl = 'published'
    if not ARG.VERSION:
        get_version()
    TARGET[f"{tbl}-{ARG.VERSION}"] = []
    tbl += f"-{ARG.VERSION}"
    key = pname.lower()
    fullname = f"{DDBASE}-{tbl}"
    DB[tbl] = DB['DYNAMO'].Table(fullname)
    LOGGER.info(f"Searching {fullname} for {key}")
    try:
        response = DB[tbl].query(KeyConditionExpression= \
                                 Key('itemType').eq('searchString') & Key('searchKey').eq(key))
    except DB['DYNAMOCLIENT'].exceptions.ResourceNotFoundException:
        terminate_program(f"DynamoDB table {tbl} does not exist")
    except Exception as err:
        terminate_program(err)
    if 'Count' not in response or not response['Count']:
        return
    if 'Items' in response and response['Items'][0]:
        TARGET[tbl].append(key)
    LOGGER.info(f"{tbl} records found: {len(TARGET[tbl]):,}")


def check_publishing_doi(pname):
    """ Check for records in the janelia-neuronbridge-publishing-doi table. These
        are keyed by publishing name.
        Keyword arguments:
          pname: publishing name
        Returns:
          None
    """
    tbl = 'publishing-doi'
    key = pname if is_light(pname) else full_body_id()
    LOGGER.info(f"Searching {DDBASE}-{tbl} for {key}")
    response = DB[tbl].query(KeyConditionExpression=Key('name').eq(key))
    if 'Count' not in response or not response['Count']:
        return
    if 'Items' in response and response['Items'][0]:
        TARGET[tbl].append(key)
    LOGGER.info(f"{tbl} records found: {len(TARGET[tbl]):,}")


# ********************************************************************************
# * Routines for deleting items                                                  *
# ********************************************************************************

def s3_cdm(area):
    """ Delete objects from the janelia-flylight-color-depth bucket
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    for key in TARGET[area]:
        LOGGER.debug(f"Deleting {key}")
        try:
            obj = S3['resource'].Object(ARG.BUCKET, key)
            if ARG.WRITE:
                response = obj.delete()
                if response['ResponseMetadata']['HTTPStatusCode'] in (200, 204):
                    COUNT[AREA[area]] += 1
                    LINES.append(f"{ARG.BUCKET}/{key}")
            else:
                try:
                    response = obj.get()
                    COUNT[AREA[area]] += 1
                    LINES.append(f"{ARG.BUCKET}/{key}")
                except Exception as err:
                    if type(err).__name__ != 'NoSuchKey':
                        LOGGER.warning(key)
                        LOGGER.warning(err)
        except Exception as err:
            terminate_program(err)


def s3_thumbnail(area):
    """ Delete objects from the janelia-flylight-color-depth-thumbnails bucket
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    for key in TARGET['s3-thumbnail']:
        LOGGER.debug(f"Deleting {key}")
        try:
            obj = S3['resource'].Object(ARG.BUCKET + '-thumbnails', key)
            if ARG.WRITE:
                response = obj.delete()
                if response['ResponseMetadata']['HTTPStatusCode'] in (200, 204):
                    COUNT[AREA[area]] += 1
                    LINES.append(f"{ARG.BUCKET}-thumbnails/{key}")
            else:
                try:
                    response = obj.get()
                    COUNT[AREA[area]] += 1
                    LINES.append(f"{ARG.BUCKET}-thumbnails/{key}")
                except Exception as err:
                    if type(err).__name__ != 'NoSuchKey':
                        LOGGER.warning(key)
                        LOGGER.warning(err)
        except Exception as err:
            terminate_program(err)


def publishedurl(area):
    """ Delete objects from the publishedURL MongoDB table
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    coll = DB['NB']['publishedURL']
    for key in TARGET[area]:
        LOGGER.debug(f"Deleting {key}")
        try:
            if ARG.WRITE:
                _ = coll.delete_one({"_id": key})
                COUNT[AREA[area]] += 1
            else:
                COUNT[AREA[area]] += 1
            LINES.append(f"publishedURL {key}")
        except Exception as err:
            terminate_program(err)


def publishedlmimage(area):
    """ Delete objects from the publishedLMImage MongoDB table
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    coll = DB['NB']['publishedLMImage']
    for key in TARGET[area]:
        LOGGER.debug(f"Deleting {key}")
        try:
            if ARG.WRITE:
                _ = coll.delete_one({"_id": key})
                COUNT[AREA[area]] += 1
            else:
                COUNT[AREA[area]] += 1
            LINES.append(f"publishedLMImage {key}")
        except Exception as err:
            terminate_program(err)


def neuronmetadata(area):
    """ Untag objects from the neuronMetadata MongoDB table
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    coll = DB['NB']['neuronMetadata']
    tag = ARG.VERSION.replace("v", "")
    for key in TARGET[area]:
        LOGGER.debug(f"Updating tags for {key}")
        try:
            row = coll.find_one({"_id": key})
            newlist = row['tags']
            newlist.remove(tag)
            if ARG.WRITE:
                payload = { "$set": { 'tags': newlist} }
                result = coll.update_one({"_id": row['_id']}, payload)
                if result.modified_count:
                    COUNT[AREA[area]] += 1
                else:
                    LOGGER.error("Could not update %s in neuronMetadata", row['_id'])
            else:
                COUNT[AREA[area]] += 1
            LINES.append(f"neuronMetadata {key}")
        except Exception as err:
            terminate_program(err)


def remove_from_bidlist(area, tbl, keytype, bkey):
    """ Remove a body ID from the list stored with neuron types
        Keyword arguments:
          area: deletion area
          tbl: DynamoDB table object
          keytype: key type
          bkey: body ID
        Returns:
          None
    """
    nkey = OTHER[keytype].lower()
    try:
        response = tbl.query(KeyConditionExpression= \
                             Key('itemType').eq('searchString') & Key('searchKey').eq(nkey))
        if response:
            payload = response['Items'][0]
            new_bids = []
            bids = payload['bodyIDs']
            for bid in bids:
                if list(bid.keys())[0] != bkey:
                    new_bids.append(bid)
            if len(new_bids) == len(bids):
                return
            LOGGER.debug(f"Updating {nkey}")
            payload['bodyIDs'] = new_bids
            if ARG.WRITE:
                try:
                    response = tbl.put_item(Item=payload)
                    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        COUNT[AREA[area]] += 1
                        LINES.append(f"{area} {nkey}")
                except ClientError:
                    terminate_program("Couldn't update {nkey} in {area}: " \
                                      + f"{response['Error']['Message']}")
                except Exception as err:
                    terminate_program(err)
            else:
                COUNT[AREA[area]] += 1
                LINES.append(f"{area} {nkey}")
        else:
            LOGGER.warning(f"{keytype} {nkey} not found in DynamoDB published table")
    except Exception as err:
        terminate_program(err)


def bump_dynamo_counter(response, area):
    """ Bump area counter based on DynamoDB delete (or get) response
        Keyword arguments:
          response: DynamoDB delete (or get) response
          area: deletion area
        Returns:
          None
    """
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        if ARG.WRITE:
            COUNT[AREA[area]] += 1
        elif response['Count'] == 1:
            COUNT[AREA[area]] += 1


def delete_from_published(area):
    """ Delete objects from the janelia-neuronbridge-published-stacks or
        janelia-neuronbridge-published-[version] table
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    tbl = DB[area]
    for key in TARGET[area]:
        LOGGER.debug(f"Deleting {key}")
        try:
            if area == 'published-stacks':
                if ARG.WRITE:
                    response = tbl.delete_item(Key={"itemType": key})
                else:
                    response = tbl.query(KeyConditionExpression=Key("itemType").eq(key))
            else:
                if ARG.WRITE:
                    response = tbl.delete_item(Key={"itemType": 'searchString', "searchKey": key})
                else:
                    response = tbl.query(KeyConditionExpression=Key("itemType").eq("searchString") \
                                                                    & Key("searchKey").eq(key)
                                        )
            LINES.append(f"{area} {key}")
            bump_dynamo_counter(response, area)
        except ClientError as err:
            LOGGER.error(err)
            terminate_program("Couldn't delete {key} from {area}: {response['Error']['Message']}")
        except Exception as err:
            terminate_program(err)
        if not is_light(key):
            for ktype in ('neuronType', 'neuronInstance'):
                if ktype in OTHER and OTHER[ktype]:
                    remove_from_bidlist(area, tbl, ktype, key)


def publishing_doi(area):
    """ Delete objects from the janelia-neuronbridge-publishing-doi table
        Keyword arguments:
          area: deletion area
        Returns:
          None
    """
    tbl = DB[area]
    for key in TARGET[area]:
        LOGGER.debug(f"Deleting {key}")
        try:
            if ARG.WRITE:
                response = tbl.delete_item(Key={"name": key})
            else:
                response = tbl.query(KeyConditionExpression=Key("name").eq(key))
            LINES.append(f"{area} {key}")
            bump_dynamo_counter(response, area)
        except ClientError as err:
            LOGGER.error(err)
            terminate_program("Couldn't delete {key} from {area}: {response['Error']['Message']}")
        except Exception as err:
            terminate_program(err)

# ********************************************************************************
# * Main processing                                                              *
# ********************************************************************************

def delete_items():
    """ Get user input on which items to delete, then delete them
        Keyword arguments:
          None
        Returns:
          None
    """
    if not TARGET['sage']:
        AREA[f"published-{ARG.VERSION}"] = f"DynamoDB {DDBASE}-published-{ARG.VERSION}"
    choices = []
    accepted = []
    for key, val in AREA.items():
        if TARGET[key]:
            choices.append((f"{val}: {len(TARGET[key])} item(s)", key))
            accepted.append(key)
    if not choices:
        LOGGER.warning("There is nothing to delete")
        terminate_program()
    if ARG.ACCEPT:
        answers = {}
        answers['area'] = accepted
    else:
        question = [inquirer.Checkbox("area", message="Where should items be deleted from",
                                      choices=choices)]
        answers = inquirer.prompt(question)
    if not answers:
        terminate_program("Operation cancelled")
    if not answers['area']:
        LOGGER.warning("Will not delete any items")
        return
    for area in answers['area']:
        LOGGER.info(f"Deleting from {AREA[area]}")
        COUNT[AREA[area]] = 0
        if 'janelia-neuronbridge-published-' in AREA[area]:
            delete_from_published(area)
        elif area in ('s3-cdm', 's3-sn-tif', 's3-sn-png'):
            s3_cdm(area)
        else:
            eval(area.replace('-', '_').lower() + '(area)')


def process_slide():
    """ Process a single slide
        Keyword arguments:
          None
        Returns:
          None
    """
    if is_light(ARG.ITEM):
        _, publishing = get_sage_info()
    else:
        TARGET['sage'] = False
        publishing = ARG.ITEM
    # AWS S3
    check_manifest()
    # MongoDB
    if is_light(ARG.ITEM):
        check_neuronmetadata_lm()
        check_publishedlmimage()
    else:
        check_neuronmetadata_em()
    check_publishedurl()
    # DynamoDB
    if is_light(ARG.ITEM):
        check_published_stacks()
    if not TARGET['sage']:
        check_published(publishing)
        check_publishing_doi(publishing)
    delete_items()
    if LINES:
        LOGGER.info("Writing cdm_deletions.txt")
        with open("cdm_deletions.txt", "w", encoding="ascii") as outstream:
            for line in LINES:
                outstream.write(f"{line}\n")
    maxlen = 0
    for area in COUNT:
        if len(area) > maxlen:
            maxlen = len(area)
    if len(COUNT):
        print("Deletions/updates:" if ARG.WRITE else "Simulated deletions/updates:")
        for key, val in COUNT.items():
            print(f"{key+':':<{maxlen+1}} {val}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Delete a slide code from NeuronBridge")
    PARSER.add_argument('--bucket', dest='BUCKET', action='store',
                        default='janelia-flylight-color-depth', help='AWS S3 bucket')
    PARSER.add_argument('--template', dest='TEMPLATE', action='store',
                        default='', help='Alignment template')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='Color depth library')
    PARSER.add_argument('--item', dest='ITEM', action='store',
                        required=True, help='Slide code')
    PARSER.add_argument('--version', dest='VERSION',
                        help='DynamoDB NeuronBridge version')
    PARSER.add_argument('--accept', dest='ACCEPT', action='store_true',
                        default=False, help='Accept all deletion choices')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=MANIFOLDS, help='S3 manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Perform deletions')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_slide()
    terminate_program()
