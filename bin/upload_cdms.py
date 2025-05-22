''' This program will use JSON data to update neuronbridge.publishedURL and create
    an order file to upload imagery to AWS S3.
'''
__version__ = '2.4.0'

import argparse
import collections
from copy import deepcopy
from datetime import datetime
import json
from operator import attrgetter
import os
import re
import socket
import sys
from time import strftime
from types import SimpleNamespace
import boto3
from botocore.exceptions import ClientError
import inquirer
from pymongo import MongoClient
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import MySQLdb
from PIL import Image
import jrc_common.jrc_common as JRC
import neuronbridge_common.neuronbridge_common as NB


# Configuration
LIBRARY = {}
MANIFOLDS = ['dev', 'prod', 'devpre', 'prodpre']
REQUIRED_PRODUCTS = ['cdm', 'cdm_thumbnail']
WILL_LOAD = []
# Database
MONGODB = 'neuronbridge-mongo'
DBM = ""
CONN = {}
CURSOR = {}
# AWS
S3_CLIENT = S3_RESOURCE = ''
S3_SECONDS = 60 * 60 * 12
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Searchable neurons
SUBDIVISION = {'prefix': 1, 'counter': 0, 'limit': 100}
# File naming
REC = {'line': '', 'slide_code': '', 'gender': '', 'objective': '', 'area': ''}
# General use
CONF = {}
DRIVER = {} # Driver by line
FALLBACK = {}
KEY_LIST = []
MANIFEST = {}
NON_PUBLIC = {}
NO_RELEASE = {}
PNAME = {}
RELEASE = {}
RELPUB = {}
UPLOADED_NAME = {}
VARIANT_UPLOADS = {}


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if S3CP:
        ERR.close()
        S3CP.close()
        for fpath in [ERR_FILE, S3CP_FILE, JSONO_FILE, NAMES_FILE]:
            if os.path.exists(fpath) and not os.path.getsize(fpath):
                os.remove(fpath)
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint, payload='', authenticate=False):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
          payload: payload for POST requests
          authenticate: pass along token in header
        Returns:
          JSON response
    '''
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        if payload or authenticate:
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer " + os.environ['NEUPRINT_JWT']}
        if payload:
            headers['Accept'] = 'application/json'
            headers['host'] = socket.gethostname()
            req = requests.put(url, headers=headers, json=payload, timeout=10)
        else:
            if authenticate:
                req = requests.get(url, headers=headers, timeout=10)
            else:
                req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Could not get response from {url}: {req.text}")
    return False


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        terminate_program(f"MySQL error [{err.args[0]}]: {err.args[1]}")
    except IndexError:
        terminate_program(f"MySQL error: {err}")


def db_connect(dbd):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
    """
    LOGGER.info("Connecting to %s on %s ", dbd['name'], dbd['host'])
    try:
        conn = MySQLdb.connect(host=dbd['host'], user=dbd['user'],
                               passwd=dbd['password'], db=dbd['name'])
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        return conn, cursor
    except MySQLdb.Error as err:
        sql_error(err)


def initialize_s3():
    """ Initialize
    """
    global S3_CLIENT, S3_RESOURCE # pylint: disable=W0603
    LOGGER.info("Opening S3 client and resource")
    if "dev" in ARG.MANIFOLD:
        S3_CLIENT = boto3.client('s3')
        S3_RESOURCE = boto3.resource('s3')
    else:
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=AWS.role_arn,
                                     RoleSessionName="AssumeRoleSession1",
                                     DurationSeconds=S3_SECONDS)
        credentials = aro['Credentials']
        S3_CLIENT = boto3.client('s3',
                                 aws_access_key_id=credentials['AccessKeyId'],
                                 aws_secret_access_key=credentials['SecretAccessKey'],
                                 aws_session_token=credentials['SessionToken'])
        S3_RESOURCE = boto3.resource('s3',
                                     aws_access_key_id=credentials['AccessKeyId'],
                                     aws_secret_access_key=credentials['SecretAccessKey'],
                                     aws_session_token=credentials['SessionToken'])

def get_library():
    """ Query the user for the CDM library
        Keyword arguments:
            None
        Returns:
            None
    """
    coll = DBM.cdmLibraryStatus
    payload = [{"$group": {"_id": "$library",
                           "doc": {"$max": {"updateDate": "$updateDate",
                                            "library": "$library",
                                            "manifold": "$manifold"}}}},
               {"$replaceRoot": {"newRoot": "$doc"}}]
    result = coll.aggregate(payload)
    updated = {}
    for row in result:
        updated[row["library"]] = {"manifold": row["manifold"],
                                   "updateDate": row["updateDate"].strftime("%Y-%m-%d %H:%M:%S")}
    coll = DBM.neuronMetadata
    mongo_libs = coll.distinct("libraryName")
    print("Select a library:")
    cdmlist = []
    liblist = []
    for cdmlib in LIBRARY:
        if ARG.MANIFOLD not in LIBRARY[cdmlib]:
            LIBRARY[cdmlib][ARG.MANIFOLD] = {'updated': None}
        if cdmlib not in mongo_libs:
            continue
        liblist.append(cdmlib)
        text = cdmlib
        if cdmlib in updated:
            text += f" (updated {updated[cdmlib]['updateDate']} on " \
                    + f"{updated[cdmlib]['manifold']})"
        cdmlist.append(text)
    terminal_menu = TerminalMenu(cdmlist)
    chosen = terminal_menu.show()
    if chosen is None:
        terminate_program("No library selected")
    ARG.LIBRARY = liblist[chosen].replace(' ', '_')


def get_parms():
    """ Query the user for the CDM library, version, and JSON file
        Keyword arguments:
            None
        Returns:
            None
    """
    if not ARG.ALIGNMENT:
        ARG.ALIGNMENT = NB.get_template(S3_CLIENT, AWS.s3_bucket.cdm)
    if not ARG.LIBRARY:
        get_library()
    if not ARG.TAG:
        ARG.TAG = NB.get_neuronbridge_version(DBM.neuronMetadata, ARG.LIBRARY)
        if not ARG.TAG:
            terminate_program("No NeuronBridge version tag selected")


def get_flyem_dataset():
    """ Set FlyEM dataset and version
        Keyword arguments:
            None
        Returns:
            None
    """
    if ARG.DATASET:
        CONF['DATASET'] = ARG.DATASET
    else:
        which = "neuprint-pre" if ARG.NEUPRINT == "pre" else "neuprint"
        response = call_responder(which, 'dbmeta/datasets', {}, True)
        datasets = list(response.keys())
        for dset in datasets:
            if ARG.LIBRARY.endswith(dset.replace(":v", "_").replace(".", "_")):
                CONF['DATASET'] = dset
    if 'DATASET' not in CONF:
        terminate_program(f"Could not find NeuPrint dataset for {ARG.LIBRARY}")


def set_searchable_subdivision(smp):
    """ Set the first searchable_neurons subdivision
        Keyword arguments:
            smp: first sample from JSON file
        Returns:
            Alignment space
    """
    if "alignmentSpace" not in smp:
        terminate_program("Could not find alignment space in first sample")
    bucket = AWS.s3_bucket.cdm
    if ARG.INTERNAL:
        bucket += '-int'
    elif ARG.MANIFOLD != 'prod':
        bucket += '-' + ARG.MANIFOLD
    library = LIBRARY[ARG.LIBRARY]['name'].replace(' ', '_')
    prefix = "/".join([smp["alignmentSpace"], library, "searchable_neurons"])
    maxnum = 0
    for pag in S3_CLIENT.get_paginator("list_objects")\
                        .paginate(Bucket=bucket, Prefix=prefix+"/", Delimiter="/"):
        if "CommonPrefixes" not in pag:
            break
        for obj in pag["CommonPrefixes"]:
            num = obj["Prefix"].split("/")[-2]
            if num.isdigit() and int(num) > maxnum:
                maxnum = int(num)
    SUBDIVISION['prefix'] = maxnum + 1
    LOGGER.warning("Will upload searchable neurons starting with subdivision %s",
                   SUBDIVISION['prefix'])


def select_uploads():
    """ Query the user for which image types to upload
        Keyword arguments:
            None
        Returns:
            None
    """
    global WILL_LOAD, REQUIRED_PRODUCTS # pylint: disable=W0603
    choices = CLOAD.variants
    defaults = ["searchable_neurons"]
    if not ARG.LIBRARY.startswith('flylight'):
        choices.append("skeletons")
        defaults.append("skeletons")
    quest = [inquirer.Checkbox('checklist',
                               message='Select image types to upload',
                               choices=choices, default=defaults)]
    WILL_LOAD = inquirer.prompt(quest)['checklist']
    for var in CLOAD.variants:
        if var in WILL_LOAD and var != "skeletons":
            REQUIRED_PRODUCTS.append(var)


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
    global DBM, LIBRARY # pylint: disable=W0603
    LIBRARY = (call_responder('config', 'config/cdm_library'))["config"]
    for tok in ['JACS_JWT', 'NEUPRINT_JWT']:
        if tok not in os.environ:
            terminate_program(f"Missing token - set in {tok} environment variable")
        response = JRC.check_token(tok)
        if isinstance(response, str):
            terminate_program(response)
        elif tok == "JACS_JWT":
            CONF['FULL_NAME'] = response['payload']['full_name']
            LOGGER.info("Authenticated as %s", CONF['FULL_NAME'])
    if not ARG.MANIFOLD:
        print("Select an AWS S3 manifold")
        terminal_menu = TerminalMenu(MANIFOLDS)
        chosen = terminal_menu.show()
        if chosen is None:
            terminate_program("You must select a manifold")
        ARG.MANIFOLD = MANIFOLDS[chosen]
    # MongoDB
    rwp = 'write' if (ARG.WRITE or ARG.CONFIG) else 'read'
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    dbo = attrgetter(f"neuronbridge.{ARG.MONGO}.{rwp}")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DBM = JRC.connect_database(dbo)
    except Exception as err: # pylint: disable=broad-exception-caught
        terminate_program(err)
    # AWS S3
    initialize_s3()
    # Get parms
    get_parms()
    if ARG.LIBRARY not in LIBRARY:
        terminate_program(f"Unknown library {ARG.LIBRARY}")
    select_uploads()
    # Get non-public slide codes
    coll = DBM['lmRelease']
    results = coll.find({"public": False})
    non_public = [row['release'] for row in results]
    sql = "SELECT DISTINCT slide_code,alps_release FROM image_data_mv WHERE alps_release IN (%s)"
    sql = sql % ('"' + '","'.join(non_public) + '"',)
    LOGGER.info("Finding non-public images in SAGE")
    dbdata = (call_responder('config', 'config/db_config'))["config"]
    (CONN['sage'], CURSOR['sage']) = db_connect(dbdata['sage']['prod'])
    CURSOR['sage'].execute(sql)
    rows =  CURSOR['sage'].fetchall()
    for row in rows:
        NON_PUBLIC[row['slide_code']] = row['alps_release']
    LOGGER.info("%d slide codes in non-public releases: %s", len(rows), ", ".join(non_public))


def log_error(err_text):
    ''' Log an error and write to error output file
        Keyword arguments:
          err_text: error message
        Returns:
          None
    '''
    LOGGER.error(err_text)
    ERR.write(err_text + "\n")


def get_s3_names(bucket, newname):
    ''' Return an S3 bucket and prefixed object name
        Keyword arguments:
          bucket: base bucket
          newname: file to upload
        Returns:
          bucket and object name
    '''
    if ARG.INTERNAL:
        bucket += '-int'
    elif ARG.MANIFOLD != 'prod':
        bucket += '-' + ARG.MANIFOLD
    library = LIBRARY[ARG.LIBRARY]['name'].replace(' ', '_')
    if ARG.LIBRARY in CLOAD.version_required:
        library += '_v' + ARG.VERSION
    object_name = '/'.join([REC['alignment_space'], library, newname])
    return bucket, object_name


def already_uploaded(ukey):
    if ukey in MANIFEST:
        print(f"{ukey} in manifest")
    return True if ukey in MANIFEST else False



def upload_aws(bucket, dirpath, fname, newname, force=False):
    ''' Transfer a file to Amazon S3
        Keyword arguments:
          bucket: S3 bucket
          dirpath: source directory
          fname: file name
          newname: new file name
          force: force upload (regardless of AWS parm)
        Returns:
          url: intended URL
          skip: do not write or perform postprocessing
    '''
    complete_fpath = '/'.join([dirpath, fname])
    bucket, object_name = get_s3_names(bucket, newname)
    url = '/'.join([AWS.base_aws_url, bucket, object_name])
    url = url.replace(' ', '+')
    if object_name in UPLOADED_NAME:
        if complete_fpath != UPLOADED_NAME[object_name]:
            err_text = f"{object_name} was already uploaded from {UPLOADED_NAME[object_name]}, " \
                       + f"but is now being uploaded from {complete_fpath}"
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            COUNT['Duplicate objects'] += 1
            return False, False
        LOGGER.debug("Already uploaded %s", object_name)
        COUNT['Duplicate objects'] += 1
        return url, True
    COUNT['Files to upload'] += 1
    UPLOADED_NAME[object_name] = complete_fpath
    if "/searchable_neurons/" in object_name:
        KEY_LIST.append(object_name)
    if (not ARG.WRITE) and (not os.path.exists(complete_fpath)):
        terminate_program(f"File {complete_fpath} does not exist")
    LOGGER.debug("Uploading %s to S3 as %s", complete_fpath, object_name)
    if ARG.MANIFEST and already_uploaded(object_name):
        return
    S3CP.write(f"{complete_fpath}\t{'/'.join([bucket, object_name])}\n")
    if ARG.AWS:
        LOGGER.info("Upload %s", object_name)
    COUNT['Images processed'] += 1
    if (not ARG.AWS) and (not force):
        return url, False
    if not ARG.AWS:
        COUNT['Amazon S3 uploads'] += 1
        return url, False
    if newname.endswith('.png'):
        mimetype = 'image/png'
    elif newname.endswith('.jpg'):
        mimetype = 'image/jpeg'
    else:
        mimetype = 'image/tiff'
    try:
        payload = {'ContentType': mimetype}
        if ARG.MANIFOLD == 'prod':
            payload['ACL'] = 'public-read'
        S3_CLIENT.upload_file(complete_fpath, bucket,
                              object_name,
                              ExtraArgs=payload)
    except ClientError as err:
        LOGGER.critical(err)
        return False, False
    COUNT['Amazon S3 uploads'] += 1
    return url, False


def get_line_mapping(publishing_db):
    ''' Create a mapping of publishing names to drivers. Note that "GAL4-Collection"
        is remapped to "GAL4".
        Keyword arguments:
          publishing_db: publishing database
        Returns:
          None
    '''
    LOGGER.info("Getting line/driver mapping")
    stmt = "SELECT DISTINCT publishing_name,driver FROM image_data_mv " \
           + "WHERE publishing_name IS NOT NULL"
    if ARG.RELEASE and ARG.BACKCHECK:
        stmt += f" AND alps_release='{ARG.RELEASE}'"
    try:
        CURSOR[publishing_db].execute(stmt)
        rows = CURSOR[publishing_db].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        if row['driver']:
            DRIVER[row['publishing_name']] = \
                row['driver'].replace("_Collection", "").replace("-", "_")
    DRIVER['spGAL4 control'] = 'Split_GAL4'


def get_image_mapping(publishing_db):
    ''' Create a dictionary of published sample IDs and releases
        Keyword arguments:
          publishing_db: publishing database
        Returns:
          None
    '''
    LOGGER.info("Getting image mapping (sample -> release)")
    stmt = "SELECT DISTINCT workstation_sample_id,alps_release FROM image_data_mv WHERE " \
           + "alps_release IS NOT NULL"
    try:
        CURSOR['sage'].execute(stmt)
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        FALLBACK[row['workstation_sample_id']] = row['alps_release']
    if ARG.RELEASE and ARG.BACKCHECK:
        stmt = stmt.replace(" IS NOT NULL", f"='{ARG.RELEASE}'")
    try:
        CURSOR[publishing_db].execute(stmt)
        rows = CURSOR[publishing_db].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        RELEASE[row['workstation_sample_id']] = row['alps_release']


def backcheck(data):
    ''' Backcheck publishing database contents versus JSON data
        Keyword arguments:
          data: JSON data
        Returns:
          None
    '''
    print("Performing backcheck")
    jacs = {}
    for smp in data:
        jacs[smp['sourceRefId'].split("#")[-1]] = True
    for sid in RELEASE:
        if sid not in jacs:
            LOGGER.error("Sample %s (%s) is not in %s", sid, RELEASE[sid], ARG.LIBRARY)
    terminate_program("Backcheck performed")


def convert_file(sourcepath, newname):
    ''' Convert file to PNG format
        Keyword arguments:
          sourcepath: source filepath
          newname: new file name
        Returns:
          New filepath
    '''
    LOGGER.debug("Converting %s to %s", sourcepath, newname)
    newpath = CLOAD.temp_dir + newname
    with Image.open(sourcepath) as image:
        image.save(newpath, 'PNG')
    return newpath


def process_flyem(smp, convert=True):
    ''' Return the file name for a FlyEM sample.
        Keyword arguments:
          smp: sample record
        Returns:
          New file name
    '''
    # Temporary!
    #bodyid, status = smp['name'].split('_')[0:2]
    bodyid = smp['publishedName']
    #field = re.match('.*-(.*)_.*\..*', smp['name'])
    #status = field[1]
    #if bodyid.endswith('-'):
    #    return False
    newname = f"{bodyid}-{REC['alignment_space']}-CDM.png"
    if convert:
        smp['filepath'] = convert_file(smp['filepath'], newname)
    else:
        newname = newname.replace('.png', '.tif')
        if '_FL' in smp['imageName']: # Used to be "name" for API call
            newname = newname.replace('CDM.', 'CDM-FL.')
    return newname


def translate_slide_code(isc, line0):
    ''' Translate a slide code to remove initials.
        Keyword arguments:
          isc: initial slide doce
          line0: line
        Returns:
          New slide code
    '''
    if 'sample_BJD' in isc:
        return isc.replace("BJD", "")
    if 'GMR' in isc:
        new = isc.replace(line0 + "_", "")
        new = new.replace("-", "_")
        return new
    return isc


def get_smp_info(smp):
    ''' Return the sample ID and publishing name
        Keyword arguments:
          smp: sample record
        Returns:
          Sample ID and publishing name, or None if error
    '''
    if 'sampleRef' not in smp or not smp['sampleRef']:
        COUNT['No sampleRef'] += 1
        err_text = f"No sampleRef for {smp['_id']} ({smp['name']})"
        LOGGER.warning(err_text)
        ERR.write(err_text + "\n")
        return None, None
    sid = (smp['sampleRef'].split('#'))[-1]
    LOGGER.debug(sid)
    if 'flylight' in ARG.LIBRARY:
        if sid not in RELEASE:
            COUNT['Sample not published'] += 1
            err_text = f"Sample {sid} was not published"
            LOGGER.warning(err_text)
            ERR.write(err_text + "\n")
            return None, None
        if 'datasetLabels' in smp and len(smp['datasetLabels']) > 1:
            COUNT['Multiple releases'] += 1
            err_text = f"Sample {sid} has multiple releases ({smp['datasetLabels']})"
            LOGGER.warning(err_text)
            ERR.write(err_text + "\n")
            return None, None
    if 'publishedName' not in smp or not smp['publishedName']:
        COUNT['No publishing name'] += 1
        err_text = f"No publishing name for sample {sid}"
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        return None, None
    publishing_name = smp['publishedName']
    if publishing_name in ['Missing consensus', 'No Consensus']:
        COUNT['Missing consensus'] += 1
        err_text = f"No consensus line for sample {sid} ({publishing_name})"
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        return None, None
    if publishing_name not in PNAME:
        PNAME[publishing_name] = 1
    else:
        PNAME[publishing_name] += 1
    return sid, publishing_name


def driver_check(publishing_name, sid):
    ''' Check that the driver is valid
        Keyword arguments:
          publishing_name: publishing name
          sid: sample ID
        Returns:
          Driver name (or False for error)
    '''
    if publishing_name in DRIVER:
        if not DRIVER[publishing_name]:
            COUNT['No driver'] += 1
            err_text = f"No driver for sample {sid} ({publishing_name})"
            ERR.write(err_text + "\n")
            if ARG.WRITE:
                terminate_program(err_text)
            return False
        drv = DRIVER[publishing_name]
        if drv not in CLOAD.drivers:
            COUNT['Bad driver'] += 1
            err_text = f"Bad driver for sample {sid} ({publishing_name})"
            ERR.write(err_text + "\n")
            if ARG.WRITE:
                terminate_program(err_text)
            return False
    else:
        COUNT['Line not published'] += 1
        err_text = f"Sample {sid} ({publishing_name}) is not published in FlyLight external"
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        #if ARG.WRITE: PLUG
        #    terminate_program(err_text)
        return False
    return drv


def process_light(smp):
    ''' Return the file name for a light microscopy sample.
        Keyword arguments:
          smp: sample record
        Returns:
          New file name
    '''
    sid, publishing_name = get_smp_info(smp)
    if not sid:
        LOGGER.error(f"Sample ID not found for ID {smp['_id']}")
        return False
    REC['line'] = publishing_name
    missing = []
    for check in ['slideCode', 'gender', 'objective', 'anatomicalArea']:
        if check not in smp or not smp[check]:
            missing.append(check)
    if missing:
        log_error(f"Missing columns for sample {smp['sampleRef']}: {', '.join(missing)}")
        return False
    REC['slide_code'] = smp['slideCode']
    REC['gender'] = smp['gender']
    REC['objective'] = smp['objective']
    REC['area'] = smp['anatomicalArea'].lower()
    drv = driver_check(publishing_name, sid)
    if not drv:
        LOGGER.error(f"Driver not found for {sid}")
        return False
    fname = os.path.basename(smp['filepath'])
    if 'gamma' in fname:
        chan = fname.split('-')[-2]
    else:
        chan = fname.split('-')[-1]
    chan = chan.split('_')[0].replace('CH', '')
    if chan not in ['1', '2', '3', '4']:
        terminate_program(f"Could not find channel for {fname} ({chan})")
    newname = f"{REC['line']}-{REC['slide_code']}-{drv}-{REC['gender']}-" \
              + f"{REC['objective']}-{REC['area']}-{REC['alignment_space']}-CDM_{chan}.png"
    return newname


def produce_thumbnail(dirpath, fname, newname, url):
    ''' Return the thumbnail path
        Keyword arguments:
          dirpath: source directory
          fname: file name
        Returns:
          thumbnail url
    '''
    turl = url.replace('.png', '.jpg')
    turl = turl.replace(AWS.s3_bucket.cdm, getattr(AWS.s3_bucket, "cdm-thumbnail"))
    return turl


def set_name_and_filepath(smp):
    ''' Determine a sample's name and filepath
        Keyword arguments:
          smp: sample record
        Returns:
          None
    '''
    smp['filepath'] = smp['cdmPath']
    smp['name'] = os.path.basename(smp['filepath'])


def add_searchable_neuron(smp, url):
    ''' Add an uploaded searchable_neurons/pngs path for a sample
        Keyword arguments:
          smp: sample record
          url: searchable_neurons TIFF URL
        Returns:
          None
    '''
    if "uploaded" not in smp:
        smp['uploaded'] = {}
    if "searchable_neurons" in smp['uploaded']:
        terminate_program(f"Duplicate searchable_neurons for {smp['_id']}")
    prefix = url.split("/")
    prefix[-2] = "pngs"
    prefix[-1] = prefix[-1].replace(".tif", ".png")
    new_url = "/".join(prefix)
    smp['uploaded']['searchable_neurons'] = new_url


def upload_flyem_variants(smp, newname):
    ''' Upload variant files for FlyEM
        Keyword arguments:
          smp: sample record
          newname: computed filename
        Returns:
          None
    '''
    if 'variants' not in smp:
        LOGGER.warning("No variants for %s", smp['name'])
        return
    fbase = newname.split('.')[0]
    for variant in smp['variants']:
        if variant not in CLOAD.variants:
            terminate_program(f"Unknown variant {variant}")
        if variant not in WILL_LOAD:
            continue
        fname, ext = os.path.basename(smp['variants'][variant]).split('.')
        ancname = '.'.join([fbase, ext])
        ancname = '/'.join([variant, ancname])
        dirpath = os.path.dirname(smp['variants'][variant])
        fname = os.path.basename(smp['variants'][variant])
        if variant == 'searchable_neurons':
            if SUBDIVISION['counter'] >= SUBDIVISION['limit']:
                SUBDIVISION['prefix'] += 1
                SUBDIVISION['counter'] = 0
            ancname = ancname.replace('searchable_neurons/',
                                      f"searchable_neurons/{str(SUBDIVISION['prefix'])}/")
            SUBDIVISION['counter'] += 1
        url, _ = upload_aws(AWS.s3_bucket.cdm, dirpath, fname, ancname)
        if not url:
            LOGGER.warning(f"Skipping {variant} for {smp['name']} - no URL")
            continue
        add_searchable_neuron(smp, url)
        if variant not in VARIANT_UPLOADS:
            VARIANT_UPLOADS[variant] = 1
        else:
            VARIANT_UPLOADS[variant] += 1


def upload_flyem_skeletons(smp):
    ''' Upload skeleton files for FlyEM
        Keyword arguments:
          smp: sample record
        Returns:
          None
    '''
    for stype in CLOAD.skeletons:
        if stype not in smp['computeFiles']:
            continue
        dirpath = os.path.dirname(smp['computeFiles'][stype])
        fname = os.path.basename(smp['computeFiles'][stype])
        s3type = stype.replace("Skeleton", "").lower()
        newname = "/".join([s3type.upper(), smp['publishedName'] + "." + s3type])
        url, _ = upload_aws(AWS.s3_bucket.cdm, dirpath, fname, newname)
        smp['uploaded'][stype.lower()] = url


def upload_flylight_variants(smp, newname):
    ''' Upload variant files for FlyLight
        Keyword arguments:
          smp: sample record
          newname: computed filename
        Returns:
          None
    '''
    if 'variants' not in smp:
        LOGGER.warning("No variants for %s", smp['name'])
        return
    fbase = newname.split('.')[0]
    for variant in smp['variants']:
        if variant not in CLOAD.variants:
            terminate_program(f"Unknown variant {variant}")
        if variant not in WILL_LOAD:
            continue
        if '.' not in smp['variants'][variant]:
            LOGGER.error("%s file %s has no extension", variant, fname)
            COUNT['Unparsable files'] += 1
            continue
        fname, ext = os.path.basename(smp['variants'][variant]).split('.')
        # MB002B-20121003_31_B2-f_20x_c1_01
        seqsearch = re.search(r"-CH\d+-(\d+)", fname)
        if seqsearch is None:
            LOGGER.error("Could not extract sequence number from %s file %s", variant, fname)
            COUNT['Unparsable files'] += 1
            continue
        seq = seqsearch[1]
        ancname = '.'.join(['-'.join([fbase, seq]), ext])
        ancname = '/'.join([variant, ancname])
        dirpath = os.path.dirname(smp['variants'][variant])
        fname = os.path.basename(smp['variants'][variant])
        #print(fname)
        #print(ancname)
        if variant == 'searchable_neurons':
            if SUBDIVISION['counter'] >= SUBDIVISION['limit']:
                SUBDIVISION['prefix'] += 1
                SUBDIVISION['counter'] = 0
            ancname = ancname.replace('searchable_neurons/',
                                      f"searchable_neurons/{str(SUBDIVISION['prefix'])}/")
            SUBDIVISION['counter'] += 1
        url, _ = upload_aws(AWS.s3_bucket.cdm, dirpath, fname, ancname)
        if not url:
            LOGGER.warning(f"Skipping {variant} for {smp['name']} - no URL")
            continue
        add_searchable_neuron(smp, url)
        if variant not in VARIANT_UPLOADS:
            VARIANT_UPLOADS[variant] = 1
        else:
            VARIANT_UPLOADS[variant] += 1


def check_image(smp):
    ''' Check that the image exists and see if the URL is already specified in publishedURL
        Keyword arguments:
          smp: sample record
        Returns:
          False if error, True otherwise
    '''
    if ARG.LIBRARY.startswith('flyem') or ARG.LIBRARY.startswith('flywire'):
        if 'imageName' not in smp:
            print(smp)
            terminate_program("Missing imageName in sample")
        LOGGER.debug('----- %s', smp['imageName'])
    else:
        # We need to have a release for this sample ID
        sid = (smp['sampleRef'].split('#'))[-1]
        if sid not in RELEASE:
            if sid not in NO_RELEASE:
                msg = f"SID {sid} has no release"
                if sid in FALLBACK:
                    msg += f" (release is {FALLBACK[sid]} in SAGE)"
                log_error(msg)
                NO_RELEASE[sid] = True
            COUNT['Missing release'] += 1
            return False
        if ARG.RELEASE and (ARG.RELEASE != RELEASE[sid]):
            COUNT['Skipped release'] += 1
            return False
        smp['alpsRelease'] = RELEASE[sid]
    # Check Mongo
    coll = DBM.publishedURL
    result = coll.find_one({'_id': int(smp['_id'])})
    if result and not ARG.REWRITE:
        COUNT['Already in Mongo'] += 1
        return False
    COUNT['Not in Mongo'] += 1
    if ARG.LIBRARY.startswith('flylight'):
        if smp['alpsRelease'] not in RELPUB:
            RELPUB[smp['alpsRelease']] = 1
        else:
            RELPUB[smp['alpsRelease']] += 1
    return True


def upload_primary(smp, newname):
    ''' Handle uploading of the primary image
        Keyword arguments:
          smp: sample record
          newname: new file name
        Returns:
          None
    '''
    dirpath = os.path.dirname(smp['filepath'])
    fname = os.path.basename(smp['filepath'])
    url, skipped = upload_aws(AWS.s3_bucket.cdm, dirpath, fname, newname)
    if url:
        # Always write CDM URLs to smp[uploaded]
        if "uploaded" not in smp:
            smp['uploaded'] = {}
        smp['uploaded']['cdm'] = url
        turl = produce_thumbnail(dirpath, fname, newname, url)
        smp['uploaded']['cdm_thumbnail'] = turl
        if not skipped:
            if ARG.WRITE:
                if ARG.AWS and (not ARG.LIBRARY.startswith('flylight')):
                    os.remove(smp['filepath'])
            elif ARG.AWS:
                LOGGER.info("Primary %s", url)
    elif ARG.WRITE:
        LOGGER.error("Did not transfer primary image %s", fname)


def handle_primary(smp):
    ''' Handle the primary image
        Keyword arguments:
          smp: sample record
        Returns:
          New file name
    '''
    skip_primary = False
    newname = None
    if ARG.LIBRARY.startswith('flyem') or ARG.LIBRARY.startswith('flywire'):
        if '_FL' in smp['imageName']:
            COUNT['FlyEM flips'] += 1
        set_name_and_filepath(smp)
        newname = process_flyem(smp)
        if not newname:
            err_text = f"No publishing name for FlyEM {smp['name']}"
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            COUNT['No publishing name'] += 1
            return None
    else:
        if 'variants' in smp and ARG.GAMMA in smp['variants']:
            smp['cdmPath'] = smp['variants'][ARG.GAMMA]
            del smp['variants'][ARG.GAMMA]
        set_name_and_filepath(smp)
        newname = process_light(smp)
        if not newname:
            err_text = f"No publishing name for FlyLight {smp['name']}"
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            return None
    if not skip_primary:
        upload_primary(smp, newname)
    return newname


def handle_variants(smp, newname):
    ''' Handle uploading of the variants
        Keyword arguments:
          smp: sample record
          newname: new file name
        Returns:
          None
    '''
    if ARG.LIBRARY.startswith('flyem') or ARG.LIBRARY.startswith('flywire'):
        if '_FL' in smp['imageName']:
            set_name_and_filepath(smp)
        newname = process_flyem(smp, False)
        if not newname:
            return
        if newname.count('.') > 1:
            terminate_program("Internal error for newname computation")
        upload_flyem_variants(smp, newname)
        if "skeletons" in WILL_LOAD:
            upload_flyem_skeletons(smp)
        #newname = 'searchable_neurons/' + newname
        #dirpath = os.path.dirname(smp['filepath'])
        #fname = os.path.basename(smp['filepath'])
        #url = upload_aws(AWS.s3_bucket.cdm, dirpath, fname, newname)
    else:
        upload_flylight_variants(smp, newname)


def confirm_run():
    ''' Display parms and confirm run
        Keyword arguments:
          None
        Returns:
          True or False
    '''
    print(f"MySQL manifold:       {ARG.MYSQL}")
    print(f"MongoDB manifold:     {ARG.MONGO}")
    print(f"S3 manifold:          {ARG.MANIFOLD}")
    print(f"Library:              {ARG.LIBRARY}")
    if "flyem_" in ARG.LIBRARY:
        print(f"NeuPrint dataset:     {CONF['DATASET']}")
    print(f"Alignment space:      {ARG.ALIGNMENT}")
    print(f"NeuronBridge version: {ARG.TAG}")
    if WILL_LOAD:
        print(f"Files to upload:      {', '.join(WILL_LOAD)}")
    print(f"Required products:    {', '.join(REQUIRED_PRODUCTS)}")
    print(f"Upload files to AWS:  {'Yes' if ARG.AWS else 'No'}")
    print(f"Update MongoDB:       {'Yes' if ARG.WRITE else 'No'}")
    print("Do you want to proceed?")
    allowed = ['No', 'Yes']
    terminal_menu = TerminalMenu(allowed)
    chosen = terminal_menu.show()
    if chosen is None or allowed[chosen] != "Yes":
        return False
    return True


def read_json():
    ''' Read in JSON from a text file or from MongoDB.
        Keyword arguments:
          None
        Returns:
          JSON
    '''
    stime = datetime.now()
    print(f"Loading JSON from Mongo for {ARG.LIBRARY}")
    coll = DBM.neuronMetadata
    payload = {"libraryName": ARG.LIBRARY,
               "$and": [{"tags": ARG.TAG},
                        {"tags": {"$nin": ["unreleased", "validationError"]}}],
               "publishedName": {"$exists": True}}
    if ARG.PUBLISHED:
        payload["publishedName"] = ARG.PUBLISHED
    elif ARG.SLIDE:
        payload["slideCode"] = ARG.SLIDE
    if ARG.ALIGNMENT:
        payload['alignmentSpace'] = ARG.ALIGNMENT
    LOGGER.info("Checking neuronMetadata for %s library entries tagged as %s",
                ARG.LIBRARY, ARG.TAG)
    data = list(coll.find(payload, allow_disk_use=True).sort("slide_code", 1))
    time_diff = datetime.now() - stime
    LOGGER.info("JSON read in %fsec", time_diff.total_seconds())
    print(f"Documents read from Mongo: {len(data):,}")
    return data


def add_image_to_mongo(smp):
    ''' Add an image to the publishedURL collection
        Keyword arguments:
          smp: sample record
        Returns:
          None
    '''
    coll = DBM.publishedURL
    payload = {}
    for col in CLOAD.published_col:
        if col in smp:
            payload[col] = deepcopy(smp[col])
    if "alpsRelease" in smp:
        payload['alpsRelease'] = smp['alpsRelease']
    if "DATASET" in CONF:
        payload['publishedName'] = ":".join([CONF['DATASET'], payload['publishedName']])
    payload["updateDate"] = datetime.now()
    try:
        result = coll.update_one({"_id": payload['_id']}, {"$set": payload}, upsert=True)
    except Exception as err:
        LOGGER.error("Could not insert %s into Mongo", smp['_id'])
    if hasattr(result, 'inserted_id') and result.inserted_id == smp['_id']:
        COUNT["Mongo insertions"] += 1
    else:
        COUNT["Mongo upserts"] += 1


def remap_sample(smp):
    ''' Perform needed file/Mongo remapping
        Keyword arguments:
          smp: sample record
        Returns:
          None
    '''
    if smp["alignmentSpace"] != ARG.ALIGNMENT:
        print(smp["alignmentSpace"], ARG.ALIGNMENT)
        terminate_program(f"ID {smp['_id']} contains multiple alignment spaces")
    if 'SourceColorDepthImage' not in smp['computeFiles']:
        terminate_program(f"Missing SourceColorDepthImage for {smp['sourceRefId']}")
    smp['cdmPath'] = smp['computeFiles']['SourceColorDepthImage']
    smp['sampleRef'] = smp['sourceRefId']
    smp['variants'] = {}
    if 'ZGapImage' in smp['computeFiles']:
        smp['variants']['zgap'] = smp['computeFiles']['ZGapImage']
    if 'InputColorDepthImage' in smp['computeFiles']:
        full = smp['computeFiles']['InputColorDepthImage']
        smp['imageArchivePath'] = os.path.dirname(full)
        smp['imageName'] = os.path.basename(full)
        smp['variants']['searchable_neurons'] = full
    if 'GradientImage' in smp['computeFiles']:
        smp['variants']['gradient'] = smp['computeFiles']['GradientImage']


def write_output_files(json_out, names_out):
    ''' Produce output files
        Keyword arguments:
          json_out: JSON output
          names_out: names to output
        Returns:
          None
    '''
    if json_out:
        LOGGER.info("Writing JSON file")
    with open(JSONO_FILE, 'w', encoding='ascii') as jsonfile:
        jsonfile.write(json.dumps(json_out, indent=4, default=str))
    if names_out:
        LOGGER.info("Writing names file")
        with open(NAMES_FILE, 'w', encoding='ascii') as namefile:
            for pname in names_out:
                namefile.write(f"{pname}\n")


def get_published_samples():
    ''' Build a dictionary of published samples from publishedLMImage
        Keyword arguments:
          None
        Returns:
          Dictionary of published samples
    '''
    coll = DBM.publishedLMImage
    rows = coll.distinct("sampleRef")
    LOGGER.info(f"Found {len(rows):,} published sample IDs")
    return dict.fromkeys(rows, True)


def upload_cdms():
    ''' Upload color depth MIPs and other files to AWS S3.
        The list of color depth MIPs comes from a supplied JSON file.
        Keyword arguments:
          None
        Returns:
          None
    '''
    data = read_json()
    entries = len(data)
    print(f"Number of entries in JSON: {entries:,}")
    if not entries:
        terminate_program("No entries to process")
    if ARG.LIBRARY.startswith('flyem') or ARG.LIBRARY.startswith('flywire'):
        get_flyem_dataset()
    else:
        # Get image mapping
        dbdata = (call_responder('config', 'config/db_config'))["config"]
        publishing_db = 'gen1mcfo' if 'gen1_mcfo' in ARG.LIBRARY else 'mbew'
        if 'raw' in ARG.LIBRARY:
            publishing_db = 'raw'
        (CONN[publishing_db], CURSOR[publishing_db]) = db_connect(dbdata[publishing_db][ARG.MYSQL])
        print("Getting image mapping")
        get_line_mapping(publishing_db)
        get_image_mapping(publishing_db)
        if ARG.BACKCHECK:
            backcheck(data)
        # Get published samples
        published_sample = get_published_samples()
    set_searchable_subdivision(data[0])
    # Manifest
    added = tried = 0
    if ARG.MANIFEST:
        with open(ARG.MANIFEST, 'r', encoding='ascii') as instream:
            rows = instream.read().splitlines()
            for row in rows:
                tried += 1
                if row.startswith(f"{ARG.ALIGNMENT}/{LIBRARY[ARG.LIBRARY]['name']}"):
                    MANIFEST[row] = True
                    added += 1
        LOGGER.info(f"Added {added:,}/{tried:,} entries from manifest")
    if not confirm_run():
        return
    print(f"Processing {ARG.LIBRARY} on {ARG.MANIFOLD} manifold")
    json_out = []
    names_out = {}
    for smp in tqdm(data):
        if 'flylight' in ARG.LIBRARY and smp['slideCode'] in NON_PUBLIC:
            COUNT['Sample not published'] += 1
            LOGGER.warning("Sample %s is in non-public release %s", smp['sourceRefId'],
                           NON_PUBLIC[smp['slideCode']])
            continue
        if 'flylight' in ARG.LIBRARY and smp['sourceRefId'] not in published_sample:
            COUNT['Sample not published'] += 1
            LOGGER.warning("Sample %s is not published in publishedLMImage", smp['sourceRefId'])
        remap_sample(smp)
        if ARG.SAMPLES and COUNT['Samples'] >= ARG.SAMPLES:
            break
        COUNT['Samples'] += 1
        if not check_image(smp):
            continue
        REC['alignment_space'] = smp['alignmentSpace']
        # Primary image
        newname = handle_primary(smp)
        if newname:
            # Publishing name
            names_out[smp['publishedName']] = True
            # Variants
            handle_variants(smp, newname)
            for product in REQUIRED_PRODUCTS:
                if product not in smp['uploaded']:
                    LOGGER.error(f"Missing {product} for ID {smp['_id']}")
            json_out.append(smp)
            if ARG.WRITE:
                add_image_to_mongo(smp)
    write_output_files(json_out, names_out)


def update_library_config():
    ''' Update the library status
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.WRITE or ARG.CONFIG:
        method = "MongoDB"
        source = "neuronMetadata"
        if NB.update_library_status(DBM.cdmLibraryStatus,
                                    library=ARG.LIBRARY,
                                    manifold=ARG.MANIFOLD,
                                    method=method,
                                    source=source,
                                    dataset=ARG.DATASET,
                                    neuprint=ARG.NEUPRINT,
                                    release=ARG.RELEASE,
                                    tag=ARG.TAG,
                                    neuronbridge=f"v{ARG.TAG}",
                                    images=COUNT['Images processed'],
                                    samples=COUNT['Samples'],
                                    updatedBy=CONF['FULL_NAME']):
            LOGGER.info("Updated cdm_library configuration")
        else:
            LOGGER.error("Could not update status in cdmLibraryStatus")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload Color Depth MIPs to AWS S3")
    PARSER.add_argument('--manifest', dest='MANIFEST', action='store',
                        default='', help='Manifest file')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--tag', dest='TAG', action='store',
                        default='', help='MongoDB neuronMetadata tag')
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        default='', help='ALPS release')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', action='store',
                        help='NeuronBridge version')
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        help='NeuPrint dataset, e.g. vnc:v0.6')
    PARSER.add_argument('--alignment', dest='ALIGNMENT', action='store',
                        help='alignment space')
    PARSER.add_argument('--backcheck', dest='BACKCHECK', action='store_true',
                        default=False, help='Perform publishing database backcheck and exit')
    PARSER.add_argument('--internal', dest='INTERNAL', action='store_true',
                        default=False, help='Upload to internal bucket')
    PARSER.add_argument('--gamma', dest='GAMMA', action='store',
                        default='gamma1_4', help='Variant key for gamma image to replace cdmPath')
    PARSER.add_argument('--rewrite', dest='REWRITE', action='store_true',
                        default=False,
                        help='Flag, Update image in AWS and on JACS')
    PARSER.add_argument('--aws', dest='AWS', action='store_true',
                        default=False, help='Write files to AWS')
    PARSER.add_argument('--config', dest='CONFIG', action='store_true',
                        default=False, help='Update configuration')
    PARSER.add_argument('--published', dest='PUBLISHED', action='store',
                        help='publishedName')
    PARSER.add_argument('--slide', dest='SLIDE', action='store',
                        help='slideCode')
    PARSER.add_argument('--samples', dest='SAMPLES', action='store', type=int,
                        default=0, help='Number of samples to transfer')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        default='1.0', help='EM Version')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=MANIFOLDS, help='S3 manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold [dev, prod]')
    PARSER.add_argument('--neuprint', dest='NEUPRINT', action='store',
                        default='prod', choices=['pre', 'prod'],
                        help='NeuPrint manifold [pre, prod]')
    PARSER.add_argument('--mysql', dest='MYSQL', action='store',
                        default='prod', choices=['staging', 'prod'],
                        help='MySQL manifold [staging, prod]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually write to JACS (and AWS if flag set)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    S3CP = ERR = ''
    REST = create_config_object("rest_services")
    AWS = create_config_object("aws")
    CLOAD = create_config_object("upload_cdms")
    initialize_program()
    STAMP = strftime("%Y%m%dT%H%M%S")
    ERR_FILE = f"{ARG.LIBRARY}_errors_{STAMP}.txt"
    ERR = open(ERR_FILE, 'w', encoding='ascii')
    S3CP_FILE = f"{ARG.LIBRARY}_s3cp_{STAMP}.txt"
    S3CP = open(S3CP_FILE, 'w', encoding='ascii')
    NAMES_FILE = f"{ARG.LIBRARY}_{STAMP}.names"
    if ARG.RELEASE:
        JSONO_FILE = f"{ARG.LIBRARY}_{ARG.RELEASE}_{STAMP}.json"
    else:
        JSONO_FILE = f"{ARG.LIBRARY}_{STAMP}.json"
    START_TIME = datetime.now()
    upload_cdms()
    STOP_TIME = datetime.now()
    update_library_config()
    if KEY_LIST:
        LOGGER.info("Writing key file")
        KEY_FILE = f"{ARG.LIBRARY}_keys_{STAMP}.txt"
        with open(KEY_FILE, 'w', encoding='ascii') as keyfile:
            keyfile.write(f"{json.dumps(KEY_LIST)}\n")
    print(f"Elapsed time: {STOP_TIME - START_TIME}")
    for key in sorted(COUNT):
        print(f"{key + ':' : <21} {COUNT[key]:,}")
    if VARIANT_UPLOADS:
        print('Uploaded variants:')
        for key in sorted(VARIANT_UPLOADS):
            print(f"  {key + ':' : <21} {VARIANT_UPLOADS[key]:,}")
    if ARG.LIBRARY.startswith('flylight') and len(RELPUB):
        print("Release counts:")
        maxlen = 0
        for key in RELPUB:
            if len(key) > maxlen:
                maxlen = len(key)
        for key, val in sorted(RELPUB.items()):
            print(f"{key+':':<{maxlen+1}} {val:,}")
    terminate_program()
