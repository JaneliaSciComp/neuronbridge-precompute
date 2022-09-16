''' This program will Upload Color Depth MIPs to AWS S3.
'''
__version__ = '2.0.0'

import argparse
from copy import deepcopy
from datetime import datetime
import glob
import json
import os
import re
import socket
import sys
from time import strftime, time
import boto3
from botocore.exceptions import ClientError
import colorlog
import inquirer
import jwt
from pymongo import MongoClient
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import MySQLdb
from PIL import Image
import neuronbridge_lib as NB


# Configuration
CONFIG = {'config': {'url': os.environ.get('CONFIG_SERVER_URL')}}
AWS = {}
CLOAD = {}
LIBRARY = {}
MANIFOLDS = ['dev', 'prod', 'devpre', 'prodpre']
PUBLISHED_COL = ["_id", "tags", "mipId", "alignmentSpace", "libraryName", "publishedName",
                 "sampleRef", "slideCode", "anatomicalArea", "gender", "objective", "name",
                 "uploaded"]
VARIANTS = ["gradient", "searchable_neurons", "zgap"]
WILL_LOAD = []
# Database
MONGODB = 'neuronbridge-mongo'
DBM = ""
CONN = {}
CURSOR = {}
# AWS
S3_CLIENT = S3_RESOURCE = ''
S3_SECONDS = 60 * 60 * 12
# Thumbnails
CREATE_THUMBNAIL = False # Let a Lambda function create it
MAX_SIZE = 500
# Counters
COUNT = {'Amazon S3 uploads': 0, 'Files to upload': 0, 'Samples': 0, 'Missing consensus': 0,
         'No sampleRef': 0, 'No publishing name': 0, 'No driver': 0, 'Sample not published': 0,
         'Line not published': 0, 'Skipped': 0, 'Already on S3': 0, 'Already on JACS': 0,
         'Already in Mongo': 0,
         'Bad driver': 0, 'Duplicate objects': 0, 'Unparsable files': 0, 'Updated on JACS': 0,
         'Mongo insertions': 0,
         'FlyEM flips': 0, 'Images processed': 0, 'Missing release': 0, 'Skipped release': 0}
TRANSACTIONS = {}
# Searchable neurons
SUBDIVISION = {'prefix': 1, 'counter': 0, 'limit': 100}
# File naming
REC = {'line': '', 'slide_code': '', 'gender': '', 'objective': '', 'area': ''}
# Consistency checks
CHECK_SOURCE = True
# General use
ALIGNMENT_SPACE = ""
DRIVER = {}
KEY_LIST = []
NO_RELEASE = {}
PNAME = {}
PUBLISHED_IDS = {}
RELEASE = {}
FULL_NAME = ''
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
    if not server in TRANSACTIONS:
        TRANSACTIONS[server] = 1
    else:
        TRANSACTIONS[server] += 1
    url = (CONFIG[server]['url'] if server else '') + endpoint
    try:
        if payload or authenticate:
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer " + os.environ['JACS_JWT']}
        if payload:
            headers['Accept'] = 'application/json'
            headers['host'] = socket.gethostname()
            req = requests.put(url, headers=headers, json=payload)
        else:
            if authenticate:
                req = requests.get(url, headers=headers)
            else:
                req = requests.get(url)
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
    LOGGER.info(f"Connecting to {dbd['name']} on {dbd['host']}")
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


def decode_token(token):
    ''' Decode a given JWT token
        Keyword arguments:
          token: JWT token
        Returns:
          decoded token JSON
    '''
    try:
        response = jwt.decode(token, verify=False)
    except jwt.exceptions.DecodeError:
        terminate_program("Token failed validation")
    except jwt.exceptions.InvalidTokenError:
        terminate_program("Could not decode token")
    return response


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
        aro = sts_client.assume_role(RoleArn=AWS['role_arn'],
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


def get_parms():
    """ Query the user for the CDM library and manifold
        Keyword arguments:
            None
        Returns:
            None
    """
    coll = DBM.neuronMetadata
    if not ARG.LIBRARY:
        if ARG.SOURCE == 'mongo':
            mongo_libs = coll.distinct("libraryName")
        print("Select a library:")
        cdmlist = []
        liblist = []
        for cdmlib in LIBRARY:
            if ARG.MANIFOLD not in LIBRARY[cdmlib]:
                LIBRARY[cdmlib][ARG.MANIFOLD] = {'updated': None}
            if ARG.SOURCE == 'mongo' and cdmlib not in mongo_libs:
                continue
            liblist.append(cdmlib)
            text = cdmlib
            if 'updated' in LIBRARY[cdmlib][ARG.MANIFOLD] and \
               LIBRARY[cdmlib][ARG.MANIFOLD]['updated'] \
               and "0000-00-00" not in LIBRARY[cdmlib][ARG.MANIFOLD]['updated']:
                text += f" (last updated {LIBRARY[cdmlib][ARG.MANIFOLD]['updated']} " \
                        + f"on {ARG.MANIFOLD})"
            cdmlist.append(text)
        terminal_menu = TerminalMenu(cdmlist)
        chosen = terminal_menu.show()
        if chosen is None:
            terminate_program("No library selected")
        ARG.LIBRARY = liblist[chosen].replace(' ', '_')
    if not ARG.NEURONBRIDGE:
        if ARG.SOURCE == 'file':
            ARG.NEURONBRIDGE = NB.get_neuronbridge_version()
            if not ARG.NEURONBRIDGE:
                terminate_program("No NeuronBridge version selected")
        else:
            rows = coll.distinct("tags", {"libraryName": ARG.LIBRARY})
            vlist = []
            for row in rows:
                if row[0].isdigit():
                    vlist.append("v" + row)
            vlist.sort()
            terminal_menu = TerminalMenu(vlist)
            chosen = terminal_menu.show()
            if chosen is None:
                terminate_program("No version selected")
            ARG.NEURONBRIDGE = vlist[chosen]
        print(ARG.NEURONBRIDGE)
    if not ARG.JSON and ARG.SOURCE == 'file':
        print("Select a JSON file:")
        json_base = CLOAD['json_dir'] + f"/{ARG.NEURONBRIDGE}"
        jsonlist = list(map(lambda jfile: jfile.split('/')[-1],
                            glob.glob(json_base + "/*.json")))
        jsonlist.sort()
        terminal_menu = TerminalMenu(jsonlist)
        chosen = terminal_menu.show()
        if chosen is None:
            terminate_program("No JSON file selected")
        ARG.JSON = '/'.join([json_base, jsonlist[chosen]])


def set_searchable_subdivision(smp):
    """ Set the first searchable_neurons subdivision
        Keyword arguments:
            smp: first sample from JSON file
        Returns:
            Alignment space
    """
    global ALIGNMENT_SPACE
    if "alignmentSpace" not in smp:
        terminate_program("Could not find alignment space in first sample")
    bucket = AWS["s3_bucket"]["cdm"]
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
    LOGGER.warning("Will upload searchable neurons starting with subdivision " \
                   + f"{SUBDIVISION['prefix']}")
    ALIGNMENT_SPACE = smp["alignmentSpace"]


def select_uploads():
    """ Query the user for which image types to upload
        Keyword arguments:
            None
        Returns:
            None
    """
    global WILL_LOAD # pylint: disable=W0603
    quest = [inquirer.Checkbox('checklist',
                               message='Select image types to upload',
                               choices=VARIANTS, default=VARIANTS)]
    WILL_LOAD = inquirer.prompt(quest)['checklist']


def initialize_program():
    """ Initialize
    """
    global AWS, CLOAD, CONFIG, DBM, FULL_NAME, LIBRARY # pylint: disable=W0603
    CONFIG = (call_responder('config', 'config/rest_services'))["config"]
    CLOAD = (call_responder('config', 'config/upload_cdms'))["config"]
    AWS = (call_responder('config', 'config/aws'))["config"]
    LIBRARY = (call_responder('config', 'config/cdm_library'))["config"]
    if ARG.WRITE:
        if 'JACS_JWT' not in os.environ:
            terminate_program("Missing token - set in JACS_JWT environment variable")
        response = decode_token(os.environ['JACS_JWT'])
        if int(time()) >= response['exp']:
            terminate_program("Your token is expired")
        FULL_NAME = response['full_name']
        LOGGER.info("Authenticated as %s", FULL_NAME)
    if not ARG.MANIFOLD:
        print("Select a manifold")
        terminal_menu = TerminalMenu(MANIFOLDS)
        chosen = terminal_menu.show()
        if chosen is None:
            terminate_program("You must select a manifold")
        ARG.MANIFOLD = MANIFOLDS[chosen]
    # MySQL
    data = (call_responder('config', 'config/db_config'))["config"]
    (CONN['sage'], CURSOR['sage']) = db_connect(data['sage']['prod'])
    # MongoDB
    LOGGER.info("Connecting to Mongo on %s", ARG.MONGO)
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        rset = 'rsProd' if ARG.MONGO == 'prod' else 'rsDev'
        client = MongoClient(data[MONGODB][ARG.MONGO][rwp]['host'],
                             replicaSet=rset)
        DBM = client.admin
        DBM.authenticate(data[MONGODB][ARG.MONGO][rwp]['user'],
                         data[MONGODB][ARG.MONGO][rwp]['password'])
        DBM = client.neuronbridge
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")
    # Get parms
    get_parms()
    select_uploads()
    if ARG.LIBRARY not in LIBRARY:
        terminate_program(f"Unknown library {ARG.LIBRARY}")
    # AWS S3
    initialize_s3()


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
    if ARG.LIBRARY in CLOAD['version_required']:
        library += '_v' + ARG.VERSION
    object_name = '/'.join([REC['alignment_space'], library, newname])
    return bucket, object_name


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
    url = '/'.join([AWS['base_aws_url'], bucket, object_name])
    url = url.replace(' ', '+')
    if object_name in UPLOADED_NAME:
        if complete_fpath != UPLOADED_NAME[object_name]:
            err_text = "f{object_name} was already uploaded from {UPLOADED_NAME[object_name]}, " \
                       + f"but is now being uploaded from {complete_fpath}"
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            COUNT['Duplicate objects'] += 1
            return False
        LOGGER.debug("Already uploaded %s", object_name)
        COUNT['Duplicate objects'] += 1
        return url, True
    COUNT['Files to upload'] += 1
    UPLOADED_NAME[object_name] = complete_fpath
    if "/searchable_neurons/" in object_name:
        KEY_LIST.append(object_name)
    if CHECK_SOURCE and not os.path.exists(complete_fpath):
        terminate_program(f"File {complete_fpath} does not exist")
    LOGGER.debug(f"Uploading {complete_fpath} to S3 as {object_name}")
    S3CP.write(f"{complete_fpath}\t{'/'.join([bucket, object_name])}\n")
    if ARG.AWS:
        LOGGER.info(f"Upload {object_name}")
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
        return False
    COUNT['Amazon S3 uploads'] += 1
    return url, False


def get_line_mapping():
    ''' Create a mapping of publishing names to drivers. Note that "GAL4-Collection"
        is remapped to "GAL4".
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Getting line/driver mapping")
    try:
        CURSOR['sage'].execute("SELECT DISTINCT publishing_name,driver FROM image_data_mv " \
                               + "WHERE publishing_name IS NOT NULL")
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        if row['driver']:
            DRIVER[row['publishing_name']] = \
                row['driver'].replace("_Collection", "").replace("-", "_")


def get_image_mapping():
    ''' Create a dictionary of published sample IDs and releases
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info("Getting image mapping (sample -> release)")
    stmt = "SELECT DISTINCT workstation_sample_id,alps_release FROM image_data_mv WHERE " \
           + "to_publish='Y' AND alps_release IS NOT NULL"
    try:
        CURSOR['sage'].execute(stmt)
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        PUBLISHED_IDS[row['workstation_sample_id']] = 1
        RELEASE[row['workstation_sample_id']] = row['alps_release']


def convert_file(sourcepath, newname):
    ''' Convert file to PNG format
        Keyword arguments:
          sourcepath: source filepath
          newname: new file name
        Returns:
          New filepath
    '''
    LOGGER.debug("Converting %s to %s", sourcepath, newname)
    newpath = CLOAD['temp_dir']+ newname
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
    if ARG.LIBRARY in ['flylight_splitgal4_drivers']:
        if sid not in PUBLISHED_IDS:
            COUNT['Sample not published'] += 1
            err_text = f"Sample {sid} was not published"
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            return None, None
    if 'publishedName' not in smp or not smp['publishedName']:
        COUNT['No publishing name'] += 1
        err_text = f"No publishing name for sample {sid}"
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        return None, None
    publishing_name = smp['publishedName']
    if publishing_name == 'Missing consensus':
        COUNT['Missing consensus'] += 1
        err_text = f"No consensus line for sample {sid} ({publishing_name})"
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        if ARG.WRITE:
            return False
    if publishing_name not in PNAME:
        PNAME[publishing_name] = 1
    else:
        PNAME[publishing_name] += 1
    return sid, publishing_name


def process_light(smp):
    ''' Return the file name for a light microscopy sample.
        Keyword arguments:
          smp: sample record
        Returns:
          New file name
    '''
    sid, publishing_name = get_smp_info(smp)
    if not sid:
        return False
    REC['line'] = publishing_name
    missing = []
    for check in ['slideCode', 'gender', 'objective', 'anatomicalArea']:
        if check not in smp or not smp[check]:
            missing.append(check)
    if missing:
        terminate_program(f"Missing columns for sample {smp['sampleRef']}: {', '.join(missing)}")
    REC['slide_code'] = smp['slideCode']
    REC['gender'] = smp['gender']
    REC['objective'] = smp['objective']
    REC['area'] = smp['anatomicalArea'].lower()
    if publishing_name in DRIVER:
        if not DRIVER[publishing_name]:
            COUNT['No driver'] += 1
            err_text = f"No driver for sample {sid} ({publishing_name})"
            ERR.write(err_text + "\n")
            if ARG.WRITE:
                terminate_program(err_text)
            return False
        drv = DRIVER[publishing_name]
        if drv not in CLOAD['drivers']:
            COUNT['Bad driver'] += 1
            err_text = f"Bad driver for sample {sid} ({publishing_name})"
            ERR.write(err_text + "\n")
            if ARG.WRITE:
                terminate_program(err_text)
            return False
    else:
        COUNT['Line not published'] += 1
        err_text = f"Sample {sid} ({publishing_name}) is not published in SAGE"
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        if ARG.WRITE:
            terminate_program(err_text)
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


def calculate_size(dim):
    ''' Return the fnew dimensions for an image. The longest side will be scaled down to MAX_SIZE.
        Keyword arguments:
          dim: tuple with (X,Y) dimensions
        Returns:
          Tuple with new (X,Y) dimensions
    '''
    xdim, ydim = list(dim)
    if xdim <= MAX_SIZE and ydim <= MAX_SIZE:
        return dim
    if xdim > ydim:
        ratio = xdim / MAX_SIZE
        xdim, ydim = [MAX_SIZE, int(ydim/ratio)]
    else:
        ratio = ydim / MAX_SIZE
        xdim, ydim = [int(xdim/ratio), MAX_SIZE]
    return tuple((xdim, ydim))


def resize_image(image_path, resized_path):
    ''' Read in an image, resize it, and write a copy.
        Keyword arguments:
          image_path: CDM image path
          resized_path: path for resized image
        Returns:
          None
    '''
    with Image.open(image_path) as image:
        new_size = calculate_size(image.size)
        image.thumbnail(new_size)
        image.save(resized_path, 'JPEG')


def produce_thumbnail(dirpath, fname, newname, url):
    ''' Transfer a file to Amazon S3
        Keyword arguments:
          dirpath: source directory
          fname: file name
        Returns:
          thumbnail url
    '''
    turl = url.replace('.png', '.jpg')
    turl = turl.replace(AWS['s3_bucket']['cdm'], AWS['s3_bucket']['cdm-thumbnail'])
    if CREATE_THUMBNAIL:
        tname = newname.replace('.png', '.jpg')
        complete_fpath = '/'.join([dirpath, fname])
        resize_image(complete_fpath, '/tmp/' + tname)
        turl, _ = upload_aws(AWS['s3_bucket']['cdm-thumbnail'], '/tmp', tname, tname)
    return turl


def update_jacs(sid, url, turl):
    ''' Update a sample in JACS with URL and thumbnail URL for viewable image
        Keyword arguments:
          sid: sample ID
          url: image URL
          turl: thumbnail URL
        Returns:
          None
    '''
    pay = {"class": "org.janelia.model.domain.gui.cdmip.ColorDepthImage",
           "publicImageUrl": url,
           "publicThumbnailUrl": turl}
    call_responder('jacsv2', 'colorDepthMIPs/' + sid \
                   + '/publicURLs', pay, True)
    COUNT['Updated on JACS'] += 1


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
        if variant not in VARIANTS:
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
        url, _ = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, ancname)
        add_searchable_neuron(smp, url)
        if variant not in VARIANT_UPLOADS:
            VARIANT_UPLOADS[variant] = 1
        else:
            VARIANT_UPLOADS[variant] += 1


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
        if variant not in VARIANTS:
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
        url, _ = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, ancname)
        add_searchable_neuron(smp,url)
        if variant not in VARIANT_UPLOADS:
            VARIANT_UPLOADS[variant] = 1
        else:
            VARIANT_UPLOADS[variant] += 1


def check_image(smp):
    ''' Check that the image exists and see if the URL is already specified
        Keyword arguments:
          smp: sample record
        Returns:
          False if error, True otherwise
    '''
    if 'flyem_' in ARG.LIBRARY:
        if 'imageName' not in smp:
            print(smp)
            terminate_program("Missing imageName in sample")
        LOGGER.debug('----- %s', smp['imageName'])
    else:
        # We need to have a release for this sample ID
        sid = (smp['sampleRef'].split('#'))[-1]
        if sid not in RELEASE:
            if sid not in NO_RELEASE:
                LOGGER.warning(f"SID {sid} has no release")
                NO_RELEASE[sid] = True
            COUNT['Missing release'] += 1
            return False
        if ARG.RELEASE and (ARG.RELEASE != RELEASE[sid]):
            COUNT['Skipped release'] += 1
            return False
        smp['alpsRelease'] = RELEASE[sid]
    # Check JACS
    if 'publicImageUrl' in smp and smp['publicImageUrl'] and not ARG.REWRITE:
        COUNT['Already on JACS'] += 1
        return False
    # Check Mongo
    coll = DBM.publishedURL
    result = coll.find_one({'_id': int(smp['_id'])})
    if result:
        COUNT['Already in Mongo'] += 1
        return False
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
    url, skipped = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, newname)
    if url:
        # Always write CDM URLs to smp[uploaded]
        if "uploaded" not in smp:
            smp['uploaded'] = {}
        smp['uploaded']['cdm'] =  url
        turl = produce_thumbnail(dirpath, fname, newname, url)
        smp['uploaded']['cdm_thumbnail'] =  turl
        if not skipped:
            if ARG.WRITE:
                if ARG.AWS and ('flyem_' in ARG.LIBRARY):
                    os.remove(smp['filepath'])
                #update_jacs(smp['_id'], url, turl)
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
    if 'flyem_' in ARG.LIBRARY:
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
        # PLUG - Dead code?
        #if 'imageArchivePath' in smp and 'imageName' in smp:
        #    smp['searchableNeuronsName'] = '/'.join([smp['imageArchivePath'], smp['imageName']])
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
    if 'flyem_' in ARG.LIBRARY:
        if '_FL' in smp['imageName']:
            set_name_and_filepath(smp)
        newname = process_flyem(smp, False)
        if not newname:
            return
        if newname.count('.') > 1:
            terminate_program("Internal error for newname computation")
        upload_flyem_variants(smp, newname)
        #newname = 'searchable_neurons/' + newname
        #dirpath = os.path.dirname(smp['filepath'])
        #fname = os.path.basename(smp['filepath'])
        #url = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, newname)
    else:
        upload_flylight_variants(smp, newname)


def confirm_run():
    ''' Display parms and confirm run
        Keyword arguments:
          None
        Returns:
          True or False
    '''
    print(f"Manifold:             {ARG.MANIFOLD}")
    print(f"MongoDB:              {ARG.MONGO}")
    print(f"Library:              {ARG.LIBRARY}")
    print(f"Alignment space:      {ALIGNMENT_SPACE}")
    print(f"NeuronBridge version: {ARG.NEURONBRIDGE}")
    if ARG.SOURCE == 'file':
        print(f"JSON file:            {ARG.JSON}")
    print(f"Files to upload:      {', '.join(WILL_LOAD)}")
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
    if ARG.SOURCE == 'file':
        print("Loading JSON file")
        time_diff = (datetime.now() - stime)
        LOGGER.info("JSON read in %fsec", time_diff.total_seconds())
        stime = datetime.now()
        with open(ARG.JSON, 'r', encoding='ascii') as jfile:
            data = json.load(jfile)
        time_diff = (datetime.now() - stime)
        LOGGER.info("JSON parsed in %fsec", time_diff.total_seconds())
    else:
        print(f"Loading JSON from Mongo for {ARG.LIBRARY}")
        coll = DBM.neuronMetadata
        payload = {"libraryName": ARG.LIBRARY}
        #payload['mipId'] = "2945073171637825547" #PLUG
        rows = coll.find(payload)
        time_diff = (datetime.now() - stime)
        LOGGER.info("JSON read in %fsec", time_diff.total_seconds())
        data = []
        stime = datetime.now()
        mrows = 0
        vsearch = ARG.NEURONBRIDGE.replace("v", "")
        for row in rows:
            mrows += 1
            if vsearch in row['tags']:
                data.append(row)
        time_diff = (datetime.now() - stime)
        LOGGER.info("JSON parsed in %fsec", time_diff.total_seconds())
        print(f"Documents read from Mongo: {mrows}")
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
    for col in PUBLISHED_COL:
        if col in smp:
            payload[key] = deepcopy(smp[col])
    if "alpsRelease" in smp:
        payload['alpsRelease'] = smp['alpsRelease']
    payload["updateDate"] = datetime.now()
    result = coll.insert_one(payload)
    if result.inserted_id == smp['_id']:
        COUNT["Mongo insertions"] += 1
    else:
        LOGGER.error(f"Could not insert {smp['_id']} into Mongo")


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
    print(f"Number of entries in JSON: {entries}")
    if not entries:
        terminate_program("No entries to process")
    # Get image mapping
    if 'flyem_' not in ARG.LIBRARY:
        print("Getting image mapping")
        get_line_mapping()
        get_image_mapping()
    set_searchable_subdivision(data[0])
    if not confirm_run():
        return
    print(f"Processing {ARG.LIBRARY} on {ARG.MANIFOLD} manifold")
    json_out = []
    names_out = {}
    for smp in tqdm(data):
        if smp["alignmentSpace"] != ALIGNMENT_SPACE:
            terminate_program("JSON contains multiple alignment spaces")
        if ARG.SOURCE == 'file':
            smp['_id'] = smp['id']
        else:
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
            json_out.append(smp)
            if ARG.WRITE:
                add_image_to_mongo(smp)
    data = None
    if ARG.SOURCE == 'mongo' and json_out:
        LOGGER.info("Writing JSON file")
        with open(JSONO_FILE, 'w', encoding='ascii') as jsonfile:
            jsonfile.write(json.dumps(json_out, indent=4, default=str))
    if names_out:
        LOGGER.info("Writing names file")
        with open(NAMES_FILE, 'w', encoding='ascii') as namefile:
            for pname in names_out:
                namefile.write(f"{pname}\n")


def update_library_config():
    ''' Update the config JSON for this library
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.MANIFOLD not in LIBRARY[ARG.LIBRARY]:
        LIBRARY[ARG.LIBRARY][ARG.MANIFOLD] = {}
    if ARG.JSON not in LIBRARY[ARG.LIBRARY][ARG.MANIFOLD]:
        LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON] = {}
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['samples'] = COUNT['Samples']
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['images'] = COUNT['Images processed']
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['updated'] = re.sub(r"\..*", '',
                                                                     str(datetime.now()))
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD]['updated'] = \
        LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['updated']
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['updated_by'] = FULL_NAME
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['method'] = 'JSON file' \
        if ARG.SOURCE == 'file' else 'MongoDB'
    if ARG.WRITE or ARG.CONFIG:
        resp = requests.post(CONFIG['config']['url'] + 'importjson/cdm_library/' + ARG.LIBRARY,
                             {"config": json.dumps(LIBRARY[ARG.LIBRARY])})
        if resp.status_code != 200:
            LOGGER.error(resp.json()['rest']['message'])
        else:
            LOGGER.info("Updated cdm_library configuration")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Upload Color Depth MIPs to AWS S3")
    PARSER.add_argument('--source', dest='SOURCE', action='store',
                        default='mongo', choices=['file', 'mongo'],
                        help='JSON source [file, mongo]')
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        default='', help='ALPS release')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', action='store',
                        help='NeuronBridge version')
    PARSER.add_argument('--json', dest='JSON', action='store',
                        help='JSON file')
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
    PARSER.add_argument('--samples', dest='SAMPLES', action='store', type=int,
                        default=0, help='Number of samples to transfer')
    PARSER.add_argument('--version', dest='VERSION', action='store',
                        default='1.0', help='EM Version')
    PARSER.add_argument('--check', dest='CHECK', action='store_true',
                        default=False,
                        help='Flag, Check for previous AWS upload')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=MANIFOLDS, help='S3 manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold [dev, prod]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually write to JACS (and AWS if flag set)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    if ARG.SOURCE == 'mongo':
        ARG.JSON = 'MongoDB'

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

    S3CP = ERR = ''
    initialize_program()
    if ARG.WRITE:
        CHECK_SOURCE = False
    STAMP = strftime("%Y%m%dT%H%M%S")
    ERR_FILE = f"{ARG.LIBRARY}_errors_{STAMP}.txt"
    ERR = open(ERR_FILE, 'w', encoding='ascii')
    S3CP_FILE = f"{ARG.LIBRARY}_s3cp_{STAMP}.txt"
    S3CP = open(S3CP_FILE, 'w', encoding='ascii')
    NAMES_FILE = f"{ARG.LIBRARY}_{STAMP}.names"
    if ARG.SOURCE == 'mongo':
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
        print(f"{key + ':' : <19} {COUNT[key]}")
    if VARIANT_UPLOADS:
        print('Uploaded variants:')
        for key in sorted(VARIANT_UPLOADS):
            print(f"  {key + ':' : <21} {VARIANT_UPLOADS[key]}")
    print("Server calls (excluding AWS)")
    print(TRANSACTIONS)
    terminate_program()
