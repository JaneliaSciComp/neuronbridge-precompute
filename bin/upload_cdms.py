''' This program will Upload Color Depth MIPs to AWS S3.
'''
__version__ = '1.3.1'

import argparse
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
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import MySQLdb
from PIL import Image
import neuronbridge_lib as NB


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
AWS = dict()
CLOAD = dict()
LIBRARY = dict()
VARIANTS = ["gradient", "searchable_neurons", "zgap"]
WILL_LOAD = list()
# Database
CONN = dict()
CURSOR = dict()
# General use
COUNT = {'Amazon S3 uploads': 0, 'Files to upload': 0, 'Samples': 0, 'No Consensus': 0,
         'No sampleRef': 0, 'No publishing name': 0, 'No driver': 0, 'Not published': 0,
         'Skipped': 0, 'Already on S3': 0, 'Already on JACS': 0, 'Bad driver': 0,
         'Duplicate objects': 0, 'Unparsable files': 0, 'Updated on JACS': 0,
         'FlyEM flips': 0, 'Images': 0}
SUBDIVISION = {'prefix': 1, 'counter': 0, 'limit': 100}
TRANSACTIONS = dict()
PNAME = dict()
REC = {'line': '', 'slide_code': '', 'gender': '', 'objective': '', 'area': ''}
S3_CLIENT = S3_RESOURCE = ''
FULL_NAME = TAGS = ''
MAX_SIZE = 500
CREATE_THUMBNAIL = False
S3_SECONDS = 60 * 60 * 12
VARIANT_UPLOADS = dict()
UPLOADED_NAME = dict()
KEY_LIST = list()


def terminate_program(code):
    ''' Terminate the program gracefully
        Keyword arguments:
          code: return code
        Returns:
          None
    '''
    if S3CP:
        ERR.close()
        S3CP.close()
        if not os.path.getsize(S3CP_FILE):
            os.remove(S3CP_FILE)
        for fpath in [ERR_FILE, S3CP_FILE]:
            if not os.path.getsize(fpath):
                os.remove(fpath)
    sys.exit(code)


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
        LOGGER.critical(err)
        terminate_program(-1)
    if req.status_code == 200:
        return req.json()
    print("Could not get response from %s: %s" % (url, req.text))
    terminate_program(-1)
    return False


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        LOGGER.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        LOGGER.critical('MySQL error: %s', err)
    terminate_program(-1)


def db_connect(dbd):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
    """
    LOGGER.info("Connecting to %s on %s", dbd['name'], dbd['host'])
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
        LOGGER.critical("Token failed validation")
        terminate_program(-1)
    except jwt.exceptions.InvalidTokenError:
        LOGGER.critical("Could not decode token")
        terminate_program(-1)
    return response


def initialize_s3():
    """ Initialize
    """
    global S3_CLIENT, S3_RESOURCE # pylint: disable=W0603
    LOGGER.info("Opening S3 client and resource")
    if ARG.MANIFOLD == 'dev':
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
    if not ARG.LIBRARY:
        print("Select a library:")
        cdmlist = list()
        liblist = list()
        for cdmlib in LIBRARY:
            if ARG.MANIFOLD not in LIBRARY[cdmlib]:
                LIBRARY[cdmlib][ARG.MANIFOLD] = {'updated': 'Never'}
            liblist.append(cdmlib)
            text = cdmlib
            if LIBRARY[cdmlib][ARG.MANIFOLD]['updated']:
                text += " (last updated %s on %s)" \
                        % (LIBRARY[cdmlib][ARG.MANIFOLD]['updated'], ARG.MANIFOLD)
            cdmlist.append(text)
        terminal_menu = TerminalMenu(cdmlist)
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.error("No library selected")
            terminate_program(0)
        ARG.LIBRARY = liblist[chosen].replace(' ', '_')
    if not ARG.NEURONBRIDGE:
        ARG.NEURONBRIDGE = NB.get_neuronbridge_version()
        if not ARG.NEURONBRIDGE:
            LOGGER.error("No NeuronBridge version selected")
            terminate_program(0)
        print(ARG.NEURONBRIDGE)
    if not ARG.JSON:
        print("Select a JSON file:")
        json_base = CLOAD['json_dir'] + "/%s/" % (ARG.NEURONBRIDGE)
        jsonlist = list(map(lambda jfile: jfile.split('/')[-1],
                            glob.glob(json_base + "/*.json")))
        jsonlist.sort()
        terminal_menu = TerminalMenu(jsonlist)
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.error("No JSON file selected")
            terminate_program(0)
        ARG.JSON = '/'.join([json_base, jsonlist[chosen]])


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
    global AWS, CLOAD, CONFIG, FULL_NAME, LIBRARY, TAGS # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/upload_cdms')
    CLOAD = data['config']
    data = call_responder('config', 'config/aws')
    AWS = data['config']
    data = call_responder('config', 'config/cdm_library')
    LIBRARY = data['config']
    get_parms()
    select_uploads()
    TAGS = 'PROJECT=CDCS&STAGE=' + ARG.MANIFOLD + '&DEVELOPER=svirskasr&' \
           + 'VERSION=' + __version__
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])
    if ARG.LIBRARY not in LIBRARY:
        LOGGER.critical("Unknown library %s", ARG.LIBRARY)
        terminate_program(-1)
    if 'JACS_JWT' not in os.environ:
        LOGGER.critical("Missing token - set in JACS_JWT environment variable")
        terminate_program(-1)
    response = decode_token(os.environ['JACS_JWT'])
    if int(time()) >= response['exp']:
        LOGGER.critical("Your token is expired")
        terminate_program(-1)
    FULL_NAME = response['full_name']
    LOGGER.info("Authenticated as %s", FULL_NAME)
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
          url
    '''
    COUNT['Files to upload'] += 1
    complete_fpath = '/'.join([dirpath, fname])
    bucket, object_name = get_s3_names(bucket, newname)
    LOGGER.debug("Uploading %s to S3 as %s", complete_fpath, object_name)
    if object_name in UPLOADED_NAME:
        if complete_fpath != UPLOADED_NAME[object_name]:
            err_text = "%s was already uploaded from %s, but is now being uploaded from %s" \
                       % (object_name, UPLOADED_NAME[object_name], complete_fpath)
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            COUNT['Duplicate objects'] += 1
            return False
        LOGGER.debug("Already uploaded %s", object_name)
        COUNT['Duplicate objects'] += 1
        return 'Skipped'
    UPLOADED_NAME[object_name] = complete_fpath
    url = '/'.join([AWS['base_aws_url'], bucket, object_name])
    url = url.replace(' ', '+')
    if "/searchable_neurons/" in object_name:
        KEY_LIST.append(object_name)
    S3CP.write("%s\t%s\n" % (complete_fpath, '/'.join([bucket, object_name])))
    LOGGER.info("Upload %s", object_name)
    COUNT['Images'] += 1
    if (not ARG.AWS) and (not force):
        return url
    if not ARG.WRITE:
        COUNT['Amazon S3 uploads'] += 1
        return url
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
    return url


def get_line_mapping():
    ''' Create a mapping of publishing names to drivers. Note that "GAL4-Collection"
        is remapped to "GAL4".
        Keyword arguments:
          None
        Returns:
          driver dictionary
    '''
    driver = dict()
    LOGGER.info("Getting line/driver mapping")
    try:
        CURSOR['sage'].execute("SELECT DISTINCT publishing_name,driver FROM image_data_mv " \
                               + "WHERE publishing_name IS NOT NULL AND driver IS NOT NULL")
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        driver[row['publishing_name']] = row['driver'].replace("_Collection", "").replace("-", "_")
    return driver


def get_image_mapping():
    ''' Create a dictionary of published sample IDs
        Keyword arguments:
          None
        Returns:
          sample ID dictionary
    '''
    LOGGER.info("Getting image mapping")
    published_ids = dict()
    stmt = "SELECT DISTINCT workstation_sample_id FROM image_data_mv WHERE " \
           + "to_publish='Y' AND alps_release IS NOT NULL"
    try:
        CURSOR['sage'].execute(stmt)
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        published_ids[row['workstation_sample_id']] = 1
    return published_ids


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
    newname = '%s-%s-CDM.png' \
    % (bodyid, REC['alignment_space'])
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


def get_smp_info(smp, published_ids):
    ''' Return the sample ID and publishing name
        Keyword arguments:
          smp: sample record
          published_ids: sample dictionary
        Returns:
          Sample ID and publishing name, or None if error
    '''
    if 'sampleRef' not in smp or not smp['sampleRef']:
        COUNT['No sampleRef'] += 1
        err_text = "No sampleRef for %s (%s)" % (smp['_id'], smp['name'])
        LOGGER.warning(err_text)
        ERR.write(err_text + "\n")
        return None, None
    sid = (smp['sampleRef'].split('#'))[-1]
    LOGGER.debug(sid)
    if ARG.LIBRARY in ['flylight_splitgal4_drivers']:
        if sid not in published_ids:
            COUNT['Not published'] += 1
            err_text = "Sample %s was not published" % (sid)
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            return None, None
    if 'publishedName' not in smp or not smp['publishedName']:
        COUNT['No publishing name'] += 1
        err_text = "No publishing name for sample %s" % (sid)
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        return None, None
    publishing_name = smp['publishedName']
    if publishing_name == 'No Consensus':
        COUNT['No Consensus'] += 1
        err_text = "No consensus line for sample %s (%s)" % (sid, publishing_name)
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        if ARG.WRITE:
            return False
    if publishing_name not in PNAME:
        PNAME[publishing_name] = 1
    else:
        PNAME[publishing_name] += 1
    return sid, publishing_name


def process_light(smp, driver, published_ids):
    ''' Return the file name for a light microscopy sample.
        Keyword arguments:
          smp: sample record
          driver: driver mapping dictionary
          published_ids: sample dictionary
        Returns:
          New file name
    '''
    sid, publishing_name = get_smp_info(smp, published_ids)
    if not sid:
        return False
    REC['line'] = publishing_name
    REC['slide_code'] = smp['slideCode']
    REC['gender'] = smp['gender']
    REC['objective'] = smp['objective']
    REC['area'] = smp['anatomicalArea'].lower()
    if publishing_name in driver:
        drv = driver[publishing_name]
        if drv not in CLOAD['drivers']:
            COUNT['Bad driver'] += 1
            err_text = "Bad driver for sample %s (%s)" % (sid, publishing_name)
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            if ARG.WRITE:
                terminate_program(-1)
            return False
    else:
        COUNT['No driver'] += 1
        err_text = "No driver for sample %s (%s)" % (sid, publishing_name)
        LOGGER.error(err_text)
        ERR.write(err_text + "\n")
        if ARG.WRITE:
            terminate_program(-1)
        return False
    fname = os.path.basename(smp['filepath'])
    if 'gamma' in fname:
        chan = fname.split('-')[-2]
    else:
        chan = fname.split('-')[-1]
    chan = chan.split('_')[0].replace('CH', '')
    if chan not in ['1', '2', '3', '4']:
        LOGGER.critical("Could not find channel for %s (%s)", fname, chan)
        terminate_program(-1)
    newname = '%s-%s-%s-%s-%s-%s-%s-CDM_%s.png' \
        % (REC['line'], REC['slide_code'], drv, REC['gender'],
           REC['objective'], REC['area'], REC['alignment_space'], chan)
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
        turl = upload_aws(AWS['s3_bucket']['cdm-thumbnail'], '/tmp', tname, tname)
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
            LOGGER.error("Unknown variant %s", variant)
            terminate_program(-1)
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
                                      'searchable_neurons/%s/' % str(SUBDIVISION['prefix']))
            SUBDIVISION['counter'] += 1
        _ = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, ancname)
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
            LOGGER.error("Unknown variant %s", variant)
            terminate_program(-1)
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
                                      'searchable_neurons/%s/' % str(SUBDIVISION['prefix']))
            SUBDIVISION['counter'] += 1
        _ = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, ancname)
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
    if 'imageName' not in smp:
        LOGGER.critical("Missing imageName in sample")
        print(smp)
        terminate_program(-1)
    LOGGER.debug('----- %s', smp['imageName'])
    if 'publicImageUrl' in smp and smp['publicImageUrl'] and not ARG.REWRITE:
        COUNT['Already on JACS'] += 1
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
    url = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, newname)
    if url:
        if url != 'Skipped':
            turl = produce_thumbnail(dirpath, fname, newname, url)
            if ARG.WRITE:
                if ARG.AWS and ('flyem_' in ARG.LIBRARY):
                    os.remove(smp['filepath'])
                update_jacs(smp['_id'], url, turl)
            else:
                LOGGER.info("Primary %s", url)
    elif ARG.WRITE:
        LOGGER.error("Did not transfer primary image %s", fname)


def handle_primary(smp, driver, published_ids):
    ''' Handle the primary image
        Keyword arguments:
          smp: sample record
          driver: driver mapping dictionary
          published_ids: sample dictionary
        Returns:
          New file name
    '''
    skip_primary = False
    newname = None
    if 'flyem_' in ARG.LIBRARY:
        if '_FL' in smp['imageName']:
            COUNT['FlyEM flips'] += 1
            skip_primary = True
        else:
            set_name_and_filepath(smp)
            newname = process_flyem(smp)
            if not newname:
                err_text = "No publishing name for FlyEM %s" % smp['name']
                LOGGER.error(err_text)
                ERR.write(err_text + "\n")
                COUNT['No publishing name'] += 1
                return None
    else:
        if 'variants' in smp and ARG.GAMMA in smp['variants']:
            smp['cdmPath'] = smp['variants'][ARG.GAMMA]
            del smp['variants'][ARG.GAMMA]
        set_name_and_filepath(smp)
        newname = process_light(smp, driver, published_ids)
        if not newname:
            err_text = "No publishing name for FlyLight %s" % smp['name']
            LOGGER.error(err_text)
            ERR.write(err_text + "\n")
            return None
        if 'imageArchivePath' in smp and 'imageName' in smp:
            smp['searchableNeuronsName'] = '/'.join([smp['imageArchivePath'], smp['imageName']])
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
            LOGGER.critical("Internal error for newname computation")
            terminate_program(-1)
        upload_flyem_variants(smp, newname)
        #newname = 'searchable_neurons/' + newname
        #dirpath = os.path.dirname(smp['filepath'])
        #fname = os.path.basename(smp['filepath'])
        #url = upload_aws(AWS['s3_bucket']['cdm'], dirpath, fname, newname)
    else:
        upload_flylight_variants(smp, newname)


def upload_cdms_from_file():
    ''' Upload color depth MIPs and other files to AWS S3.
        The list of color depth MIPs comes from a supplied JSON file.
        Keyword arguments:
          None
        Returns:
          None
    '''
    if 'flyem_' not in ARG.LIBRARY:
        driver = get_line_mapping()
        published_ids = get_image_mapping()
    else:
        driver = {}
        published_ids = {}
    jfile = open(ARG.JSON, 'r')
    data = json.load(jfile)
    jfile.close()
    entries = len(data)
    print("Number of entries in JSON: %d" % entries)
    for smp in tqdm(data):
        smp['_id'] = smp['id']
        if ARG.SAMPLES and COUNT['Samples'] >= ARG.SAMPLES:
            break
        COUNT['Samples'] += 1
        if not check_image(smp):
            continue
        REC['alignment_space'] = smp['alignmentSpace']
        # Primary image
        newname = handle_primary(smp, driver, published_ids)
        # Variants
        if newname:
            handle_variants(smp, newname)


def update_library_config():
    ''' Update the config JSON for this library
        Keyword arguments:
          None
        Returns:
          None
    '''
    if ARG.MANIFOLD not in LIBRARY[ARG.LIBRARY]:
        LIBRARY[ARG.LIBRARY][ARG.MANIFOLD] = dict()
    if ARG.JSON not in LIBRARY[ARG.LIBRARY][ARG.MANIFOLD]:
        LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON] = dict()
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['samples'] = COUNT['Samples']
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['images'] = COUNT['Images']
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['updated'] = re.sub(r"\..*", '',
                                                                     str(datetime.now()))
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD]['updated'] = \
        LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['updated']
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['updated_by'] = FULL_NAME
    LIBRARY[ARG.LIBRARY][ARG.MANIFOLD][ARG.JSON]['method'] = 'JSON file'
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
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        default='', help='color depth library')
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
                        default='dev', help='S3 manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually write to JACS (and AWS if flag set)')
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
    STAMP = strftime("%Y%m%dT%H%M%S")
    ERR_FILE = '%s_errors_%s.txt' % (ARG.LIBRARY, STAMP)
    ERR = open(ERR_FILE, 'w')
    S3CP_FILE = '%s_s3cp_%s.txt' % (ARG.LIBRARY, STAMP)
    S3CP = open(S3CP_FILE, 'w')
    START_TIME = datetime.now()
    print("Processing %s on %s manifold" % (ARG.LIBRARY, ARG.MANIFOLD))
    upload_cdms_from_file()
    STOP_TIME = datetime.now()
    print("Elapsed time: %s" %  (STOP_TIME - START_TIME))
    update_library_config()
    if KEY_LIST:
        KEY_FILE = '%s_keys_%s.txt' % (ARG.LIBRARY, STAMP)
        KEY = open(KEY_FILE, 'w')
        KEY.write("%s\n" % json.dumps(KEY_LIST))
        KEY.close()
    for key in sorted(COUNT):
        print("%-20s %d" % (key + ':', COUNT[key]))
    if VARIANT_UPLOADS:
        print('Uploaded variants:')
        for key in sorted(VARIANT_UPLOADS):
            print("  %-20s %d" % (key + ':', VARIANT_UPLOADS[key]))
    print("Server calls (excluding AWS)")
    print(TRANSACTIONS)
    terminate_program(0)
