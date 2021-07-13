''' copy_ppp_imagery.py
    Rename and copy PPP PNGs
'''

import argparse
from glob import glob
from datetime import datetime, timezone
from io import BytesIO
import json
import os
from pathlib import Path
import re
import shutil
import sys
import colorlog
import boto3
from botocore.exceptions import ClientError
import dask
from dask.callbacks import Callback
from PIL import Image
from pymongo import MongoClient
import requests
from simple_term_menu import TerminalMenu
from tqdm.auto import tqdm


__version__ = '0.0.3'
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# AWS
AWS = dict()
DATABASE = dict()
S3_SECONDS = 60 * 60 * 12
# Database
DBM = ''
# General use
CDM_ALIGNMENT_SPACE = 'JRC2018_Unisex_20x_HR'
NEURONBRIDGE_JSON_BASE = '/nrs/neuronbridge'
RENAME_COMPONENTS = ['maskPublishedName', 'publishedName', 'slideCode', 'objective']
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
# pylint: disable=W0703


class ProgressBar(Callback):
    """ Callback to replace Dask progress bar
    """
    def _start_state(self, _, state):
        self._tqdm = tqdm(total=sum(len(state[k]) for k in ['ready', 'waiting',
                                                            'running', 'finished']),
                          colour='green')

    def _posttask(self, key, result, dsk, state, worker_id):
        self._tqdm.update(1)

    def _finish(self, dsk, state, errored):
        pass


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
        Returns:
          JSON results
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        LOGGER.error('Status: %s (%s)', str(req.status_code), url)
        sys.exit(-1)
    return req.json()


def initialize_program():
    """ Initialize
    """
    global AWS, CONFIG, DBM # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/aws')
    AWS = data['config']
    data = call_responder('config', 'config/db_config')
    DATABASE = data['config']
    # Connect to Mongo
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        if ARG.MANIFOLD != 'local':
            client = MongoClient(DATABASE['jacs-mongo'][ARG.MANIFOLD][rwp]['host'])
        else:
            client = MongoClient()
        DBM = client.ppp
        if ARG.MANIFOLD == 'prod':
            DBM.authenticate(DATABASE['jacs-mongo'][ARG.MANIFOLD][rwp]['user'],
                             DATABASE['jacs-mongo'][ARG.MANIFOLD][rwp]['password'])
    except Exception as err:
        LOGGER.error('Could not connect to Mongo: %s', err)
        sys.exit(-1)


def initialize_s3():
    """ Initialize S3 client
        Keyword arguments:
          None
        Returns:
          S3 client
    """
    if ARG.MANIFOLD == 'prod':
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=AWS['role_arn'],
                                     RoleSessionName="AssumeRoleSession1",
                                     DurationSeconds=S3_SECONDS)
        credentials = aro['Credentials']
        s3_client = boto3.client('s3',
                                 aws_access_key_id=credentials['AccessKeyId'],
                                 aws_secret_access_key=credentials['SecretAccessKey'],
                                 aws_session_token=credentials['SessionToken'])
    else:
        s3_client = boto3.client('s3')
    return s3_client


def get_library(client, bucket):
    """ Prompt the user for a library selected from AWS S3 prefixes
        Keyword arguments:
          client: S3 client
          bucket: S3 bucket
        Returns:
          None (sets ARG.LIBRARY)
    """
    library = list()
    try:
        response = client.list_objects_v2(Bucket=bucket,
                                          Prefix=CDM_ALIGNMENT_SPACE + '/', Delimiter='/')
    except ClientError as err:
        LOGGER.critical(err)
        sys.exit(-1)
    except Exception as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if 'CommonPrefixes' not in response:
        LOGGER.critical("Could not find any libraries")
        sys.exit(-1)
    for prefix in response['CommonPrefixes']:
        prefixname = prefix['Prefix'].split('/')[-2]
        try:
            key = CDM_ALIGNMENT_SPACE + '/' + prefixname \
                  + '/searchable_neurons/keys_denormalized.json'
            client.head_object(Bucket=bucket, Key=key)
            library.append(prefixname)
        except ClientError:
            pass
    print("Select a library:")
    terminal_menu = TerminalMenu(library)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No library selected")
        sys.exit(0)
    ARG.LIBRARY = library[chosen]


def get_nb_version():
    """ Prompt the user for a MeuronBridge version from subdirs in the base dir
        Keyword arguments:
          None
        Returns:
          None (sets ARG.NEURONBRIDGE)
    """
    version = [re.sub('.*/', '', path) for path in glob(NEURONBRIDGE_JSON_BASE + '/v[0-9]*')]
    print("Select a NeuronBridge version:")
    terminal_menu = TerminalMenu(version)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No NeuronBridge version selected")
        sys.exit(0)
    ARG.NEURONBRIDGE = version[chosen]


def write_file(source_path, newdir, newname):
    """ Copy a file to a new directory
        Keyword arguments:
          source_path: source directory/filename
          newdir: target directory
          newname: target filename
        Returns:
          None
    """
    newpath = '/'.join([newdir, newname])
    try:
        if ARG.LINK:
            os.link(source_path, newpath)
        else:
            shutil.copy(source_path, newpath)
    except Exception as err:
        LOGGER.error("Could not copy %s to %s", source_path, newpath)
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)


def upload_aws(client, bucket, sourcepath, targetpath):
    """ Transfer a file to Amazon S3
        Keyword arguments:
          client: S3 client
          bucket: S3 bucket
          sourcepath: source path
          targetpath: target path
        Returns:
          url
    """
    LOGGER.debug("Uploading %s", targetpath)
    try:
        client.upload_file(sourcepath, bucket, targetpath,
                           ExtraArgs={'ContentType': 'image/png', 'ACL': 'public-read'})
    except Exception as err:
        LOGGER.critical(err)


def handle_single_json_file(path):
    """ Process a single JSON file (there is one JSON file per body ID)
        Keyword arguments:
          path: JSON file path
        Returns:
          None
    """
    s3_client = initialize_s3()
    bucket = AWS['s3_bucket']['ppp']
    if ARG.MANIFOLD != 'prod':
        bucket += '-dev'
    # Read JSON file
    try:
        with open(path) as handle:
            data = json.load(handle)
    except Exception as err:
        LOGGER.error("Could not open %s", path)
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
        sys.exit(-1)
    filedict = dict()
    newdir = '/'.join([NEURONBRIDGE_JSON_BASE, 'ppp_imagery', ARG.NEURONBRIDGE, ARG.LIBRARY,
                       os.path.basename(path)[0:2]])
    newdir += '/' + os.path.basename(path).split('.')[0]
    try:
        Path(newdir).mkdir(parents=True, exist_ok=True)
    except Exception as err:
        LOGGER.error("Could not create %s", newdir)
        LOGGER.error(TEMPLATE, type(err).__name__, err.args)
    for key in ['maskPublishedName', 'results']:
        if key not in data:
            LOGGER.critical("Invalid format for %s - missing %s")
            sys.exit(-1)
    body_id = data['maskPublishedName']
    LOGGER.debug("Processing %s", body_id)
    coll = DBM.pppBodyIds
    check = coll.find_one({"bodyid": body_id})
    if not check:
        payload = {"bodyid": body_id,
                   "resultsFound": len(data['results']),
                   "resultsUpdated": 0,
                   "resultsSkipped": 0,
                   "filesFound": 0,
                   "filesUpdated": 0,
                   "creationDate": datetime.now(),
                   "updatedDate": datetime.now()
                  }
        if ARG.WRITE:
            mongo_id = coll.insert_one(payload).inserted_id
    else:
        mongo_id = check['_id']
        if check['resultsFound'] == (check['resultsSkipped'] + check['resultsUpdated']):
            LOGGER.debug("Body ID %s has already been processed", body_id)
            return
        payload = {"resultsFound": len(data['results']), "resultsUpdated": 0, "resultsSkipped": 0,
                   "filesFound": 0, "filesUpdated": 0}
        if ARG.WRITE:
            coll.update_one({"_id": mongo_id},
                            {"$set": payload})
    count = {"ffound": 0, "fupdated": 0, "rskipped": 0, "rupdated": 0}
    # Loop over files
    for match in data['results']:
        if 'sourceImageFiles' not in match:
            #LOGGER.warning("No sourceImageFiles for %s in %s", match['sampleName'], path)
            count['rskipped'] += 1
            if ARG.WRITE:
                coll.update_one({"_id": mongo_id},
                                {"$set": {"resultsSkipped": count['rskipped']}})
            continue
        match['maskPublishedName'] = body_id
        good = True
        for key in RENAME_COMPONENTS:
            if key not in match:
                good = False
                LOGGER.error("No %s for %s in %s", match['sampleName'], key, path)
        if not good:
            count['rskipped'] += 1
            if ARG.WRITE:
                coll.update_one({"_id": mongo_id},
                                {"$set": {"resultsSkipped": count['rskipped']}})
            continue
        count['ffound'] += len(match['sourceImageFiles'])
        if ARG.WRITE:
            coll.update_one({"_id": mongo_id},
                            {"$set": {"filesFound": count['ffound']}})
        for img_type, source_path in match['sourceImageFiles'].items():
            newname = '%s-%s-%s-%s' % tuple([match[key] for key in RENAME_COMPONENTS])
            newname += "-%s-%s.png" % (CDM_ALIGNMENT_SPACE, img_type.lower())
            if newname in filedict:
                LOGGER.error("Duplicate file name found for %s in %s", match['sampleName'], path)
                sys.exit(-1)
            filedict[newname] = 1
            if ARG.WRITE:
                write_file(source_path, newdir, newname)
                s3_target = '/'.join([CDM_ALIGNMENT_SPACE,
                                      re.sub('.*' + ARG.LIBRARY, ARG.LIBRARY, newdir),
                                      newname])
                upload_aws(s3_client, bucket, source_path, s3_target)
                count['fupdated'] += 1
                coll.update_one({"_id": mongo_id},
                                {"$set": {"filesUpdated": count['fupdated']}})
        count['rupdated'] += 1
        if ARG.WRITE:
            coll.update_one({"_id": mongo_id},
                            {"$set": {"resultsUpdated": count['rupdated'],
                                      "updatedDate": datetime.now()}})


def copy_files():
    """ Copy files specified in JSON files to /nrs/neuronbridge
        Keyword arguments:
          None
        Returns:
          None
    """
    #pylint: disable=no-member
    bucket = AWS['s3_bucket']['cdm']
    if ARG.MANIFOLD != 'prod':
        bucket += '-dev'
    if not ARG.LIBRARY:
        s3_client = initialize_s3()
        get_library(s3_client, bucket)
    if not os.access(NEURONBRIDGE_JSON_BASE, os.R_OK):
        LOGGER.critical("Can't read from %s", NEURONBRIDGE_JSON_BASE)
        sys.exit(-1)
    if not ARG.NEURONBRIDGE:
        get_nb_version()
    search_base = "%s/%s/pppresults/flyem-to-flylight" \
                  % (NEURONBRIDGE_JSON_BASE, ARG.NEURONBRIDGE)
    if ARG.FILE:
        with open('C:/path/numbers.txt') as bids:
            for line in file:
                line = line.strip()
                lines.append("/".join([search_base, line]))
    else:
        search_path = "/".join([search_base, "*.json"])
        if ARG.BODYID:
            search_path = search_path.replace("*", ARG.BODYID)
        json_files = glob(search_path)
    if len(json_files) == 1:
        handle_single_json_file(json_files[0])
        return
    print("Preparing Dask")
    parallel = []
    for path in tqdm(json_files):
        parallel.append(dask.delayed(handle_single_json_file)(path))
    print("Copying and uploading %d body IDs" % (len(parallel)))
    with ProgressBar():
        dask.compute(*parallel)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Produce denormalization files")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        help='Library')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', action='store',
                        help='NeuronBridge data version')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File of body IDs to process')
    PARSER.add_argument('--bodyid', dest='BODYID', action='store',
                        help='Body ID')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', help='AWS S3 manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write PNGs to local filesystem')
    PARSER.add_argument('--link', dest='LINK', action='store_true',
                        default=False, help='Use symlinks instead onn copying')
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
    copy_files()
