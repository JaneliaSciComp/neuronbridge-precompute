''' convert_neuron_tiffs.py
    Convert searchabler neuron TIFFs into PNGs
'''

import argparse
from io import BytesIO
import json
import os
import re
import sys
import colorlog
import boto3
from botocore.exceptions import ClientError
import dask
from dask.callbacks import Callback
from PIL import Image
import requests
from simple_term_menu import TerminalMenu
from tqdm.auto import tqdm


__version__ = '0.0.2'
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
AWS = dict()
S3_SECONDS = 60 * 60 * 12
CDM_ALIGNMENT_SPACE = 'JRC2018_Unisex_20x_HR'


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
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
    global AWS, CONFIG # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/aws')
    AWS = data['config']


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


def get_keyfile(client, bucket):
    if not ARG.LIBRARY:
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
    if ARG.KEYFILE:
        with open(ARG.KEYFILE) as kfile:
            data = json.load(kfile)
        return data
    print(ARG.LIBRARY)
    key = CDM_ALIGNMENT_SPACE + '/' + ARG.LIBRARY + '/searchable_neurons/keys_denormalized.json'
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
    except ClientError as err:
        LOGGER.error(key)
        LOGGER.critical(err)
    except Exception as err:
        LOGGER.error(key)
        LOGGER.critical(err)
    data = json.loads(content.decode())
    return data


def convert_img(img, newname):
    ''' Convert file to PNG format
        Keyword arguments:
          img: PIL image object
          newname: new file name
        Returns:
          New filepath
    '''
    LOGGER.debug("Converting %s", newname)
    newpath = '/tmp/pngs/' + newname
    img.save(newpath, 'PNG')
    return newpath


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


def convert_single_file(bucket, key):
    s3_client = initialize_s3()
    try:
        s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
        object_content = s3_response_object['Body'].read()
        data_bytes_io = BytesIO(object_content)
        img = Image.open(data_bytes_io)
    except Exception as err:
        LOGGER.critical(err)
    if img.format != 'TIFF':
        LOGGER.error("%s is not a TIFF file", key)
    file = key.split('/')[-1].replace('.tif', '.png')
    tmp_path = convert_img(img, file)
    upload_path = re.sub(r'searchable_neurons.*', 'searchable_neurons/pngs/', key)
    if ARG.AWS:
        upload_aws(s3_client, bucket, tmp_path, upload_path + file)
        os.remove(tmp_path)


class ProgressBar(Callback):
    def _start_state(self, dsk, state):
        self._tqdm = tqdm(total=sum(len(state[k]) for k in ['ready', 'waiting',
                                                            'running', 'finished']),
                          colour='green')

    def _posttask(self, key, result, dsk, state, worker_id):
        self._tqdm.update(1)

    def _finish(self, dsk, state, errored):
        pass


def convert_tiffs():
    """ Denormalize a bucket into a JSON file
        Keyword arguments:
          None
        Returns:
          None
    """
    #pylint: disable=no-member
    s3_client = initialize_s3()
    bucket = "janelia-flylight-color-depth"
    if ARG.MANIFOLD != 'prod':
        bucket += '-dev'
    data = get_keyfile(s3_client, bucket)
    parallel = []
    LOGGER.info("Preparing Dask")
    for key in data:
        if '.tif' not in key.lower():
            continue
        parallel.append(dask.delayed(convert_single_file)(bucket, key))
    print("Creating %sPNGs" % ('and uploading ' if ARG.AWS else ''))
    with ProgressBar():
        dask.compute(*parallel)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Produce denormalization files")
    PARSER.add_argument('--library', dest='LIBRARY', action='store',
                        help='Library')
    PARSER.add_argument('--keyfile', dest='KEYFILE', action='store',
                        help='AWS S3 key file')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', help='AWS S3 manifold')
    PARSER.add_argument('--aws', dest='AWS', action='store_true',
                        default=False, help='Write PNGs to S3')
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
    convert_tiffs()
