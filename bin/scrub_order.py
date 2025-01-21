''' This program will scrub an upload order file of searchable_neurons using a manifest
'''
__version__ = '1.0.0'

import argparse
import re
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint: disable=logging-fstring-interpolation

MANIFEST = {}


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


def scrub_order_file():
    ''' Scrub an order file to produce a new order file
        Keyword arguments:
          None
        Returns:
          None
    '''
    pattern = re.compile(r"/searchable_neurons/\d+/")
    LOGGER.info(f"Reading manifest {ARG.MANIFEST}")
    with open(ARG.MANIFEST, 'r', encoding='ascii') as instream:
        rows = instream.read().splitlines()
        for key in tqdm(rows):
            if 'searchable_neurons' in key:
                result = pattern.search(key)
                if result:
                    key = re.sub(r"searchable_neurons/\d+", "searchable_neurons/partition", key)
            MANIFEST[key] = True
    # Key count might be less than lines in manifest due to files being
    # in more than one partition
    LOGGER.info(f"Keys in manifest {len(MANIFEST):,}")
    LOGGER.info(f"Processing {ARG.FILE}")
    out = []
    with open(ARG.FILE, 'r', encoding='ascii') as instream:
        rows = instream.read().splitlines()
        for row in tqdm(rows):
            key = row.split("\t")[-1].replace("janelia-flylight-color-depth/", "")
            result = pattern.search(key)
            if result:
                key = re.sub(r"searchable_neurons/\d+", "searchable_neurons/partition", key)
            if key not in MANIFEST:
                out.append(row)
    LOGGER.info(f"Keys in new order file: {len(out):,}")
    if out:
        with open(ARG.FILE + ".scrubbed", 'w', encoding='ascii') as outstream:
            for row in tqdm(out):
                outstream.write(f"{row}\n")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Scrub order file")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='', help='Order file')
    PARSER.add_argument('--manifest', dest='MANIFEST', action='store',
                        default='janelia-flylight-color-depth_manifest.txt',
                        help='Manifest file')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    scrub_order_file()
    terminate_program()
