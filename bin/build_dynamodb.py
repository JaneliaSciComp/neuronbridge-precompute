''' Create and display commands needed to populate DynamoDB
'''

import argparse
from glob import glob
import json
from os.path import exists
import re
import sys
import colorlog
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm

# pylint: disable=no-member
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
NEURONBRIDGE = dict()

def call_responder(server, endpoint):
    """ Call a REST API
        Keyword arguments:
          server: server name
          endpoint: endpoint
        Returns:
          JSON
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    global CONFIG, NEURONBRIDGE # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/neuronbridge_data_version')
    NEURONBRIDGE = data['config']


def get_nb_version():
    """ Prompt the user for a NeuronBridge version from subdirs in the base dir
        Keyword arguments:
          None
        Returns:
          None (sets ARG.NEURONBRIDGE)
    """
    base_path = NEURONBRIDGE["base_path"]
    version = [re.sub('.*/', '', path)
               for path in glob(base_path + '/v[0-9]*')]
    print("Select a NeuronBridge data version:")
    version.sort()
    terminal_menu = TerminalMenu(version)
    chosen = terminal_menu.show()
    if chosen is None:
        LOGGER.error("No NeuronBridge data version selected")
        sys.exit(0)
    ARG.NEURONBRIDGE = version[chosen]


def create_name_file(path):
    """ Create a "names" file
        Keyword arguments:
          path: path for names file
        Returns:
          None
    """
    new_path = path.replace(".json", ".names")
    LOGGER.info("Creating %s", new_path)
    with open(path) as jsonfile:
        data = json.load(jsonfile)
    line = dict()
    for smp in tqdm(data, desc="Building names file"):
        if "publishedName" in smp:
            line[smp['publishedName']] = True
    with open(new_path, 'w') as namefile:
        for pname in sorted(line):
            namefile.write("%s\n" % (pname))


def run_populate():
    """ Crreate commands needed to populate DynamoDB
        Keyword arguments:
          None
        Returns:
          None
    """
    if not ARG.NEURONBRIDGE:
        get_nb_version()
    print("Run the following commands in sequence:")
    command = list()
    cmd = "  python3 populate_published.py --neuronbridge %s --result ppp --action index" \
          % (ARG.NEURONBRIDGE)
    command.append(cmd + " -write")
    print(cmd)
    for filedesc in NEURONBRIDGE["versions"][ARG.NEURONBRIDGE]:
        version, lib = filedesc["file"].split("/")
        if filedesc["type"] == "LM":
            load = lib.replace(".json", ".names")
            if not exists("/".join([NEURONBRIDGE["base_path"], version, load])):
                create_name_file("/".join([NEURONBRIDGE["base_path"], filedesc["file"]]))
        else:
            load = lib
        cmd = "  python3 populate_published.py --library " \
              + "%s --neuronbridge %s --result cdm --type %s" \
              % (load, ARG.NEURONBRIDGE, filedesc["type"])
        command.append(cmd + " -write")
        print(cmd)
    cmd = "  python3 populate_published.py --neuronbridge %s --result ppp --action populate" \
          % (ARG.NEURONBRIDGE)
    command.append(cmd + " -write")
    print(cmd)
    print("If there are no errors, run the commands in sequence with write mode enabled")
    for cmd in command:
        print(cmd)

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Populate a NeuronBridge DynamoDB table')
    PARSER.add_argument('--neuronbridge', dest='NEURONBRIDGE', action='store',
                        help='NeuronBridge data version')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
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
    run_populate()
    sys.exit(0)
