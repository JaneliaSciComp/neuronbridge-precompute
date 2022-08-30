''' This program will update the janelia-neuronbridge-publishing-doi table on DynamoDB.
    Data is pulled from the Split-GAL4 and Gen1MCFO prod databases and [production] NeuPrint.
'''

import argparse
import os
import sys
import boto3
import colorlog
import MySQLdb
from neuprint import Client, fetch_custom, set_default_client
import requests
from tqdm import tqdm

# pylint: disable=R1710, W0703
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
CITATION = {}
DOI = {}
MAPPING = {}
SERVER = {}
DOI_BASE = "https://doi.org"
# Database
DATABASE = {}
CONN = {}
CURSOR = {}
READ = {"LINES": "SELECT DISTINCT line,value AS doi,GROUP_CONCAT(DISTINCT original_line) AS olines "
                 + "FROM image_data_mv mv JOIN line l ON (l.name=mv.line) "
                 + "JOIN line_property lp ON (lp.line_id=l.id AND "
                 + "type_id=getCvTermId('line','doi',NULL)) GROUP BY 1,2",
        "LINESREL": "SELECT DISTINCT line,value AS doi,GROUP_CONCAT(DISTINCT original_line) AS "
                    + "olines FROM image_data_mv mv JOIN line l ON (l.name=mv.line) "
                    + "JOIN line_property lp ON (lp.line_id=l.id AND "
                    + "type_id=getCvTermId('line','doi',NULL)) AND alps_release=%s GROUP BY 1,2"
       }
# General use
COUNT = {"dynamodb": 0}


def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        terminate_program(f"MySQL error [{err.args[0]}]: {err.args[1]}")
    except IndexError:
        terminate_program(f"MySQL error: {err}")


def call_responder(server, endpoint, authenticate=False):
    """ Call a REST API
        Keyword arguments:
          server: server name
          endpoint: endpoint
          authenticate: authenticate to server
        Returns:
          JSON
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        if authenticate:
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer " + os.environ["NEUPRINT_JWT"]}
            req = requests.get(url, headers=headers)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Status: {str(req.status_code)}")


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


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    data = call_responder('config', 'config/rest_services')
    for key in data['config']:
        CONFIG[key] = data['config'][key]
    data = call_responder('config', 'config/servers')
    for key in data['config']:
        SERVER[key] = data['config'][key]
    data = call_responder('config', 'config/dois')
    for key in data['config']:
        DOI[key] = data['config'][key]
    data = call_responder('config', 'config/db_config')
    (CONN['sage'], CURSOR['sage']) = db_connect(data['config']['sage']['prod'])
    (CONN['mbew'], CURSOR['mbew']) = db_connect(data['config']['mbew']['staging'])
    (CONN['gen1mcfo'], CURSOR['gen1mcfo']) = db_connect(data['config']['gen1mcfo']['staging'])
    #(CONN['flew'], CURSOR['flew']) = db_connect(data['config']['flew']['prod'])
    # DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    DATABASE["DOI"] = dynamodb.Table('janelia-neuronbridge-publishing-doi')


def get_citation(doi):
    """ Return the citation for a DOI
        Keyword arguments:
          doi: DOI
        Returns:
          Citation
    """
    if doi not in CITATION:
        first = []
        for auth in DOI[doi]["author"]:
            if auth["sequence"] == "first":
                first.append(auth["family"])
        if len(first) == 2:
            authors = f"{first[0]} & {first[1]}"
        else:
            authors = ", ".join(first)
        pub = DOI[doi]["created"]["date-parts"][0][0]
        CITATION[doi] = f"{authors}, et al., {pub}"
    return CITATION[doi]


def write_dynamodb(payload):
    """ Write a record to DynamoDB
        Keyword arguments:
          doi: payload
        Returns:
          None
    """
    if ARG.WRITE:
        response = DATABASE["DOI"].put_item(Item=payload)
        if 'ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] == 200:
            COUNT['dynamodb'] += 1
    else:
        COUNT['dynamodb'] += 1


def setup_dataset(dataset):
    """ Insert or update a data set in Mongo
        Keyword arguments:
          dataset: data set
        Returns:
          last_uid: last UID assigned
          action: what to do with bodies in this data set (ignore, insert, or update)
    """
    LOGGER.info("Initializing Client for %s %s", SERVER["neuprint"]["address"], dataset)
    npc = Client(SERVER["neuprint"]["address"], dataset=dataset)
    set_default_client(npc)
    if ':' in dataset:
        name, version = dataset.split(':')
        version = version.replace('v', '')
    else:
        name = dataset
        version = ''
    return name, version


def process_em_dataset(dataset):
    """ Process a single EM data set
        Keyword arguments:
          dataset: data set
        Returns:
          None
    """
    dsname, dsver = setup_dataset(dataset)
    query = """
    MATCH (n: Neuron)
    RETURN n.bodyId as bodyId,n.status as status,n.statusLabel as label,n.type as type,n.instance as instance,n.size as size
    ORDER BY n.type, n.instance
    """
    results = fetch_custom(query, format='json')
    LOGGER.info("%d Body IDs found in NeuPrint %s", len(results['data']), dataset)
    for row in tqdm(results['data'], desc=dataset):
        bid = "#".join([dsname, str(row[0])])
        doi = "10.7554/eLife.57685" #PLUG
        if bid not in MAPPING:
            MAPPING[bid] = doi
            payload = {"name": bid,
                       "doi": "/".join([DOI_BASE, doi]),
                       "citation": get_citation(doi)
                      }
            write_dynamodb(payload)


def process_em():
    """ Process specified EM datasets
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.RELEASE:
        process_em_dataset(ARG.RELEASE)
        return
    response = call_responder('neuprint', 'dbmeta/datasets', True)
    datasets = list(response.keys())
    datasets.reverse()
    for dataset in datasets:
        process_em_dataset(dataset)


def process_single_lm_image(row):
    """ Process a single LM image
        Keyword arguments:
          row: image row
        Returns:
          None
    """
    if row['line'] in MAPPING:
        LOGGER.error("DOI %s does not match previous %s for publishing name %s",
                     row['doi'], MAPPING[row['line']], row['line'])
    else:
        MAPPING[row['line']] = row['doi']
        payload = {"name": row['line'],
                   "doi": "/".join([DOI_BASE, row['doi']]),
                   "citation": get_citation(row['doi']),
                   "original_lines": row['olines'].split(",")
                  }
        write_dynamodb(payload)


def process_lm():
    """ Process specified LM datasets
        Keyword arguments:
          None
        Returns:
          None
    """
    for database in ["mbew", "gen1mcfo"]:
        LOGGER.info("Fetching lines from %s", database)
        try:
            if ARG.RELEASE:
                CURSOR[database].execute(READ["LINESREL"], (ARG.RELEASE,))
            else:
                CURSOR[database].execute(READ["LINES"])
            rows = CURSOR[database].fetchall()
        except MySQLdb.Error as err:
            sql_error(err)
        for row in tqdm(rows, desc=database):
            process_single_lm_image(row)


def perform_mapping():
    """ Map publishing names to DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    if ARG.SOURCE != "lm":
        process_em()
    if ARG.SOURCE != "em":
        process_lm()
    print(f"Unique publishing names:     {len(MAPPING)}")
    print(f"Records written to DynamoDB: {COUNT['dynamodb']}")


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Update DynamoDB table janelia-neuronbridge-publishing-doi')
    PARSER.add_argument('--release', dest='RELEASE', default='', help='ALPS release')
    PARSER.add_argument('--source', dest='SOURCE', choices=['', 'em', 'lm'], default='',
                        help='Source release (em or lm)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['staging', 'prod'], default='staging', help='Manifold')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write to DynamoDB')
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
    if ARG.RELEASE and not ARG.SOURCE:
        terminate_program("If you specify a release, you must also specify a source")
    if ARG.SOURCE and not ARG.RELEASE:
        terminate_program("If you specify a source, you must also specify a release")
    initialize_program()
    perform_mapping()
    terminate_program()