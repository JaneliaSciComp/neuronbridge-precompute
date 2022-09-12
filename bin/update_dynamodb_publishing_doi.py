''' This program will update the janelia-neuronbridge-publishing-doi table on DynamoDB.
    Data is pulled from the Split-GAL4 and Gen1MCFO prod databases and [production] NeuPrint.
    For EM data, Body IDs (in the form dataset#bodyid) are written. FOr LM data, publishing
    names are written.
    Note that Gen1 GAL4/LexAs are not yet supported (these are not yet in NeuronBridge).
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
CONFIG = {'config': {'url': os.environ.get('CONFIG_SERVER_URL')}}
CITATION = {}
DOI = {}
EMDOI = {}
MAPPING = {}
SERVER = {}
# Database
PUBLISHING_DATABASE = ["mbew", "gen1mcfo"]
DATABASE = {}
CONN = {}
CURSOR = {}
READ = {"LINES": "SELECT DISTINCT line,value AS doi,GROUP_CONCAT(DISTINCT original_line) AS olines "
                 + "FROM image_data_mv mv JOIN line l ON (l.name=mv.line) "
                 + "JOIN line_property lp ON (lp.line_id=l.id AND "
                 + "type_id=getCvTermId('line','doi',NULL)) GROUP BY 1,2"
       }
READ["LINESREL"] = READ["LINES"].replace("GROUP BY", "AND alps_release=%" + "s GROUP BY")
# General use
COUNT = {"dynamodb": 0, "read": 0}


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
    """ Log a critical SQL error and exit
        Keyword arguments:
          err: error text
        Returns:
          None
    """
    try:
        terminate_program(f"MySQL error {err.args[0]}: {err.args[1]}")
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
    for key, val in call_responder('config', 'config/rest_services')['config'].items():
        CONFIG[key] = val
    for key, val in call_responder('config', 'config/servers')['config'].items():
        SERVER[key] = val
    for key, val in call_responder('config', 'config/dois')['config'].items():
        DOI[key] = val
    for key, val in call_responder('config', 'config/em_dois')['config'].items():
        EMDOI[key] = val
    # MySQL
    data = call_responder('config', 'config/db_config')
    for pdb in PUBLISHING_DATABASE:
        (CONN[pdb], CURSOR[pdb]) = db_connect(data['config'][pdb][ARG.MANIFOLD])
    # DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    DATABASE["DOI"] = dynamodb.Table('janelia-neuronbridge-publishing-doi')


def get_citation(doi):
    """ Return the citation for a DOI.
        The citation will be the first author(s)
        followed by "et al." and the publication date.
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
    """ Write a record to DynamoDB. This will create a new record or update an existing one.
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
    """ Set up a NeuPrint data set for use
        Keyword arguments:
          dataset: data set
        Returns:
          name: data set name
    """
    LOGGER.info("Initializing Client for %s %s", SERVER["neuprint"]["address"], dataset)
    npc = Client(SERVER["neuprint"]["address"], dataset=dataset)
    set_default_client(npc)
    if ':' in dataset:
        name, _ = dataset.split(':')
    else:
        name = dataset
    return name


def process_em_dataset(dataset):
    """ Process a single EM data set
        Keyword arguments:
          dataset: data set
        Returns:
          None
    """
    dsname = setup_dataset(dataset)
    if (dsname not in EMDOI) or (not EMDOI[dsname]):
        LOGGER.warning(f"Dataset {dsname} is not associated with a DOI")
        return
    query = """
    MATCH (n: Neuron)
    RETURN n.bodyId as bodyId,n.status as status,n.statusLabel as label,n.type as type,n.instance as instance,n.size as size
    ORDER BY n.type, n.instance
    """
    results = fetch_custom(query, format='json')
    LOGGER.info("%d Body IDs found in NeuPrint %s", len(results['data']), dataset)
    for row in tqdm(results['data'], desc=dataset):
        COUNT['read'] += 1
        bid = "#".join([dsname, str(row[0])])
        doi = EMDOI[dsname]
        if bid not in MAPPING:
            MAPPING[bid] = doi
            payload = {"name": bid,
                       "doi": "/".join([SERVER["doi"]["address"], doi]),
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
    response = call_responder('neuprint', 'dbmeta/datasets', True)
    datasets = list(response.keys())
    if ARG.RELEASE:
        if ARG.RELEASE not in datasets:
            terminate_program(f"{ARG.RELEASE} is not a valid dataset for FlyEM")
        process_em_dataset(ARG.RELEASE)
        return
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
                   "doi": "/".join([SERVER["doi"]["address"], row['doi']]),
                   "citation": get_citation(row['doi'])
                  }
        write_dynamodb(payload)


def process_lm():
    """ Process specified LM datasets
        Keyword arguments:
          None
        Returns:
          None
    """
    for database in PUBLISHING_DATABASE:
        LOGGER.info("Fetching lines from %s", database)
        try:
            if ARG.RELEASE:
                CURSOR[database].execute(READ["LINESREL"], (ARG.RELEASE,))
            else:
                CURSOR[database].execute(READ["LINES"])
            rows = CURSOR[database].fetchall()
            if ARG.RELEASE and not rows:
                terminate_program(f"{ARG.RELEASE} is not a valid release for FlyLight")
        except MySQLdb.Error as err:
            sql_error(err)
        for row in tqdm(rows, desc=database):
            COUNT['read'] += 1
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
    print(f"Publishing names/body IDs read:   {COUNT['read']}")
    print(f"Unique publishing names/body IDs: {len(MAPPING)}")
    print(f"Records written to DynamoDB:      {COUNT['dynamodb']}")


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Update DynamoDB table janelia-neuronbridge-publishing-doi')
    PARSER.add_argument('--release', dest='RELEASE', default='', help='ALPS release or EM dataset')
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
    if ARG.RELEASE and not ARG.SOURCE:
        terminate_program("If you specify a release, you must also specify a source")
    initialize_program()
    perform_mapping()
    terminate_program()
