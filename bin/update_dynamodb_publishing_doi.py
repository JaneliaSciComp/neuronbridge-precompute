''' This program will update the janelia-neuronbridge-publishing-doi table on
    DynamoDB. Data is pulled from the Split-GAL4 and Gen1MCFO prod databases
    and NeuronBridge MongoDB. For EM data, Body IDs (in the form
    dataset:version:bodyid) are written. For LM data, publishing names are
    written.
    Note that Gen1 GAL4/LexAs are not yet supported (these are not yet in NeuronBridge).
'''

import argparse
import collections
import json
from operator import attrgetter
import re
import sys
import boto3
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,inconsistent-return-statements,logging-fstring-interpolation
# Configuration
GEN1_MCFO_DOI = "10.7554/eLife.80660"
CITATION = {}
DOI = {}
MAPPING = {}
# Database
DB = {}
ITEMS = []
PUBLISHING_DATABASE = ["mbew", "gen1mcfo", "raw"]
READ = {"LINES": "SELECT DISTINCT line,value AS doi,GROUP_CONCAT(DISTINCT original_line) AS olines "
                 + "FROM image_data_mv mv JOIN line l ON (l.name=mv.line) "
                 + "JOIN line_property lp ON (lp.line_id=l.id AND "
                 + "type_id=getCvTermId('line','doi',NULL)) GROUP BY 1,2"
       }
READ["LINESREL"] = READ["LINES"].replace("GROUP BY", "AND alps_release=%" + "s GROUP BY")
MONGODB = 'neuronbridge-mongo'
# General use
COUNT = collections.defaultdict(lambda: 0, {})

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def create_dynamodb_table(dynamodb, table):
    """ Create a DynamoDB table
        Keyword arguments:
          dynamodb: DynamoDB resource
          table: table name
        Returns:
          None
    """
    payload = {"TableName": table,
               "KeySchema": [{"AttributeName": "name", "KeyType": "HASH"}
                            ],
               "AttributeDefinitions": [{'AttributeName': 'name', 'AttributeType': 'S'}
                                       ],
               "BillingMode": "PAY_PER_REQUEST",
               "Tags": [{"Key": "PROJECT", "Value": "NeuronBridge"},
                        {"Key": "DEVELOPER", "Value": "svirskasr"},
                        {"Key": "STAGE", "Value": "prod"},
                        {"Key": "DESCRIPTION",
                         "Value": "Stores citations/DOIs for individual bodies/publishing names"}]
              }
    if ARG.WRITE:
        print(f"Creating DynamoDB table {table}")
        table = dynamodb.create_table(**payload)
        table.wait_until_exists()


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
    dbs = ['neuronbridge']
    if ARG.SOURCE != "em":
        dbs.extend(PUBLISHING_DATABASE)
    for source in dbs:
        rwp = 'write' if (ARG.WRITE and dbs == 'neuronbridge') else 'read'
        manifold = 'prod' if source == 'neuronbridge' else ARG.MANIFOLD
        dbo = attrgetter(f"{source}.{manifold}.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    # DynamoDB
    table = "janelia-neuronbridge-publishing-doi"
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    try:
        _ = dynamodb_client.describe_table(TableName=table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        LOGGER.warning("Table %s doesn't exist", table)
        create_dynamodb_table(dynamodb, table)
    LOGGER.info("Writing results to DynamoDB table %s", table)
    DB["DOI"] = dynamodb.Table(table)
    # Releases
    reldict = JRC.simplenamespace_to_dict(JRC.get_config("releases"))
    for _, rel in reldict.items():
        if 'doi' not in rel:
            continue
        if 'publication' in rel['doi']:
            CITATION[rel['doi']['publication']] = rel['citation']
        if 'preprint' in rel['doi']:
            CITATION[rel['doi']['preprint']] = rel['citation']


def from_crossref(rec):
    ''' Generate and print a Crossref citation
        Keyword arguments:
          rec: record from Crossref
        Returns:
          Citation
    '''
    message = rec['message']
    if 'author' not in message:
        LOGGER.critical("No author found")
        terminate_program(json.dumps(message, indent=4))
    author = message['author']
    first = []
    for auth in author:
        if auth["sequence"] == "first":
            first.append(auth["family"])
    if len(first) == 2:
        authors = f"{first[0]} & {first[1]}"
    else:
        authors = ", ".join(first)
    pub = message["created"]["date-parts"][0][0]
    return f"{authors}, et al., {pub}"


def from_datacite(doi):
    ''' Generate and print a DataCite citation
        Keyword arguments:
          doi: DOI
        Returns:
          Citation
    '''
    rec = JRC.call_datacite(doi)
    if not rec:
        terminate_program(f"{doi} is not on Crossref or DataCite")
    message = rec['data']['attributes']
    if 'creators' not in message:
        LOGGER.critical("No author found")
        terminate_program(json.dumps(message, indent=4))
    author = message['creators']
    first = []
    for auth in author:
        if not first:
            first.append(auth["familyName"])
    if len(first) == 2:
        authors = f"{first[0]} & {first[1]}"
    else:
        authors = ", ".join(first)
    year = str(message['publicationYear'])
    return f"{authors}, et al., {year}"


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
        if 'in prep' in doi or 'janelia' in doi:
            CITATION[doi] = doi
            LOGGER.info(f"Internal DOI: {doi}")
            return doi
        rec = JRC.call_crossref(doi)
        if rec:
            CITATION[doi] = from_crossref(rec)
            LOGGER.info(f"Crossref DOI: {doi}: {CITATION[doi]}")
        else:
            CITATION[doi] = from_datacite(doi)
            LOGGER.info(f"DataCite DOI: {doi}: {CITATION[doi]}")
    if not CITATION[doi]:
        terminate_program(f"No citation for {doi}")
    return CITATION[doi]


def write_dynamodb():
    ''' Write rows from ITEMS to DynamoDB in batch
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Batch writing {len(ITEMS):,} items to DynamoDB")
    with DB["DOI"].batch_writer() as writer:
        for item in tqdm(ITEMS, desc="DynamoDB"):
            if ARG.WRITE:
                writer.put_item(Item=item)
            COUNT["dynamodb"] += 1


def process_em_library(coll, library, count):
    """ Process a single EM library
        Keyword arguments:
          coll: MongoDB collection
          library: EM library
          count: number of body IDs
        Returns:
          None
    """
    if 'flywire' in library:
        result = re.search(r"(flywire_[^_]*)(_\d+)", library)
        lib = result[1]
        version = result[2][1:].replace("_", ".")
        prefix = ":".join([lib, 'v' + version])
    else:
        result = re.search(r"flyem_([^_]*)((_\d)+)", library)
        lib = result[1]
        version = "v" + result[2][1:].replace("_", ".")
        prefix = ":".join([lib, version])
    results = coll.find({"libraryName": library})
    if (lib not in EMDOI) or (not EMDOI[lib]):
        LOGGER.warning("Dataset %s is not associated with a DOI", prefix)
        return
    doi = EMDOI[lib]
    for row in tqdm(results, desc=prefix, total=count):
        COUNT['read'] += 1
        if prefix in str(row["publishedName"]):
            bid = str(row["publishedName"])
        else:
            if 'flywire' in prefix and 'flywire' in str(row["publishedName"]):
                bid = ":".join([prefix, str(row["publishedName"]).split(':')[-1]])
            else:
                bid = ":".join([prefix, str(row["publishedName"])])
        if bid not in MAPPING:
            MAPPING[bid] = doi
            payload = {"name": bid, "doi": []}
            if isinstance(doi, str):
                payload["doi"].append({"link": "/".join([SERVER.doi.address, doi]),
                                       "citation": get_citation(doi)})
            else:
                for ref in doi:
                    payload["doi"].append({"link": "/".join([SERVER.doi.address, ref]),
                                           "citation": get_citation(ref)})
            ITEMS.append(payload)


def process_em():
    """ Process EM libraries
        Keyword arguments:
          None
        Returns:
          None
    """
    payload = [{"$unwind": "$tags"},
               {"$project": {"_id": 0, "libraryName": 1, "tags": 1}},
               {"$group": {"_id": {"lib": "$libraryName", "tag": "$tags"},
                           "count":{"$sum": 1}}},
               {"$sort": {"_id.lib": 1, "_id.tag": 1}}
              ]
    coll = DB["neuronbridge"][ARG.EMSOURCE]
    results = coll.aggregate(payload)
    for row in results:
        library = row["_id"]["lib"]
        if library.startswith("flylight"):
            continue
        if ARG.RELEASE and library != ARG.RELEASE:
            continue
        LOGGER.info("Library %s %s", library, row["_id"]["tag"])
        process_em_library(coll, library, row["count"])


def process_single_lm_image(row, database):
    """ Process a single LM image
        Keyword arguments:
          row:      image row
          database: database name
        Returns:
          None
    """
    if row['line'] in MAPPING:
        if row['doi'] != MAPPING[row['line']]:
            LOGGER.error("DOI %s does not match previous %s for publishing name %s",
                         row['doi'], MAPPING[row['line']], row['line'])
    else:
        doi = row['doi']
        MAPPING[row['line']] = doi
        citation = get_citation(doi)
        payload = {"name": row['line'],
                   "doi": [{"citation": citation}]
                  }
        if 'in prep' not in doi:
            payload['doi'][0]['link'] = "/".join([SERVER.doi.address, doi])
        if database == 'gen1mcfo' and doi != GEN1_MCFO_DOI:
            payload["doi"].append({"link": "/".join([SERVER.doi.address, GEN1_MCFO_DOI]),
                                   "citation": get_citation(GEN1_MCFO_DOI)})
        ITEMS.append(payload)


def process_lm():
    """ Process specified LM datasets
        Keyword arguments:
          None
        Returns:
          None
    """
    for database in PUBLISHING_DATABASE:
        if ARG.RELEASE:
            if database == "gen1mcfo" and "Gen1 MCFO" not in ARG.RELEASE:
                continue
            if database != "gen1mcfo" and "Gen1 MCFO" in ARG.RELEASE:
                continue
            if database == "raw" and ARG.RELEASE != 'Split-GAL4 Omnibus Broad':
                continue
            if database != "raw" and ARG.RELEASE == 'Split-GAL4 Omnibus Broad':
                continue
        LOGGER.info("Fetching lines from %s", database)
        try:
            if ARG.RELEASE:
                DB[database]['cursor'].execute(READ["LINESREL"], (ARG.RELEASE,))
            else:
                DB[database]['cursor'].execute(READ["LINES"])
            rows = DB[database]['cursor'].fetchall()
            if ARG.RELEASE and not rows:
                terminate_program(f"{ARG.RELEASE} is not a valid release for FlyLight")
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        for row in tqdm(rows, desc=database):
            COUNT['read'] += 1
            process_single_lm_image(row, database)


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
    if ITEMS:
        write_dynamodb()
    print(f"Publishing names/body IDs read:   {COUNT['read']:,}")
    print(f"Unique publishing names/body IDs: {len(MAPPING):,}")
    print(f"Records written to DynamoDB:      {COUNT['dynamodb']:,}")


# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Update DynamoDB table janelia-neuronbridge-publishing-doi')
    PARSER.add_argument('--release', dest='RELEASE', default='', help='ALPS release or EM dataset')
    PARSER.add_argument('--source', dest='SOURCE', choices=['', 'em', 'lm'], default='',
                        help='Source release ([blank], em, or lm)')
    PARSER.add_argument('--table', dest='EMSOURCE', choices=['', 'em', 'lm'],
                        default='publishedURL',
                        help='Mongo table for EM data ([publishedURL] or neuronMetadata)')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['staging', 'prod'], default='prod',
                        help='MySQL manifold (staging, [prod])')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Write to DynamoDB')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    if ARG.RELEASE and not ARG.SOURCE:
        terminate_program("If you specify a release, you must also specify a source")
    REST = JRC.get_config("rest_services")
    SERVER = JRC.get_config("servers")
    initialize_program()
    try:
        EMDOI = JRC.simplenamespace_to_dict(JRC.get_config("em_dois"))
    except Exception as gerr:
        terminate_program(gerr)
    perform_mapping()
    terminate_program()
