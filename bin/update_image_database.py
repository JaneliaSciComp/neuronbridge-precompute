''' update_image_database.py
    Update the MongoDB publishedImage collection
'''

import argparse
from datetime import datetime
import sys
import time
import colorlog
import MySQLdb
from pymongo import MongoClient
import requests
from tqdm import tqdm
import neuronbridge_lib as NB

__version__ = '0.0.1'
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# Database
DATABASE = ["gen1mcfo", "mbew"]
CONN = dict()
CURSOR = dict()
DBM = ''
INSERT_BATCH = 5000
READ = {"PRIMARY": "SELECT slide_code,alignment_space_unisex,objective,s.url,"
                   + "line,original_line,area,tile,workstation_sample_id,alps_release,"
                   + "s.product FROM image_data_mv i JOIN secondary_image_vw s ON "
                   + "(i.id=s.image_id) WHERE alignment_space_unisex IS NOT NULL "
                   + "AND s.product='aligned_jrc2018_unisex_hr_stack' ORDER BY 1,3"
       }
# General
CURRENT = dict()
JACS_KEYS = dict()
LAST_UID = None
COUNT = {"insert": 0, "skip": 0, "update": 0}
# pylint: disable=W0703,E1101

# -------------------------------------------------------------------------------

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
          err: error message
        Returns:
           None
    """
    try:
        msg = 'MySQL error [%d]: %s' % (err.args[0], err.args[1])
    except IndexError:
        msg = 'MySQL error: %s' % (err)
    terminate_program(msg)


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
        terminate_program('Status: %s (%s)' % (str(req.status_code), url))
    return req.json()


def db_connect(dbd):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
        Returns:
          None
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
    """ Initialize program
    """
    global CONFIG, DATABASE, DBM, PRODUCT # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/imagery_product')
    PRODUCT = data['config']
    data = call_responder('config', 'config/db_config')
    # Connect to external publishing databases
    if ARG.DATABASE:
        DATABASE = [ARG.DATABASE]
    for dbn in DATABASE:
        (CONN[dbn], CURSOR[dbn]) = db_connect(data['config'][dbn][ARG.MANIFOLD])
    # Connect to Mongo
    LOGGER.info("Connecting to Mongo on %s", ARG.MONGO)
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        if ARG.MONGO == 'prod':
            client = MongoClient(data['config']['jacs-mongo'][ARG.MONGO][rwp]['host'],
                                 replicaSet='replWorkstation')
        elif ARG.MONGO == 'local':
            client = MongoClient()
        else:
            client = MongoClient(data['config']['jacs-mongo'][ARG.MONGO][rwp]['host'])
        DBM = client.jacs
        if ARG.MONGO == 'prod':
            DBM.authenticate(data['config']['jacs-mongo'][ARG.MONGO][rwp]['user'],
                             data['config']['jacs-mongo'][ARG.MONGO][rwp]['password'])
    except Exception as err:
        terminate_program('Could not connect to Mongo: %s' % (err))


def get_short_objective(obj):
    """ Shorten an objective by retuning just the magnification factor
        Keyword arguments:
          obj: full objective
        Returns:
          Shortened objective
    """
    obj = obj.lower()
    for tobj in ('20x', '40x', '63x'):
        if tobj in obj:
            return tobj
    terminate_program("Could not find objective from %s" % (obj))


def get_all_current():
    """ Populate CURRENT with documents in the colection
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info("Getting current database information")
    coll = DBM.publishedImage
    if ARG.RELEASE:
        payload = {"releaseName": ARG.RELEASE}
    else:
        payload = {}
    rows = coll.find(payload)
    for row in rows:
        key = "-".join([row["slideCode"], row["objective"],
                        row["alignmentSpace"]])
        CURRENT[key] = row
    LOGGER.info("Found %d rows in current database", len(CURRENT))


def set_payload(row, key):
    """ Set the payload for a new document in Mongo
        Keyword arguments:
          row: row
          update: update flag
        Returns:
          Payload dictionary
    """
    global LAST_UID # pylint: disable=W0603
    update = key in CURRENT
    dtm = datetime.now()
    if update:
        payload = CURRENT[key]
    else:
        next_uid = NB.generate_jacs_uid(last_uid=LAST_UID)
        if next_uid in JACS_KEYS:
            terminate_program("%d is a duplicate key" % (next_uid))
        else:
            JACS_KEYS[next_uid] = True
            LAST_UID = next_uid
        time.sleep(.0005)
        payload = {"_id": next_uid,
                   "name": row["line"],
                   "line": row["line"],
                   "originalLine": row["original_line"],
                   "area": row["area"],
                   "tile": row["tile"],
                   "sampleRef" : "Sample#" + row["workstation_sample_id"],
                   "releaseName": row["alps_release"],
                   "slideCode": row["slide_code"],
                   "objective": row["objective"],
                   "alignmentSpace": row["alignment_space_unisex"],
                   "ownerKey": "group:flylight",
                   "readers": ["group:flylight"],
                   "writers": ["group:flylight"],
                   "class": "org.janelia.model.domain.sample.PublishedImage",
                   "creationDate": dtm
                  }
    payload["updateDate"] = dtm
    return payload


def compare_images(coll, key, row, payload):
    """ Compare image records from MySQL and Mongo
        Keyword arguments:
          coll: collection
          key: document key
          row: row
          payload: MySQL record
        Returns:
          None
    """
    update_db = False
    for itype in ["aligned_jrc2018_unisex_hr_stack"]:
        if CURRENT[key]["files"][PRODUCT["sage"][itype]] == row["url"]:
            COUNT["skip"] += 1
        else:
            update_db = True
            newpayload["files"] = {PRODUCT["sage"][itype]: row["url"]}
    if "ownerKey" not in payload:
        update_db = True
        newpayload = {"name": payload["line"],
                      "ownerKey": "group:flylight",
                      "readers": ["group:flylight"],
                      "writers": ["group:flylight"]}
    if update_db:
        newpayload["updateDate"] = datetime.now()
        COUNT["update"] += 1
        if ARG.WRITE:
            coll.update_one({"_id": payload["_id"]}, {"$set": newpayload})


def process_db(dbn):
    """ Process a single MySQL database
        Keyword arguments:
          dbn: database name
        Returns:
          None
    """
    LOGGER.info("Selecting images from %s", dbn)
    if ARG.RELEASE:
        READ["PRIMARY"] = READ["PRIMARY"].replace("ORDER BY",
                                                  " AND alps_release='%s' ORDER BY" % (ARG.RELEASE))
    try:
        CURSOR[dbn].execute(READ["PRIMARY"])
        rows = CURSOR[dbn].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    LOGGER.info("Found %d images", len(rows))
    coll = DBM.publishedImage
    icounter = 0
    payload_list = list()
    for row in tqdm(rows, desc=dbn):
        row["objective"] = get_short_objective(row["objective"])
        key = "-".join([row["slide_code"], row["objective"], row["alignment_space_unisex"]])
        payload = set_payload(row, key)
        if key in CURRENT:
            compare_images(coll, key, row, payload)
        else:
            if row["product"] not in PRODUCT["sage"]:
                terminate_program("Key %s is not in the imagery_product config", row["product"])
            payload["files"] = {PRODUCT["sage"][row["product"]]: row["url"]}
            if icounter == INSERT_BATCH:
                if ARG.WRITE:
                    LOGGER.debug("Writing %d records", len(payload_list))
                    result = coll.insert_many(payload_list)
                    COUNT["insert"] += len(result.inserted_ids)
                else:
                    COUNT["insert"] += icounter
                icounter = 0
                payload_list = list()
            payload_list.append(payload)
            icounter += 1
    if icounter:
        if ARG.WRITE:
            LOGGER.debug("Writing %d records", len(payload_list))
            result = coll.insert_many(payload_list)
            COUNT["insert"] += len(result.inserted_ids)
        else:
            COUNT["insert"] += icounter
    if ARG.WRITE and not CURRENT:
        LOGGER.info("Creating index")
        resp = coll.create_index([("slideCode", 1),
                                  ("objective", 1),
                                  ("alignmentSpace", 1)
                                 ])
        print("index response:", resp)


def create_database():
    """ Process MySQL databases to produce a MongoDB collection
        Keyword arguments:
          None
        Returns:
          None
    """
    get_all_current()
    for dbn in DATABASE:
        process_db(dbn)
    print(COUNT)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Get NeuPrint metadata")
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        help='Release [optional]')
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        choices=['gen1mcfo', 'mbew'], default='', help='Database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod', 'staging'], default='prod',
                        help='Manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        choices=['dev', 'prod', 'local'], default='dev',
                        help='Mongo manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to Mongo')
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
    create_database()
    terminate_program()
