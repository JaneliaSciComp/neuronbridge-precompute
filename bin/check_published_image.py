''' This program will compare publishedImage counts to MySQL publishing database
'''

import argparse
import sys
from colorama import Fore
import colorlog
import requests
import MySQLdb
from pymongo import MongoClient


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
# Database
CONN = dict()
CURSOR = dict()
DBM = ''
READ = {"IMG": "SELECT alps_release,COUNT(1) AS cnt FROM image_data_mv i JOIN secondary_image_vw s "
               + "ON (i.id=s.image_id) WHERE alignment_space_unisex IS NOT NULL "
               + "AND s.product='aligned_jrc2018_unisex_hr_stack' GROUP BY 1"
       }
# pylint: disable=no-member, W0703

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
          err: error object
        Returns:
          None
    """
    try:
        msg = 'MySQL error [%d]: %s' % (err.args[0], err.args[1])
    except IndexError:
        msg = 'MySQL error: %s' % (err)
    terminate_program(msg)


def db_connect(dbd):
    """ Connect to a database
        Keyword arguments:
          dbd: database dictionary
        Returns:
          connector and cursor
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


def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
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
    if req.status_code == 400:
        try:
            if "error" in req.json():
                LOGGER.error("%s %s", url, req.json()["error"])
        except Exception as err:
            pass
        return False
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize
    """
    global CONFIG, DBM  # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    # Databases
    data = call_responder('config', 'config/db_config')
    if not ARG.QUICK:
        (CONN[ARG.DATABASE], CURSOR[ARG.DATABASE]) = \
            db_connect(data['config'][ARG.DATABASE][ARG.MANIFOLD])
    # Connect to Mongo
    LOGGER.info("Connecting to Mongo on %s", ARG.MONGO)
    rwp = 'read'
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


def process_imagery():
    """ Find representative iages from a publishing database
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DBM.publishedImage
    result = coll.aggregate([{"$group" : {"_id":"$releaseName", "count": {"$sum":1}}}])
    mongo = {}
    for rel in result:
        mongo[rel['_id']] = rel['count']
    for mkey in sorted(mongo):
        print(mkey, mongo[mkey])
    if ARG.QUICK:
        print("%-26s  %-6s" % ("Release", "Mongo"))
        total = 0
        for mkey in sorted(mongo):
            print("%-26s  %6d" % (mkey, mongo[mkey]))
            total += mongo[mkey]
        print("%-26s  %-6s" % ("-"*26, "-"*6))
        print("%-26s  %6d" % ("TOTAL", total))
    else:
        try:
            CURSOR[ARG.DATABASE].execute(READ["IMG"])
            rows = CURSOR[ARG.DATABASE].fetchall()
        except MySQLdb.Error as err:
            sql_error(err)
        total = {"mysql": 0, "mongo": 0}
        print("%-26s  %-6s  %-6s" % ("Release", "MySQL", "Mongo"))
        for row in rows:
            mcnt = mongo[row["alps_release"]] if row["alps_release"] in mongo else 0
            line = "%-26s  %-6d  %-6d" % (row["alps_release"], row["cnt"], mcnt)
            print((Fore.GREEN if row["cnt"] == mcnt else Fore.RED) + line)
            total["mysql"] += row["cnt"]
            total["mongo"] += mcnt
        print("%-26s  %-6s    %-6s" % ("-"*26, "-"*6, "-"*6))
        print("%-26s  %6d  %6d" % ("TOTAL", total["mysql"], total["mongo"]))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Compare publishedImage counts to MySQL publishing database")
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        default='mbew', choices=['mbew', 'gen1mcfo'], help='Database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='staging', choices=['dev', 'staging', 'prod'],
                        help='Publishing manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='dev', choices=['dev', 'prod', 'local'], help='Mongo manifold')
    PARSER.add_argument('--quick', dest='QUICK', action='store_true',
                        default=False, help='Do not crosscheck with publishing database')
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
    process_imagery()
    sys.exit(0)
