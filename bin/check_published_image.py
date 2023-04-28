''' This program will compare publishedImage counts to MySQL publishing database
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
import MySQLdb
from simple_term_menu import TerminalMenu
from common_lib import setup_logging, get_config, connect_database


# Configuration
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
# Database
DBM = {}
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
        msg = f"MySQL error [{err.args[0]}]: { err.args[1]}"
    except IndexError:
        msg = f"MySQL error: {err}"
    terminate_program(msg)


def get_parms():
    """ Get parameters
        Keyword arguments:
          None
        Returns:
          None
    """
    choices = ['mbew', 'gen1mcfo']
    if not ARG.DATABASE:
        terminal_menu = TerminalMenu(choices, title="Select a publishing database")
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.critical("You must select a publishing database")
            terminate_program(-1)
        ARG.DATABASE = choices[chosen]
    choices = ['staging', 'prod', 'dev']
    if not ARG.MANIFOLD:
        terminal_menu = TerminalMenu(choices, title="Select a manifold")
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.critical("You must select a manifold")
            terminate_program(-1)
        ARG.MANIFOLD = choices[chosen]
    choices = ['prod', 'dev', 'local']
    if not ARG.MONGO:
        terminal_menu = TerminalMenu(choices, title="Select a MongoDB instance")
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.critical("You must select a MongoDB instance")
            terminate_program(-1)
        ARG.MONGO = choices[chosen]


def initialize_program():
    """ Initialize
    """
    # Get parms
    get_parms()
    # Databases
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = get_config("databases")
    except Exception as err:
        terminate_program(err)
    if not ARG.QUICK:
        dbo = attrgetter(f"{ARG.DATABASE}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DBM[ARG.DATABASE] = connect_database(dbo)
        except MySQLdb.Error as err:
            terminate_program(err)
    dbo = attrgetter(f"jacs.{ARG.MONGO}.read")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MONGO, dbo.host, dbo.user)
    try:
        DBM["jacs"] = connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def process_imagery():
    """ Find representative iages from a publishing database
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        coll = DBM["jacs"].publishedImage
        result = coll.aggregate([{"$group" : {"_id":"$releaseName", "count": {"$sum":1}}}])
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))
    mongo = {}
    for rel in result:
        mongo[rel['_id']] = rel['count']
    if ARG.QUICK:
        print(f"{'Release':<26}  Mongo")
        total = 0
        for mkey in sorted(mongo):
            print(f"{mkey:<26}  {mongo[mkey]:>6}")
            total += mongo[mkey]
        print(f"{'-'*26}  {'-'*6}")
        print(f"{'TOTAL':<26}  {total:>6}")
    else:
        try:
            DBM[ARG.DATABASE]["cursor"].execute(READ["IMG"])
            rows = DBM[ARG.DATABASE]["cursor"].fetchall()
        except MySQLdb.Error as err:
            sql_error(err)
        total = {"mysql": 0, "mongo": 0}
        print(f"{'Release':<26}  {'MySQL':6}  {'Mongo':6}")
        for row in rows:
            if not row["alps_release"]:
                continue
            mcnt = mongo[row["alps_release"]] if row["alps_release"] in mongo else 0
            line = f"{row['alps_release']:<26}  {row['cnt']:>6}  {mcnt:>6}"
            print((Fore.GREEN if row["cnt"] == mcnt else Fore.RED) + line)
            total["mysql"] += row["cnt"]
            total["mongo"] += mcnt
        print(Style.RESET_ALL + f"{'-'*26}  {'-'*6}  {'-'*6}")
        line = f"{'TOTAL':<26}  {total['mysql']:>6}  {total['mongo']:>6}"
        print((Fore.GREEN if total["mysql"] == total["mongo"] else Fore.RED) + line)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Compare publishedImage counts to MySQL publishing database")
    PARSER.add_argument('--database', dest='DATABASE', action='store',
                        choices=['mbew', 'gen1mcfo'], help='Database')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'staging', 'prod'], help='Publishing manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        choices=['dev', 'prod', 'local'], help='Mongo manifold')
    PARSER.add_argument('--quick', dest='QUICK', action='store_true',
                        default=False, help='Do not crosscheck with publishing database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = setup_logging(ARG)
    initialize_program()
    process_imagery()
    sys.exit(0)
