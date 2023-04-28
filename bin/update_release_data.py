''' This program will update ALPS release data in the MongoDB lmReleases collection
'''

import argparse
from operator import attrgetter
import sys
from colorama import Fore, Style
import MySQLdb
from common_lib import setup_logging, get_config, connect_database

# Database
DBM = {}
READ = {"STATS": "SELECT alps_release,COUNT(DISTINCT line) AS line,"
                 + "COUNT(DISTINCT workstation_sample_id) AS sample,"
                 + "COUNT(id) AS image FROM image_data_mv GROUP BY 1"
       }

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


def initialize_program():
    """ Connect to databases
        Keyword arguments:
          None
        Returns:
          None
    """
    # Databases
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = get_config("databases")
    except Exception as err:
        terminate_program(err)
    for manifold in ("prod", "staging",):
        for source in ("gen1mcfo", "mbew"):
            dbo = attrgetter(f"{source}.{manifold}.read")(dbconfig)
            LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, manifold, dbo.host, dbo.user)
            try:
                DBM[f"{source}-{manifold}"] = connect_database(dbo)
            except MySQLdb.Error as err:
                terminate_program(err)
    dbo = attrgetter(f"neuronbridge.{ARG.MANIFOLD}.{'write' if ARG.WRITE else 'read'}")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DBM["neuronbridge"] = connect_database(dbo)
    except Exception as err:
        terminate_program(err)


def process_releases():
    """ Process all releases for Gen1MCFO and Split-GAL4
        Keyword arguments:
          None
        Returns:
          None
    """
    count = {}
    for manifold in ("prod", "staging",):
        for source in ("gen1mcfo", "mbew"):
            dbn = f"{source}-{manifold}"
            try:
                DBM[dbn]["cursor"].execute(READ["STATS"])
                rows = DBM[dbn]["cursor"].fetchall()
            except MySQLdb.Error as err:
                sql_error(err)
            for row in rows:
                if manifold == "staging" and row["alps_release"] in count:
                    continue
                count[row["alps_release"]] = {"lines": row["line"],
                                              "samples": row["sample"],
                                              "images": row["image"],
                                              "public": bool(manifold == "prod")
                                             }
    coll = DBM["neuronbridge"].lmRelease
    maxr = 0
    for release in count:
        if len(release) > maxr:
            maxr = len(release)
    print(f"{'Release':<{maxr}}  {'Lines':<5}  {'Samples':<7}  {'Images':<6}  Public")
    for release in sorted(count):
        pub = Fore.GREEN + "YES" if count[release]['public'] else Fore.YELLOW + "NO"
        print(f"{release:<{maxr}}  {count[release]['lines']:>5}  " \
              + f"{count[release]['samples']:>7}  {count[release]['images']:>6}    " \
              + f"{pub:>3}{Style.RESET_ALL}")
        if not ARG.WRITE:
            continue
        payload = {"release": release}
        for col in count[release]:
            payload[col] = count[release][col]
        coll.update_one({"release": release}, {"$set": payload}, upsert=True)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update release data in MongoDB lmReleases")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='MongoDB manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write updated results to MongoDB')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = setup_logging(ARG)
    initialize_program()
    process_releases()
    sys.exit(0)
