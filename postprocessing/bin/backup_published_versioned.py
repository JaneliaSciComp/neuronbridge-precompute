''' backup_published_versioned.py
    Create a backup of a published_versioned DynamoDB table
'''

import argparse
import sys
import time
import boto3
from botocore.exceptions import ClientError
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC

#pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Databases
DB = {}

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


def initialize_program():
    """ Initialize AWS DynamoDB connection and select table if needed
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        DB['client'] = boto3.client('dynamodb', region_name='us-east-1')
        DB['resource'] = boto3.resource('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    if not ARG.TABLE:
        dtables = list(DB['resource'].tables.all())
        tables = []
        for tbl in dtables:
            if tbl.name.startswith('janelia-neuronbridge-published-v'):
                tables.append(tbl.name)
        quest = [inquirer.List("table",
                               message="Select table to back up",
                               choices=tables, carousel=True)]
        ans = inquirer.prompt(quest, theme=BlueComposure())
        if not ans or not ans['table']:
            terminate_program("No table selected")
        ARG.TABLE = ans['table']


def process():
    """ Create the backup
        Keyword arguments:
          None
        Returns:
          None
    """
    bname = f"backup-v{ARG.TABLE.split('-v')[-1]}"
    LOGGER.info(f"Creating backup {bname} of table {ARG.TABLE}")
    try:
        resp = DB['client'].create_backup(TableName=ARG.TABLE,
                                          BackupName=bname)
    except ClientError as err:
        if type(err).__name__ == 'TableNotFoundException':
            terminate_program(f"Table {ARG.TABLE} not found")
        terminate_program(err)
    except Exception as err:
        terminate_program(err)
    backup_created = False
    while not backup_created:
        time.sleep(1)
        resp = DB['client'].list_backups(TableName=ARG.TABLE)
        if not isinstance(resp, dict) or 'BackupSummaries' not in resp:
            continue
        if resp['BackupSummaries'][0]['BackupStatus'] != 'AVAILABLE':
            continue
        backup_created = True
    print(f"{bname} created")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Create a backup of a published_versioned DynamoDB table")
    PARSER.add_argument('--table', dest='TABLE', action='store',
                        help='DynamoDB table to back up')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process()
    terminate_program()
