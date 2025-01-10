''' find_annotation.py
    Find a line, cell type or body ID in the janelia-neuronbridge-custom-annotations
    DynamoDB table.
'''

import argparse
import collections
import sys
import boto3
import jrc_common.jrc_common as JRC

#pylint:disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Field lengths
MAXLEN = collections.defaultdict(lambda: 0, {})

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
    ''' Initialize database and S3 connections
        Keyword arguments:
          None
        Returns:
          None
    '''
    # DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    table = 'janelia-neuronbridge-custom-annotations2'
    try:
        _ = dynamodb_client.describe_table(TableName=table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        terminate_program(f"Table {table} doesn't exist")
    except Exception as err:
        terminate_program(err)
    DB["DYN"] = dynamodb.Table(table)


def find_item():
    ''' Look up an item in the table
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        response = DB["DYN"].get_item(Key={'entryType': 'searchString',
                                           'searchKey': ARG.ITEM.lower()})
    except Exception as err:
        terminate_program(err)
    if 'Item' in response:
        print(f"Name:       {response['Item']['name']}")
        print(f"Search key: {response['Item']['searchKey']}")
        print(f"Filter key: {response['Item']['filterKey']}")
        print(f"Item type:  {response['Item']['itemType']}")
        for preset in ('annotation', 'region', 'annotator'):
            MAXLEN[preset] = len(preset)
        MAXLEN['cell_type'] = MAXLEN['body_id'] = 17
        for mtch in response['Item']['matches']:
            for key, val in mtch.items():
                if len(val) > MAXLEN[key]:
                    MAXLEN[key] = len(val)
        header = f"{'Line':<{MAXLEN['line']}}  " if 'line' in MAXLEN \
                 else "Cell type/Body ID  "
        header += f"{'Region':<{MAXLEN['region']}}  " \
                  + f"{'Annotation':<{MAXLEN['annotation']}}  " \
                  + f"{'Annotator':<{MAXLEN['annotator']}}"
        print(f"Matches:\n  {header}")
        for mtch in response['Item']['matches']:
            out = '  '
            if 'line' in mtch:
                out += f"{mtch['line']:<{MAXLEN['line']}}  "
            elif 'cell_type' in mtch:
                out += f"{mtch['cell_type']:<{MAXLEN['cell_type']}}  "
            else:
                out += f"{mtch['body_id']:<{MAXLEN['body_id']}}  "
            out += f"{mtch['region']:<{MAXLEN['region']}}  " \
                   + f"{mtch['annotation']:<{MAXLEN['annotation']}}  " \
                   + f"{mtch['annotator']:<{MAXLEN['annotator']}}"
            print(out)
    else:
        LOGGER.warning(f"{ARG.ITEM} was not found")

# --------------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find an item in janelia-neuronbridge-custom-annotations")
    PARSER.add_argument('--item', dest='ITEM', action='store',
                        required=True, help='Line, cell type, or body ID')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    find_item()
    terminate_program()
