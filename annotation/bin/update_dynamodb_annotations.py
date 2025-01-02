''' update_dynamodb_annotations.py
    Update the DynamoDB janelia-neuronbridge-custom-annotations table
    and write a copy of the input file to S3
'''

import argparse
import collections
import os
import sys
import boto3
import pandas as pd
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint:disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# AWS
DYNAMO = {}
S3 = {}
# Cache
CACHE = {'lines': {}, 'cells': {}}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

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


def create_dynamodb_table(dynamodb, table):
    """ Create a DynamoDB table
        Keyword arguments:
          dynamodb: DynamoDB resource
          table: table name
        Returns:
          None
    """
    payload = {"TableName": table,
               "KeySchema": [{"AttributeName": "entryType", "KeyType": "HASH"},
                             {"AttributeName": "searchKey", "KeyType": "RANGE"}
                            ],
               "AttributeDefinitions": [{'AttributeName': 'entryType', 'AttributeType': 'S'},
                                        {'AttributeName': 'searchKey', 'AttributeType': 'S'}
                                       ],
               "BillingMode": "PAY_PER_REQUEST",
               "Tags": [{"Key": "PROJECT", "Value": "NeuronBridge"},
                        {"Key": "DEVELOPER", "Value": "svirskasr"},
                        {"Key": "STAGE", "Value": 'prod'},
                        {"Key": "DESCRIPTION",
                         "Value": "Stores line and cell type custom annotations"}
                       ]
              }
    LOGGER.warning(f"Creating {table}")
    ddbtable = dynamodb.create_table(**payload)
    ddbtable.wait_until_exists()


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
    table = 'janelia-neuronbridge-custom-annotations'
    try:
        _ = dynamodb_client.describe_table(TableName=table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        LOGGER.warning("Table %s doesn't exist", table)
        create_dynamodb_table(dynamodb, table)
    except Exception as err:
        terminate_program(err)
    DB["DYN"] = dynamodb.Table(table)
    try:
        ddt = dynamodb_client.describe_table(TableName=table)
        DYNAMO['client'] = dynamodb_client
        DYNAMO['arn'] = ddt['Table']['TableArn']
    except dynamodb_client.exceptions.ResourceNotFoundException:
        terminate_program(f"Table {table} doesn't exist")
    except Exception as err:
        terminate_program(err)
    # S3
    try:
        S3['CLIENT'] = boto3.client('s3')
    except Exception as err:
        terminate_program(err)


def add_existing_cell_types(lines, line):
    ''' Add existing cell types to lines dictionary
        Keyword arguments:
          lines: lines dictionary
          line: line
        Returns:
          None
    '''
    try:
        response = DB["DYN"].get_item(Key={'entryType': 'searchString',
                                           'searchKey': lines[line]['searchKey']})
        COUNT['dynamo_reads'] += 1
    except Exception as err:
        terminate_program(err)
    if 'Item' in response:
        for ann in response['Item']['matches']:
            lines[line]['matches'].append(ann)
            lines[line]['present'].append(ann['cell_type'])
        if line not in CACHE['lines']:
            CACHE['lines'][line] = response['Item']
    else:
        COUNT['new_lines'] += 1


def replace_lines_annotation(lines, line, ann):
    ''' Replace annotation in the lines dictionary
        Keyword arguments:
          lines: lines dictionary
          line: line
          ann: annotation
        Returns:
          None
    '''
    new_record = []
    for cell_ann in lines[line]['matches']:
        if ann['cell_type'] != cell_ann['cell_type']:
            new_record.append(cell_ann)
    lines[line]['matches'] = new_record


def add_existing_lines(cells, cell):
    ''' Add existing lines to cells dictionary
        Keyword arguments:
          cells: cells dictionary
          cell: cell type
        Returns:
          None
    '''
    try:
        response = DB["DYN"].get_item(Key={'entryType': 'searchString',
                                           'searchKey': cells[cell]['searchKey']})
        COUNT['dynamo_reads'] += 1
    except Exception as err:
        terminate_program(err)
    if 'Item' in response:
        for ann in response['Item']['matches']:
            cells[cell]['matches'].append(ann)
            cells[cell]['present'].append(ann['line'])
    else:
        COUNT['new_cells'] += 1


def replace_cells_annotation(cells, cell, ann):
    ''' Replace annotation in the cells dictionary
        Keyword arguments:
          cells: cells dictionary
          cell: cell
          ann: annotation
        Returns:
          None
    '''
    new_record = []
    for line_ann in cells[cell]['matches']:
        if ann['line'] != line_ann['line']:
            new_record.append(line_ann)
    cells[cell]['matches'] = new_record


def update_dynamodb(lines, cells):
    ''' Update DynamoDB
        Keyword arguments:
          lines: lines dictionary
          cells: cells dictionary
          ann: annotation
        Returns:
          None
    '''
    for _, ann in tqdm(lines.items(), desc="Updating lines"):
        if 'present' in ann:
            del ann['present']
        if ARG.WRITE:
            try:
                DB["DYN"].put_item(Item=ann)
                COUNT['updates'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['updates'] += 1
    for _, ann in tqdm(cells.items(), desc="Updating cell types"):
        if 'present' in ann:
            del ann['present']
        if ARG.WRITE:
            try:
                DB["DYN"].put_item(Item=ann)
                COUNT['updates'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['updates'] += 1


def upload_input(df):
    ''' Upload input file to S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    filename, _ = os.path.splitext(os.path.basename(ARG.FILE))
    filename = f"{filename}.txt"
    filepath = f"/tmp/{filename}"
    try:
        df.to_csv(filepath, sep="\t", index=False)
    except Exception as err:
        terminate_program(err)
    try:
        S3['CLIENT'].upload_file(filepath, 'janelia-neuronbridge-annotation',
                                 f"input/{filename}",
                                 ExtraArgs={'ContentType': 'text/tab-separated-values'})
        LOGGER.info(f"Uploaded {filename} to S3")
    except Exception as err:
        terminate_program(err)


def process_annotations():
    ''' Process annotations
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        df = pd.read_excel(ARG.FILE)
    except Exception as err:
        terminate_program(err)
    lines = {}
    cells = {}
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing annotations"):
        COUNT['entries'] += 1
        line = row['Line Name']
        cell = row['Cell types']
        # Line
        if line not in lines:
            lines[line] = {'entryType': 'searchString',
                           'searchKey': line.lower(),
                           'itemType': 'line_name',
                           'filterKey': line.lower(),
                           'name': line,
                           'matches': [],
                           'present': []}
            add_existing_cell_types(lines, line)
        ann = {'region': row['Region'],
               'cell_type': cell,
               'annotation': row['Annotation'].capitalize()
              }
        if cell in lines[line]['present']:
            LOGGER.debug(f"Cell type {cell} already present for {line}")
            replace_lines_annotation(lines, line, ann)
        lines[line]['matches'].append(ann)
        # Cell type
        if cell not in cells:
            cells[cell] = {'entryType': 'searchString',
                           'searchKey': cell.lower(),
                           'itemType': 'cell_type',
                           'filterKey': cell.lower(),
                           'name': cell,
                           'matches': [],
                           'present': []}
            add_existing_lines(cells, cell)
        ann = {'region': row['Region'],
               'line': line,
               'annotation': row['Annotation'].capitalize()
              }
        if line in cells[cell]['present']:
            LOGGER.debug(f"Line {line} already present for {cell}")
            replace_cells_annotation(cells, cell, ann)
        cells[cell]['matches'].append(ann)
    update_dynamodb(lines, cells)
    upload_input(df)
    print(f"File rows:      {COUNT['entries']:,} entries")
    print(f"Lines:          {len(lines):,}")
    print(f"Cell types:     {len(cells):,}")
    print(f"New lines:      {COUNT['new_lines']:,}")
    print(f"New cell types: {COUNT['new_cells']:,}")
    print(f"DynamoDB reads: {COUNT['dynamo_reads']:,}")
    print(f"Rows updated:   {COUNT['updates']:,}")


# --------------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Parse annotation spreadsheet")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='Excel file')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to DynamoDB')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_annotations()
    terminate_program()
