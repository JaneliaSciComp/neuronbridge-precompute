''' update_dynamodb_annotations.py
    Given an Excel file, update the DynamoDB janelia-neuronbridge-custom-annotations table
    and write a copy of the input file to S3.
    The Excel file contains annotations for lines and cell types/body IDs.
    The following columns are required:
      Line Name: publishing name
      Dataset: NeuPrint dataset
      Region: brain region
      Term: cell type or body ID
      Term type: "cell_type" or "body_id"
      Annotation: annotation ("Candidate", "Probable", or "Confident")
      Annotator: annotator
    One or more output files are created:
        manifest_YYYYMMDDTHHMMSS.json: manifest of updated annotations
        replacements_YYYYMMDDTHHMMSS.txt: list of replaced annotations
        new_cells_YYYYMMDDTHHMMSS.txt: list of new cell types/body IDs
        new_lines_YYYYMMDDTHHMMSS.txt: list of new lines
    Output files are also loaded to S3.
'''

__version__ = '2.0.0'

import argparse
import collections
import json
from operator import attrgetter
import os
from pathlib import Path
import sys
from time import strftime
import boto3
import pandas as pd
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint:disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# AWS
DDB_TABLE = 'janelia-neuronbridge-custom-annotations'
DYNAMO = {}
S3 = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# General
ADD_CELL = []
ADD_LINE = []
ERROR = {}
MANIFEST = []
REPLACEMENTS = []
NEUPRINT = {}

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
                         "Value": "Stores line and body ID and cell type custom annotations"}
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
    # Databases
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    for source in ("jacs", "neuronbridge"):
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info(f"Connecting to {dbo.name} {ARG.MANIFOLD} on {dbo.host} as {dbo.user}")
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    payload = {"published": True}
    try:
        rows = DB['jacs'].emDataSet.find(payload)
    except Exception as err:
        terminate_program(err)
    published = []
    for row in rows:
        if 'version' in row and row['version']:
            published.append(f"{row['name']}:v{row['version']}")
        else:
            published.append(row['name'])
    payload = [{"$match": {"status": "Traced", "neuronType": {"$ne": None},
                           "dataSetIdentifier": {"$in": published}}},
               {"$group": {"_id": {"dataSetIdentifier": "$dataSetIdentifier",
                                   "neuronType": "$neuronType"}}}]
    try:
        rows = DB['jacs'].emBody.aggregate(payload)
    except Exception as err:
        terminate_program(err)
    for row in rows:
        if row['_id']['dataSetIdentifier'] not in NEUPRINT:
            NEUPRINT[row['_id']['dataSetIdentifier']] = {}
        NEUPRINT[row['_id']['dataSetIdentifier']][row['_id']['neuronType']] = True
    payload = [{"$group": {"_id": "$libraryName"}}]
    try:
        rows = DB['neuronbridge'].publishedURL.aggregate(payload)
    except Exception as err:
        terminate_program(err)
    available = []
    for row in rows:
        available.append(row['_id'])
    for dset, val in dict(sorted(NEUPRINT.items())).items():
        cmp = 'flyem_' + dset.replace(":v", "_").replace(".", "_")
        if cmp in available or ARG.OVERRIDE:
            LOGGER.info(f"{dset}: {len(val):,} cell types")
        else:
            del NEUPRINT[dset]
            LOGGER.debug(f"{dset} is not available in NeuronBridge")
    # DynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    except Exception as err:
        terminate_program(err)
    try:
        _ = dynamodb_client.describe_table(TableName=DDB_TABLE)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        LOGGER.warning(f"Table {DDB_TABLE} doesn't exist")
        create_dynamodb_table(dynamodb, DDB_TABLE)
    except Exception as err:
        terminate_program(err)
    DB["DYN"] = dynamodb.Table(DDB_TABLE)
    try:
        ddt = dynamodb_client.describe_table(TableName=DDB_TABLE)
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


def cell_type_in_neuprint(dataset, cell):
    ''' Check if cell type is in Neuprint
        Keyword arguments:
          dataset: NeuPrint dataset
          cell: cell type
        Returns:
          True if cell type is in Neuprint, False otherwise
    '''
    if dataset not in NEUPRINT:
        msg = f"{dataset} not found in NeuronBridge"
        if msg not in ERROR:
            LOGGER.error(msg)
            ERROR[msg] = True
        return None
    if cell not in NEUPRINT[dataset]:
        NEUPRINT[dataset][cell] = False
        LOGGER.warning(f"{cell} not found in {dataset}")
        return None
    return NEUPRINT[dataset][cell]


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
            if 'cell_type' in ann:
                lines[line]['present'].append(ann['cell_type'])
            elif 'body_id' in ann:
                lines[line]['present'].append(ann['body_id'])
    else:
        ADD_LINE.append(line)
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
        if 'cell_type' in ann and 'cell_type' in cell_ann:
            if ann['cell_type'] != cell_ann['cell_type']:
                new_record.append(cell_ann)
            else:
                new_record.append(ann)
        if 'body_id' in ann and 'body_id' in cell_ann:
            if ann['body_id'] != cell_ann['body_id']:
                new_record.append(cell_ann)
            else:
                new_record.append(ann)
    if not new_record:
        print(json.dumps(lines[line], indent=2))
        print(json.dumps(ann, indent=2))
        terminate_program(f"Annotation not found for {line}")
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
        ADD_CELL.append(cell)
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
        else:
            new_record.append(ann)
    if not new_record:
        print(json.dumps(cells[cell], indent=2))
        print(json.dumps(ann, indent=2))
        terminate_program(f"Annotation not found for {cell} in {ann['line']}")
    cells[cell]['matches'] = new_record


def cell_annotation_by_line(cellrec, line):
    ''' Get cell annotation by line
        Keyword arguments:
          cellrec: cell record in dictionary
          line: line
        Returns:
          annotation
    '''
    for ann in cellrec['matches']:
        if ann['line'] == line:
            return ann['annotation']
    terminate_program(f"Annotation not found for {line} in {cellrec}")


def line_annotation_by_cell(linerec, cell):
    ''' Get line annotation by cell
        Keyword arguments:
          linerec: line record in dictionary
          cell: cell type
        Returns:
          annotation
    '''
    for ann in linerec['matches']:
        if 'cell_type' in ann and ann['cell_type'] == cell:
            return ann['annotation']
        if 'body_id' in ann and ann['body_id'] == cell:
            return ann['annotation']
    terminate_program(f"Annotation not found for {cell} in {linerec}")


def higher_confidence(new_confidence, old_confidence):
    """ Determine if new confidence is higher than old confidence
        Keyword arguments:
          new_confidence: new confidence level
          old_confidence: old confidence level
        Returns:
          True if new confidence is higher, False otherwise
    """
    if new_confidence == 'Confident':
        return not old_confidence == 'Confident'
    if new_confidence == 'Probable':
        return not old_confidence in ('Probable', 'Confident')
    if new_confidence == 'Candidate':
        return False
    terminate_program(f"Unknown confidence level: {new_confidence}")


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
        MANIFEST.append(ann)
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
        MANIFEST.append(ann)
        if ARG.WRITE:
            try:
                DB["DYN"].put_item(Item=ann)
                COUNT['updates'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['updates'] += 1
        if ann['itemType'] != 'body_id':
            continue
        # Add an additional fully-qualified search key for body IDs
        # e.g. 100186 -> manc:v1.0:100186
        dataset = ann['matches'][0]['dataset']
        if len(ann['matches']) > 1:
            for match in ann['matches']:
                if match['dataset'] != dataset:
                    terminate_program(f"Multiple datasets for {ann['name']}")
        ann2 = ann.copy()
        ann2['searchKey'] = f"{dataset}:{ann2['searchKey']}"
        ann2['filterKey'] = ann2['searchKey']
        MANIFEST.append(ann2)
        if ARG.WRITE:
            try:
                DB["DYN"].put_item(Item=ann2)
                COUNT['body_updates'] += 1
            except Exception as err:
                terminate_program(err)
        else:
            COUNT['body_updates'] += 1


def upload_file_to_s3(filepath, prefix, content='text/plain'):
    ''' Upload file to S3
        Keyword arguments:
          filepath: file path
          prefix: S3 prefix
          content: content type
        Returns:
          None
    '''
    desc = f"Upload {filepath} to S3"
    tags = f"PROJECT=NeuronBridge&STAGE=prod&DEVELOPER=svirskasr&DESCRIPTION={desc}" \
           + f"&VERSION={__version__}"
    try:
        filename = Path(filepath).name
        S3['CLIENT'].upload_file(filepath, 'janelia-neuronbridge-annotation',
                                 f"{prefix}/{filename}",
                                 ExtraArgs={'ContentType': content,
                                            'Tagging': tags})
        LOGGER.info(f"Uploaded {filename} to S3")
    except Exception as err:
        terminate_program(err)


def upload_input(df):
    ''' Upload input file (as tab-separated values) to S3
        Keyword arguments:
          None
        Returns:
          None
    '''
    filename, _ = os.path.splitext(os.path.basename(ARG.FILE))
    filename = f"{filename}_{TIMESTAMP}.txt"
    filepath = f"/tmp/{filename}"
    try:
        df.to_csv(filepath, sep="\t", index=False)
    except Exception as err:
        terminate_program(err)
    upload_file_to_s3(filepath, 'input', 'text/tab-separated-values')


def generate_output_files():
    ''' Generate output files
        Keyword arguments:
          None
        Returns:
          None
    '''
    filepath = f"manifest_{TIMESTAMP}.json"
    with open(filepath, "w", encoding="utf-8") as fobj:
        fobj.write(JRC.json.dumps(MANIFEST, indent=2))
    if ARG.WRITE:
        upload_file_to_s3(filepath, 'output', 'application/json')
    if REPLACEMENTS:
        filepath = f"replacements_{TIMESTAMP}.txt"
        with open(filepath, "w", encoding="utf-8") as fobj:
            fobj.write("\n".join(REPLACEMENTS))
        if ARG.WRITE:
            upload_file_to_s3(filepath, 'output')
    if ADD_CELL:
        filepath = f"new_cells_{TIMESTAMP}.txt"
        with open(filepath, "w", encoding="utf-8") as fobj:
            fobj.write("\n".join(ADD_CELL))
        if ARG.WRITE:
            upload_file_to_s3(filepath, 'output')
    if ADD_LINE:
        filepath = f"new_lines_{TIMESTAMP}.txt"
        with open(filepath, "w", encoding="utf-8") as fobj:
            fobj.write("\n".join(ADD_LINE))
        if ARG.WRITE:
            upload_file_to_s3(filepath, 'output')


def statistics():
    ''' Print statistics
        Keyword arguments:
          None
        Returns:
          None
    '''
    print(f"File rows:                   {COUNT['entries']:,} entries")
    print(f"Lines:                       {COUNT['lines']:,}")
    print(f"Cell types:                  {COUNT['cells']:,}")
    print(f"Cell types not in neuPrint:  {COUNT['neuprint']:,} " \
          + f"({COUNT['neuprint']/COUNT['cells']*100:.2f}%)")
    print(f"New lines:                   {COUNT['new_lines']:,} " \
          + f"({COUNT['new_lines']/COUNT['lines']*100:.2f}%)")
    print(f"New cell types:              {COUNT['new_cells']:,} " \
          + f"({COUNT['new_cells']/COUNT['cells']*100:.2f}%)")
    print(f"DynamoDB reads:              {COUNT['dynamo_reads']:,}")
    print(f"Rows updated:                {COUNT['updates']:,}")
    print(f"Additional Body IDs updated: {COUNT['body_updates']:,}")
    generate_output_files()


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
    cells = {} # Stores both cell types and body IDs
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing annotations"):
        COUNT['entries'] += 1
        cell = str(row['Term'])
        if row['Term type'] == 'cell_type' and not cell_type_in_neuprint(row['Dataset'], cell):
            COUNT['neuprint'] += 1
            continue
        line = row['Line Name']
        # Line
        if line not in lines:
            # matches will contain annotation dict (annotation, cell type, region, dataset)
            # present will contain dict of cell types
            lines[line] = {'entryType': 'searchString',
                           'searchKey': line.lower(),
                           'itemType': 'line_name',
                           'filterKey': line.lower(),
                           'name': line,
                           'matches': [],
                           'present': []}
            add_existing_cell_types(lines, line)
        ann = {'region': row['Region'],
               row['Term type']: cell,
               'annotation': row['Annotation'].capitalize(),
               'annotator': row['Annotator']
              }
        if 'Dataset' in row:
            ann['dataset'] = row['Dataset']
        if cell in lines[line]['present']:
            LOGGER.debug(f"{cell} already present for {line} ({ann})")
            annlist = line_annotation_by_cell(lines[line], cell)
            higher = higher_confidence(row['Annotation'], annlist)
            if higher:
                REPLACEMENTS.append(f"Replace lines {annlist} with {row['Annotation']} " \
                                    + f"annotation for {line} {cell}")
                replace_lines_annotation(lines, line, ann)
        else:
            lines[line]['matches'].append(ann)
        # Cell type
        if cell not in cells:
            # matches will contain annotation dict (annotation, line, region, dataset)
            # present will contain dict of lines
            cells[cell] = {'entryType': 'searchString',
                           'searchKey': cell.lower(),
                            'itemType': row['Term type'],
                            'filterKey': cell.lower(),
                            'name': cell,
                            'matches': [],
                            'present': []}
            add_existing_lines(cells, cell)
        ann = {'region': row['Region'],
               'line': line,
               'annotation': row['Annotation'].capitalize(),
               'annotator': row['Annotator']
              }
        if 'Dataset' in row:
            ann['dataset'] = row['Dataset']
        higher = True
        if line in cells[cell]['present']:
            LOGGER.debug(f"Line {line} already present for {cell} ({ann})")
            annlist = cell_annotation_by_line(cells[cell], line)
            higher = higher_confidence(row['Annotation'], annlist)
            if higher:
                REPLACEMENTS.append(f"Replace cells {annlist} with {row['Annotation']} " \
                                    + f"annotation for {line} {cell}")
                replace_cells_annotation(cells, cell, ann)
            continue
        cells[cell]['matches'].append(ann)
    update_dynamodb(lines, cells)
    if ARG.WRITE:
        upload_input(df)
    COUNT['lines'] = len(lines)
    COUNT['cells'] = len(cells)
    statistics()

# --------------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Parse annotation spreadsheet")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        required=True, help='Excel file')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=["dev", "prod"], default="prod", help='MongoDB manifold')
    PARSER.add_argument('--override', dest='OVERRIDE', action='store_true',
                        default=False, help='Allow usage of datasets not in NeuronBridge')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to DynamoDB')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        REST = JRC.get_config("rest_services")
    except Exception as err:
        terminate_program(err)
    initialize_program()
    TIMESTAMP = strftime('%Y%m%dT%H%M%S')
    process_annotations()
    terminate_program()
