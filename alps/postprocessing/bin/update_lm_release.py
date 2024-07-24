''' update_lm_release.py
    This program will update the ALPS release in NeuronBridge tables
'''
import argparse
import json
from operator import attrgetter
import sys
import MySQLdb
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint:disable=logging-fstring-interpolation

# Database
DB = {}
READ = {'RELEASE': "SELECT DISTINCT value FROM image_property_vw WHERE "
                   + "type='alps_release' AND value != '' ORDER BY 1",
        'MAIN': "SELECT DISTINCT slide_code,publishing_name,alignment_space_cdm "
                + "FROM image_data_mv WHERE alps_release=%s AND "
                + "alignment_space_cdm IS NOT NULL ORDER BY 1",
       }
BATCH = {'neuronMetadata': [], 'publishedLMImage': []}
# Counters
CHANGED = {}
COUNT = {'nmd': 0, 'nmd_added': 0, 'nmd_changed': 0, 'nmd_missing': 0,
         'pli': 0, 'pli_added': 0, 'pli_changed': 0,
         'neuronMetadata_insert': 0, 'neuronMetadata_update': 0,
         'publishedLMImage_insert': 0, 'publishedLMImage_update': 0
        }

def terminate_program(msg=None):
    """ Log an optional error to output and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    """ Initialize program
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught)
        terminate_program(err)
    # Connect to databases
    for source in ('sage', 'neuronbridge'):
        rwp = 'write' if source == 'neuronbridge' else 'read'
        dbo = attrgetter(f"{source}.prod.{rwp}")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, 'prod', dbo.host, dbo.user)
        try:
            DB['nb' if source == 'neuronbridge' else source] = JRC.connect_database(dbo)
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
    if not ARG.RELEASE:
        try:
            DB['sage']['cursor'].execute(READ['RELEASE'])
            rows = DB['sage']['cursor'].fetchall()
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        rlist = [row['value'] for row in rows]
        terminal_menu = TerminalMenu(rlist, title="Select a release:")
        chosen = terminal_menu.show()
        if chosen is None:
            terminate_program("You must specify a release")
        ARG.RELEASE = rlist[chosen]


def process_nmd(scode, coll):
    """ Batch updates to neuronMetadata for a single slide code
        Keyword arguments:
          scode: slide code
          coll: collection
        Returns:
          None
    """
    result = coll.find({"slideCode": scode}, {"datasetLabels": 1})
    for row in result:
        COUNT['nmd'] += 1
        payload = {'_id': row['_id'],
                   'datasetLabels': [ARG.RELEASE]}
        if not 'datasetLabels' in row:
            COUNT['nmd_added'] += 1
            BATCH['neuronMetadata'].append(payload)
        elif ARG.RELEASE not in row['datasetLabels']:
            if len(row['datasetLabels']) > 1:
                terminate_program(f"More than one release for {rec['_id']}")
            if row['datasetLabels'][0] not in CHANGED:
                CHANGED[row['datasetLabels'][0]] = 1
            else:
                CHANGED[row['datasetLabels'][0]] += 1
            COUNT['nmd_changed'] += 1
            BATCH['neuronMetadata'].append(payload)


def process_pli(scode, coll):
    """ Batch updates to publishedLMImage for a single slide code
        Keyword arguments:
          scode: slide code
          coll: collection
        Returns:
          None
    """
    result = coll.find({"slideCode": scode})
    for row in result:
        COUNT['pli'] += 1
        payload = {'_id': row['_id'],
                   'releaseName': ARG.RELEASE}
        if not 'releaseName' in row:
            COUNT['pli_added'] += 1
            BATCH['publishedLMImage'].append(payload)
        elif ARG.RELEASE != row['releaseName']:
            if row['releaseName'] not in CHANGED:
                CHANGED[row['releaseName']] = 1
            else:
                CHANGED[row['releaseName']] += 1
            COUNT['pli_changed'] += 1
            BATCH['publishedLMImage'].append(payload)


def update_database(collection, recs):
    """ Update a MongoDB collection
        Keyword arguments:
          collection: collection to update
          recs: list of records to update
        Returns:
          None
    """
    LOGGER.info(f"Updating {collection}")
    if not ARG.WRITE:
        return
    coll = DB["nb"][collection]
    for rec in recs:
        try:
            result = coll.update_one({"_id": rec['_id']}, {"$set": rec}, upsert=True)
        except Exception as err:
            LOGGER.error("Could not insert %s into Mongo", rec['_id'])
            terminate_program(err)
        if hasattr(result, 'inserted_id') and result.inserted_id == rec['_id']:
            COUNT[f"{collection}_insert"] += 1
        else:
            COUNT[f"{collection}_update"] += 1


def show_report():
    """ Show processing report
        Keyword arguments:
          None
        Returns:
          None
    """
    if len(CHANGED):
        print("Releases were changed for:")
        for key, val in CHANGED.items():
            print(f"  {key}: {val}")
    with open('lm_release_updates.txt', 'w', encoding='ascii') as batchout:
        for key, val in BATCH.items():
            for bat in val:
                batchout.write(json.dumps(bat))
                batchout.write("\n")
            update_database(key, val)
    print(f"Slides missing from neuronMetadata:  {COUNT['nmd_missing']:,}")
    print(f"Images found in neuronMetadata:      {COUNT['nmd']:,}")
    print(f"Changed release in neuronMetadata:   {COUNT['nmd_changed']:,}")
    print(f"Added release in neuronMetadata:     {COUNT['nmd_added']:,}")
    print(f"Images found in publishedLMImage:    {COUNT['pli']:,}")
    print(f"Changed release in publishedLMImage: {COUNT['pli_changed']:,}")
    print(f"Added release in publishedLMImage:   {COUNT['pli_added']:,}")
    print(f"neuronMetadata inserts:              {COUNT['neuronMetadata_insert']:,}")
    print(f"neuronMetadata updates:              {COUNT['neuronMetadata_update']:,}")
    print(f"publishedLMImage inserts:            {COUNT['publishedLMImage_insert']:,}")
    print(f"publishedLMImage updates:            {COUNT['publishedLMImage_update']:,}")


def process_release():
    """ Process the release
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        DB['sage']['cursor'].execute(READ['MAIN'], (ARG.RELEASE,))
        rows = DB['sage']['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    print(f"Found {len(rows):,} images in {ARG.RELEASE}")
    # Get known slide codes
    slides = {}
    coll = DB["nb"].neuronMetadata
    result = coll.distinct("slideCode")
    for row in result:
        slides[row] = True
    # Process slides in release
    item = {}
    for row in tqdm(rows, desc='Images'):
        if row['slide_code'] not in slides:
            COUNT['nmd_missing'] += 1
            continue
        if row['slide_code'] not in item:
            item[row['slide_code']] = {}
        item[row['slide_code']]['line'] = row['publishing_name']
        item[row['slide_code']]['template'] = row['alignment_space_cdm'].lower()
    LOGGER.info(f"Slide codes: {len(item):,}")
    coll = DB["nb"].neuronMetadata
    for scode in tqdm(item, desc='neuronMetadata'):
        process_nmd(scode, coll)
    coll = DB["nb"].publishedLMImage
    for scode in tqdm(item, desc='publishedLMImage'):
        process_pli(scode, coll)
    show_report()


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Modify ALPS release in NeuronBridge MongoDB tables')
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        help='ALPS release')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Update MongoDB tables')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_release()
    terminate_program()
