''' update_lm_release.py
    This program will update the ALPS release in NeuronBridge tables
'''
import argparse
import collections
from operator import attrgetter
import sys
from colorama import Fore, Style
import MySQLdb
from simple_term_menu import TerminalMenu
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint:disable=broad-exception-caught,logging-fstring-interpolation

# General
ARG = LOGGER = None
DB = {}
READ = {'RELEASE': "SELECT DISTINCT value FROM image_property_vw WHERE "
                   + "type='alps_release' AND value != '' ORDER BY 1",
        'OLD': "SELECT DISTINCT slide_code,publishing_name,alignment_space_cdm "
                + "FROM image_data_mv WHERE alps_release=%s AND "
                + "alignment_space_cdm IS NOT NULL ORDER BY 1",
        'MAIN': "SELECT DISTINCT slide_code,publishing_name,workstation_sample_id,"
                + "alignment_space_cdm FROM image_data_mv WHERE alps_release=%s ORDER BY 1",
       }
ADD = {'neuronMetadata': [], 'publishedURL': [], 'publishedLMImage': []}
AUDIT = {'neuronMetadata': [], 'publishedURL': [], 'publishedLMImage': []}
BATCH = {'neuronMetadata': [], 'publishedURL': [], 'publishedLMImage': []}
BOUND = {'neuronMetadata': [], 'publishedURL': [], 'publishedLMImage': []}
# Counters
CHANGED = {}
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


def crosses_boundary(reldict):
    """ Determine if a release crosses a boundary
        Keyword arguments:
          reldict: release dictionary in the form {'old': 'old release', 'new': 'new release'}
        Returns:
          True if boundary crossed
    """
    dataset = {'old': '', 'new': ''}
    for reltype in ('old', 'new'):
        if reldict[reltype].endswith("GEN1 MCFO"):
            dataset[reltype] = "flylight_gen1_mcfo_published"
        elif reldict[reltype].endswith("Omnibus Broad"):
            dataset[reltype] = "flylight_raw_published"
        else:
            dataset[reltype] = "flylight_split_gal4_published"
    return dataset['new'] if (dataset['old'] != dataset['new']) else None
    #return dataset['old'] != dataset['new']


def process_nmd(scode, pname, coll):
    """ Batch updates to neuronMetadata for a single slide code
        Keyword arguments:
          scode: slide code
          pname: publishing name
          coll: collection
        Returns:
          None
    """
    result = coll.find({"slideCode": scode, "publishedName": pname})
    for row in result:
        COUNT['nmd'] += 1
        payload = {'_id': row['_id'],
                   'datasetLabels': [ARG.RELEASE]}
        if not 'datasetLabels' in row:
            COUNT['added'] += 1
            ADD['neuronMetadata'].append(f"{scode} {pname} {row['_id']} " \
                                         + "needs release added to neuronMetadata")
            continue
        if ARG.RELEASE not in row['datasetLabels']:
            if len(row['datasetLabels']) > 1:
                terminate_program(f"More than one release for {row['_id']}")
            if row['datasetLabels'][0] not in CHANGED:
                CHANGED[row['datasetLabels'][0]] = 1
            else:
                CHANGED[row['datasetLabels'][0]] += 1
            crossed = crosses_boundary({'old': row['datasetLabels'][0], 'new': ARG.RELEASE})
            msg = f"{scode}\t{pname}\t{row['_id']}\t{row['sourceRefId']}\t" \
                  + f"{row['datasetLabels']}\t{ARG.RELEASE}"
            if crossed:
                COUNT['nmd_crossed'] += 1
                BOUND['neuronMetadata'].append(msg)
                payload['libraryName'] = crossed
                BATCH['neuronMetadata'].append(payload)
            else:
                COUNT['nmd_changed'] += 1
                AUDIT['neuronMetadata'].append(msg)
                BATCH['neuronMetadata'].append(payload)


def process_purl(scode, pname, coll):
    """ Batch updates to publishedURL for a single slide code
        Keyword arguments:
          scode: slide code
          pname: publishing name
          coll: collection
        Returns:
          None
    """
    result = coll.find({"slideCode": scode, "publishedName": pname})
    for row in result:
        COUNT['purl'] += 1
        payload = {'_id': row['_id'],
                   'alpsRelease': ARG.RELEASE}
        if not 'alpsRelease' in row:
            COUNT['added'] += 1
            ADD['publishedURL'].append(f"{scode} {pname} {row['_id']} " \
                                       + "needs release added to publishedURL")
            continue
        if ARG.RELEASE != row['alpsRelease']:
            if row['alpsRelease'] not in CHANGED:
                CHANGED[row['alpsRelease']] = 1
            else:
                CHANGED[row['alpsRelease']] += 1
            crossed = crosses_boundary({'old': row['alpsRelease'], 'new': ARG.RELEASE})
            msg = f"{scode}\t{pname}\t{row['_id']}\t{row['sampleRef']}\t{row['alpsRelease']}\t" \
                  + f"{ARG.RELEASE}"
            if crossed:
                COUNT['purl_crossed'] += 1
                BOUND['publishedURL'].append(msg)
            else:
                COUNT['purl_changed'] += 1
                AUDIT['publishedURL'].append(msg)
                BATCH['publishedURL'].append(payload)


def process_pli(scode, pname, coll):
    """ Batch updates to publishedLMImage for a single slide code
        Keyword arguments:
          scode: slide code
          pname: publishing name
          coll: collection
        Returns:
          None
    """
    result = coll.find({"slideCode": scode, "name": pname})
    for row in result:
        COUNT['pli'] += 1
        payload = {'_id': row['_id'],
                   'releaseName': ARG.RELEASE}
        if not 'releaseName' in row:
            COUNT['added'] += 1
            ADD['publishedLMImage'].append(f"{scode} {pname} {row['_id']} " \
                                           + "needs release added to publishedLMImage")
            continue
        if ARG.RELEASE != row['releaseName']:
            if row['releaseName'] not in CHANGED:
                CHANGED[row['releaseName']] = 1
            else:
                CHANGED[row['releaseName']] += 1
            crossed = crosses_boundary({'old': row['releaseName'], 'new': ARG.RELEASE})
            msg = f"{scode}\t{pname}\t{row['_id']}\t{row['sampleRef']}\t{row['releaseName']}\t" \
                  + f"{ARG.RELEASE}"
            if crossed:
                COUNT['pli_crossed'] += 1
                BOUND['publishedLMImage'].append(msg)
            else:
                COUNT['pli_changed'] += 1
                AUDIT['publishedLMImage'].append(msg)
                BATCH['publishedLMImage'].append(payload)


def update_database(collection, recs):
    """ Update a MongoDB collection
        Keyword arguments:
          collection: collection to update
          recs: list of records to update
        Returns:
          None
    """
    if not recs:
        return
    LOGGER.info(f"Updating {collection} with {len(recs):,} record{'s' if len(recs) > 1 else ''}")
    coll = DB["nb"][collection]
    for rec in recs:
        if not ARG.WRITE:
            COUNT[f"{collection}_update"] += 1
            continue
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
        print("Releases changed for:")
        for key, val in CHANGED.items():
            print(f"  {key}: {val:,}")
    for key, val in BATCH.items():
        update_database(key, val)
    # Produce output files
    samples = 0
    for val in AUDIT.values():
        samples += len(val)
    if samples:
        with open('lm_release_updates.txt', 'w', encoding='ascii') as batchout:
            batchout.write("Collection\tSlide code\tPublishing name\tID\tSample\t" \
                           + "Old release\tNew release\n")
            for key, val in AUDIT.items():
                for msg in val:
                    batchout.write(f"{key} {msg}\n")
    samples = 0
    for val in BOUND.values():
        samples += len(val)
    if samples:
        with open('lm_release_crossovers.txt', 'w', encoding='ascii') as batchout:
            batchout.write("Collection\tSlide code\tPublishing name\tID\tSample\t" \
                           + "Old release\tNew release\n")
            for key, val in BOUND.items():
                for msg in val:
                    batchout.write(f"{key} {msg}\n")
    if COUNT['added']:
        with open('lm_release_need_releases.txt', 'w', encoding='ascii') as batchout:
            for key, val in ADD.items():
                for msg in val:
                    batchout.write(f"{key} {msg}\n")


def display_stats():
    """ Display processing stats
        Keyword arguments:
          None
        Returns:
          None
    """
    red = Fore.RED
    grn = Fore.GREEN
    rst = Style.RESET_ALL
    # Display stats
    if COUNT['nmd_missing']:
        print(f"Slides missing from neuronMetadata:   {COUNT['nmd_missing']:,}")
    print(f"Images found in neuronMetadata:       {COUNT['nmd']:,}")
    print(f"Changed release in neuronMetadata:    {COUNT['nmd_changed']:,}")
    if COUNT['nmd_crossed']:
        print(f"{red}Crossed boundary in neuronMetadata:   {COUNT['nmd_crossed']:,}{rst}")
    if COUNT['nmd_added']:
        print(f"Add release in neuronMetadata:        {COUNT['nmd_added']:,}")
    print(f"Images found in publishedURL:         {COUNT['purl']:,}")
    print(f"Changed release in publishedURL:      {COUNT['purl_changed']:,}")
    if COUNT['purl_crossed']:
        print(f"{red}Crossed boundary in publishedURL:     {COUNT['purl_crossed']:,}{rst}")
    if COUNT['purl_added']:
        print(f"Add release in publishedURL:          {COUNT['purl_added']:,}")
    print(f"Images found in publishedLMImage:     {COUNT['pli']:,}")
    print(f"Changed release in publishedLMImage:  {COUNT['pli_changed']:,}")
    if COUNT['pli_crossed']:
        print(f"{red}Crossed boundary in publishedLMImage: {COUNT['pli_crossed']:,}{rst}")
    if COUNT['pli_added']:
        print(f"Add release in publishedLMImage:      {COUNT['pli_added']:,}")
    print(f"neuronMetadata inserts:               {COUNT['neuronMetadata_insert']:,}")
    clr = red if COUNT['neuronMetadata_update'] != COUNT['nmd_changed'] else grn
    rst = Style.RESET_ALL
    print(f"neuronMetadata updates:               {clr}{COUNT['neuronMetadata_update']:,}{rst}")
    print(f"publishedURL inserts:                 {COUNT['publishedURL_insert']:,}")
    clr = red if COUNT['publishedURL_update'] != COUNT['purl_changed'] else grn
    print(f"publishedURL updates:                 {clr}{COUNT['publishedURL_update']:,}{rst}")
    print(f"publishedLMImage inserts:             {COUNT['publishedLMImage_insert']:,}")
    clr = red if COUNT['publishedLMImage_update'] != COUNT['pli_changed'] else grn
    print(f"publishedLMImage updates:             {clr}{COUNT['publishedLMImage_update']:,}{rst}")


def process_slides(sample, rows, slides):
    """ Process slides/samples in release
        Keyword arguments:
          sample: sample to process (optional)
          rows: rows from SAGE to process
          slides: slides to process
        Returns:
          None
    """
    item = {}
    sample = {}
    for row in tqdm(rows, desc='Images'):
        if sample and row['workstation_sample_id'] != sample:
            continue
        if row['slide_code'] not in slides:
            COUNT['nmd_missing'] += 1
            continue
        sample[row['workstation_sample_id']] = True
        if row['slide_code'] not in item:
            item[row['slide_code']] = {}
        item[row['slide_code']]['line'] = row['publishing_name']
        item[row['slide_code']]['template'] = row['alignment_space_cdm'].lower() \
            if row['alignment_space_cdm'] else ''
    LOGGER.info(f"Slide codes: {len(item):,}")
    LOGGER.info(f"Samples: {len(sample):,}")
    coll = DB["nb"].neuronMetadata
    for scode, val in tqdm(item.items(), desc='neuronMetadata'):
        process_nmd(scode, val['line'], coll)
    coll = DB["nb"].publishedURL
    for scode, val in tqdm(item.items(), desc='publishedURL'):
        process_purl(scode, val['line'], coll)
    coll = DB["nb"].publishedLMImage
    for scode, val in tqdm(item.items(), desc='publishedLMImage'):
        process_pli(scode, val['line'], coll)


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
    print(f"Found {len(rows):,} images in {ARG.RELEASE} on SAGE")
    # Get known slide codes
    slides = {}
    coll = DB["nb"].neuronMetadata
    result = coll.distinct("slideCode")
    for row in result:
        slides[row] = True
    # Process slides/samples in release
    if ARG.FILE:
        with open(ARG.FILE, 'r', encoding='ascii') as fh:
            samples = fh.readlines()
        for sample in samples:
            process_slides(sample.strip(), rows, slides)
    else:
        process_slides(ARG.SAMPLE, rows, slides)
    show_report()
    display_stats()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Modify ALPS release in NeuronBridge MongoDB tables')
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        help='New ALPS release')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File containing samples to process')
    PARSER.add_argument('--sample', dest='SAMPLE', action='store',
                        help='Sample (optional)')
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
