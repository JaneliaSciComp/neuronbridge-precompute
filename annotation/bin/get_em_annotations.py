''' get_em_annotations.py
    Get EM annotations from Raw and Split-GAL4 databases and dump to an Excel file
'''

import argparse
import collections
from operator import attrgetter
import sys
from time import strftime
import pandas as pd
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint:disable=broad-exception-caught,logging-fstring-interpolation,logging-not-lazy

# Database
DB = {}
SOURCES = ('raw', 'mbew')
READ = {'MAIN': "SELECT * FROM em_annotation_vw WHERE url != ''",
        'GROUP': "SELECT dataset,term_type,COUNT(1) AS c FROM em_annotation_vw WHERE url != '' "
                 + "GROUP BY 1,2",
       }
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# General
ANN = {}

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
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    # Connect to databases
    for source in SOURCES:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.read")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


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


def process_rows(source, rows):
    """ Process rows
        Keyword arguments:
          source: database source
          rows: rows to process
        Returns:
          None
    """
    for row in tqdm(rows, desc=f"Processing {source} database"):
        try:
            if row['line'] not in ANN:
                ANN[row['line']] = {}
            if row['term'] not in ANN[row['line']]:
                ANN[row['line']][row['term']] = {'dataset': row['dataset'],
                                                 'term_type': row['term_type'],
                                                 'annotator': row['annotator'],
                                                 'confidence': row['confidence'],
                                                }
                continue
            if higher_confidence(row['confidence'], ANN[row['line']][row['term']]['confidence']):
                LOGGER.warning(f"Replace {ANN[row['line']][row['term']]['confidence']} with " \
                               + f"{row['confidence']} annotation for {row['line']} {row['term']}")
                ANN[row['line']][row['term']] = {'dataset': row['dataset'],
                                                 'term_type': row['term_type'],
                                                 'annotator': row['annotator'],
                                                 'confidence': row['confidence'],
                                                }
        except Exception as err:
            terminate_program(err)


def generate_output_file():
    """ Generate output file
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        dsets = JRC.get_config("em_datasets")
    except Exception as err:
        terminate_program(err)
    outlist = []
    for line, aline in ANN.items():
        for term, ann in aline.items():
            short = ann['dataset'].split(':')[0]
            wtp = attrgetter(f"{short}.anatomicalArea")(dsets).lower()
            outlist.append({'Line Name': line, 'Region': wtp, 'Term': term,
                            'Term type': ann['term_type'], 'Annotator': ann['annotator'],
                            'Annotation': ann['confidence']})
    pdf = pd.DataFrame(outlist)
    pdf.sort_values(['Line Name', 'Region', 'Term'],
                    ascending=[True, True, True], inplace=True)
    fname = f"em_annotations_{strftime('%Y%m%dT%H%M%S')}.xlsx"
    pdf.to_excel(fname, index=False)
    LOGGER.info(f"EM annotations saved to {fname}")


def show_diagnostics():
    """ Show diagnostics
        Keyword arguments:
          None
        Returns:
          None
    """
    body = {}
    neuron = {}
    for aline in ANN.values():
        for term, ann in aline.items():
            COUNT['annotations'] += 1
            if ann['term_type'] == 'body_id':
                body[term] = True
            else:
                neuron[term] = True
    just = 30
    for source in SOURCES:
        print(f"{'EM annotations in ' + source + ':':{just}} {COUNT[source]:>5,}")
        try:
            DB[source]['cursor'].execute(READ['GROUP'])
            rows = DB[source]['cursor'].fetchall()
        except Exception as err:
            terminate_program(err)
        for row in rows:
            print(f"{'  ' + row['dataset'] + ' ' + row['term_type'] + ':':{just}} {row['c']:>5,}")
    print(f"{'Lines with EM annotations:':{just}} {len(ANN):>5,}")
    print(f"{'Body IDs:':{just}} {len(body):>5,}")
    print(f"{'Neurons:':{just}} {len(neuron):>5,}")
    print(f"{'Total annotations:':{just}} {COUNT['annotations']:>5,}")


def processing():
    """ Get EM annotations and dump them to an Excel file
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        for source in SOURCES:
            LOGGER.info(f"Reading {source} database")
            try:
                DB[source]['cursor'].execute(READ['MAIN'])
                rows = DB[source]['cursor'].fetchall()
            except Exception as err:
                terminate_program(err)
            COUNT[source] = len(rows)
            process_rows(source, rows)
    except Exception as err:
        terminate_program(err)
    generate_output_file()
    show_diagnostics()

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Dump EM annotations to Excel file')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['staging', 'prod'], default='staging',
                        help='Manifold')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Update MongoDB tables')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    processing()
    terminate_program()
