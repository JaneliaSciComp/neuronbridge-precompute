''' correlate_annotations.py
    This program will correlate annotations from a spreadsheet with NeuronBridge
'''
import argparse
import collections
from operator import attrgetter
import sys
import pandas as pd
from tqdm import tqdm
import jrc_common.jrc_common as JRC

#pylint:disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
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


def initialize_program():
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['jacs', 'neuronbridge']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_publishing_names():
    ''' Get publishing names
        Keyword arguments:
          None
        Returns:
          dict of publishing names
    '''
    try:
        rows = DB['neuronbridge']['publishedURL'].distinct('publishedName')
    except Exception as err:
        terminate_program(err)
    names = {}
    for row in rows:
        names[row] = True
    return names


def get_neuron_types():
    ''' Get neuron types
        Keyword arguments:
          None
        Returns:
          disct of neuron types
    '''
    try:
        rows = DB['jacs']['emBody'].distinct('neuronType')
    except Exception as err:
        terminate_program(err)
    ntypes = {}
    for row in rows:
        if row:
            ntypes[row] = True
    return ntypes


def process_annotations():
    """ Process images with EM data
        Keyword arguments:
          None
        Returns:
          None
    """
    # Get publishing names and neuron types
    LOGGER.info("Gettting publishing names")
    pname = get_publishing_names()
    LOGGER.info("Getting neuron types")
    ntype = get_neuron_types()
    # Process the spreadsheet
    pdf = pd.read_excel(ARG.FILE, sheet_name=0)
    pdf = pdf.fillna('')
    pdf.rename(columns={"Cell types": "Term"}, inplace=True)
    if 'Annotator' not in pdf.columns:
        pdf['Annotator'] = 'Proofreader'
    if 'Term type' not in pdf.columns:
        pdf['Term type'] = 'cell_type'
    #pdf = pdf.reset_index()
    for col in ('Random Sample', 'Comment', 'Expert annotation'):
        pdf.drop(col, axis=1, inplace=True)
    line_in_nb = []
    all_lines_found = True
    ntype_in_nb = []
    confidence = []
    ntype_found = {}
    ntype_not_found = {}
    for _, row in tqdm(pdf.iterrows(), desc="Reading file"):
        annotation = str(row.loc['Annotation'])
        normalized = 'Confident' if 'true' in annotation.lower() else 'Candidate'
        confidence.append(normalized)
        if row.loc['Line Name'] in pname:
            line_in_nb.append(True)
        else:
            all_lines_found = False
            LOGGER.warning(f"Line {row.loc['Line Name']} not in NeuronBridge")
            line_in_nb.append(False)
        if row.loc['Term'] in ntype:
            ntype_found[row.loc['Term']] = True
            ntype_in_nb.append(True)
        else:
            LOGGER.warning(f"Cell type {row.loc['Term']} not in JACS")
            ntype_not_found[row.loc['Term']] = True
            ntype_in_nb.append(False)
    pdf['Annotation'] = confidence
    if not all_lines_found:
        pdf['Line in NeuronBridge'] = line_in_nb
    if ntype_not_found:
        pdf['Neuron type in JACS'] = ntype_in_nb
    pdf.sort_values(['Line Name', 'Region', 'Term'],
                    ascending=[True, True, True], inplace=True)
    print(f"Neuron types found in JACS: {len(ntype_found.keys()):,}")
    print(f"Neuron types not found in JACS: {len(ntype_not_found.keys()):,}")
    fname = ARG.FILE.replace('.xlsx', '_reformatted.xlsx')
    pdf.to_excel(fname, index=False)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Parse annotation spreadsheet")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='Excel file')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='Manifold ([prod], dev)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    process_annotations()
    terminate_program()
