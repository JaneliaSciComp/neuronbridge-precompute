''' This program will find the representative images in SAGE for imagery in FLEW
'''

import argparse
from datetime import datetime
import json
from os.path import exists
import os
import re
import sys
import time
from types import SimpleNamespace
import colorlog
import requests
import MySQLdb
from pymongo import MongoClient
from tqdm import tqdm
import neuronbridge_lib as NB


# Configuration
TARGET = "s3://janelia-flylight-imagery/Gen1/CDM"
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
# Database
CONN = {}
CURSOR = {}
DBM = PRODUCT = ''
MONGODB = 'neuronbridge-mongo'
INSERT_BATCH = 1000
READ = {"EXT": "SELECT line,name FROM image_data_mv WHERE "
               + "family IN ('dickson','rubin_chacrm') ORDER BY 1",
        "IMG": "SELECT * FROM image_data_mv WHERE line=%s AND name LIKE %s"
       }
# General
JACS_KEYS = {}
LAST_UID = None
COUNT = {"publishing": 0, "sage": 0, "jacs": 0, "missing_cdm": 0,
         "missing_obj": 0, "missing_unisex": 0,
         "jacs_error": 0, "sage_error": 0, "insert": 0}
# pylint: disable=W0703,E1101


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


def sql_error(err):
    """ Log a critical SQL error and exit
        Keyword arguments:
          err: error object
        Returns:
          None
    """
    try:
        msg = f"MySQL error [{err.args[0]}]: {err.args[1]}"
    except IndexError:
        msg = f"MySQL error: {err}"
    terminate_program(msg)


def db_connect(dbc):
    """ Connect to a database
        Keyword arguments:
          dbd: database object
        Returns:
          connector and cursor
    """
    LOGGER.info("Connecting to %s on %s", dbc.name, dbc.host)
    try:
        conn = MySQLdb.connect(host=dbc.host, user=dbc.user,
                               passwd=dbc.password, db=dbc.name)
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    except MySQLdb.Error as err:
        sql_error(err)
    return conn, cursor


def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
          endpoint: endpoint
        Returns:
          JSON
    """
    url = ((getattr(getattr(REST, server), "url") if server else "") if "REST" in globals() \
           else (os.environ.get('CONFIG_SERVER_URL') if server else "")) + endpoint
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    if req.status_code == 400:
        try:
            if "error" in req.json():
                LOGGER.error("%s %s", url, req.json()["error"])
        except Exception:
            pass
        return False
    terminate_program(f"Status: {str(req.status_code)}")


def create_config_object(config):
    """ Convert the JSON received from a configuration to an object
        Keyword arguments:
          config: configuration name
        Returns:
          Configuration object
    """
    data = (call_responder("config", f"config/{config}"))["config"]
    return json.loads(json.dumps(data), object_hook=lambda dat: SimpleNamespace(**dat))


def initialize_program():
    """ Initialize
    """
    global DBM # pylint: disable=W0603
    # Databases
    dbconfig = create_config_object("db_config")
    dbc = getattr(getattr(dbconfig, "flew"), ARG.MANIFOLD)
    (CONN['flew'], CURSOR['flew']) = db_connect(dbc)
    dbc = getattr(getattr(dbconfig, "sage"), "prod")
    (CONN['sage'], CURSOR['sage']) = db_connect(dbc)
    # Connect to Mongo
    LOGGER.info("Connecting to Mongo on %s", ARG.MONGO)
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        dbc = getattr(getattr(getattr(dbconfig, MONGODB), ARG.MONGO), rwp)
        if ARG.MONGO == 'prod':
            if MONGODB == 'neuronbridge-mongo':
                client = MongoClient(dbc.host, replicaSet=dbc.replicaset)
            else:
                client = MongoClient(dbc.host, replicaSet=dbc.replicaset)
        elif ARG.MONGO == 'local':
            client = MongoClient()
        else:
            if MONGODB == 'neuronbridge-mongo':
                client = MongoClient(dbc.host, replicaSet=dbc.replicaset)
            else:
                client = MongoClient(dbc.host)
        DBM = client.jacs if MONGODB == 'jacs-mongo' else client.admin
        if ARG.MONGO == 'prod' or MONGODB == 'neuronbridge-mongo':
            DBM.authenticate(dbc.user, dbc.password)
        if MONGODB == 'neuronbridge-mongo':
            DBM = client.neuronbridge
    except Exception as err:
        terminate_program(f"Could not connect to Mongo: {err}")


def delete_existing(coll):
    """ Delete existing Gen1 GAL4 and LexA records from Mongo. Since FLEW updates
        are very rare, we don't bother with a more complex update strategy.
        Keyword arguments:
          coll: collection
        Returns:
          None
    """
    result = coll.delete_many({"releaseName": {"$in": ["Gen1 GAL4", "Gen1 LexA"]}})
    LOGGER.info("Records deleted from Mongo: %s", result.deleted_count)


def get_sage_rows(line, file):
    """ Find representative iages from a publishing database
        Keyword arguments:
          line: fly line
          file: filename
        Returns:
          Rows from SAGE
    """
    uid = file.split(".")[0]
    try:
        CURSOR['sage'].execute(READ['IMG'], (line, "%" + uid + "%"))
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    if not rows:
        ERROR.write(f"Line {line} {file} was not found in SAGE\n")
        COUNT["sage_error"] += 1
        return None
    if len(rows) > 1:
        ERROR.write(f"Line {line} {file} has multiple entries in SAGE\n")
        COUNT["sage_error"] += 1
        return None
    if not rows[0]["area"]:
        ERROR.write(f"No area for {line} {rows[0]['name']}\n")
        COUNT["sage_error"] += 1
        return None
    return rows


def get_jacs_data(row):
    """ Get data from JACS for an LSM
        Keyword arguments:
          row: SAGE row
        Returns:
          Results (or None for error)
    """
    query = row["name"].split("/")[-1].replace(".bz2", "")
    endpoint = REST.jacs.query.LSMImages + '?name=' + query
    result = call_responder('jacs', endpoint)
    if (not result) or ("sample" not in result):
        ERROR.write(f"Could not find LSM {query} in JACS\n")
        COUNT["jacs_error"] += 1
        return None
    # Check result
    sample = result['sample'].split("#")[-1]
    endpoint = REST.jacs.query.SampleJSON + '?sampleId=' + sample
    #print(CONFIG['jacs']['url'], endpoint)
    result = call_responder('jacs', endpoint)
    if not result:
        ERROR.write(f"Could not find sample {sample} in JACS\n")
        COUNT["jacs_error"] += 1
        return None
    return result


def get_objective(result, sample):
    """ Find 20x objective results
        Keyword arguments:
          result: JACS data
          sample: sample
        Returns:
          Objective record (or None if not found)
    """
    if not isinstance(result, list) or len(result) != 1:
        ERROR.write(f"Could not find sample list {sample} in JACS\n")
        COUNT["jacs_error"] += 1
        return None
    if "objectiveSamples" not in result[0]:
        ERROR.write(f"Could not find sample {sample} in JACS\n")
        COUNT["jacs_error"] += 1
        return None
    for obj_dict in result[0]['objectiveSamples']:
        if "20x" in obj_dict["objective"]:
            return obj_dict
    ERROR.write(f"No 20x objective in sample {sample}\n")
    COUNT["missing_obj"] += 1
    return None


def get_pipeline_run_result(obj, sample):
    """ Find pipeline run result
        Keyword arguments:
          obj: objective record
          sample: sample
        Returns:
          Run result record (or None if not found)
    """
    prr = None
    for pr_list in obj["pipelineRuns"]:
        if "results" in pr_list:
            for res_dict in pr_list["results"]:
                if re.search(r"JRC\d\d\d\d_.*Unisex_.*CMTK", res_dict["name"]):
                    prr = res_dict
    if not prr:
        ERROR.write(f"No Unisex pipeline run in sample {sample}\n")
        COUNT["missing_unisex"] += 1
    return prr


def set_payload(pname, result, obj):
    """ Use a JACS sample result to find the Unisex CDM
        Keyword arguments:
          pname: publishing name
          result: JACS data
          obj: JACS objective data
          tile: tile
          sample: sample
        Returns:
          Payload
          CDM file path
    """
    global LAST_UID # pylint: disable=W0603
    release = "Gen1 " + ("LexA" if "LexA" in result[0]["driver"] else "GAL4")
    dtm = datetime.now()
    next_uid = NB.generate_jacs_uid(last_uid=LAST_UID)
    if next_uid in JACS_KEYS:
        terminate_program(f"{next_uid} is a duplicate key")
    else:
        JACS_KEYS[next_uid] = True
        LAST_UID = next_uid
    time.sleep(.0005)
    payload = {"_id": next_uid, "name": pname, "line": pname,
               "originalLine": result[0]["line"],
               "area": obj["tiles"][0]["anatomicalArea"],
               "releaseName": release, "slideCode": result[0]["slideCode"],
               "objective": obj["objective"],
               "gender": result[0]["gender"],
               "ownerKey": obj["tiles"][0]["lsmReferences"][0]["ownerKey"],
               "readers": obj["tiles"][0]["lsmReferences"][0]["readers"],
               "writers": obj["tiles"][0]["lsmReferences"][0]["writers"],
               "class": "org.janelia.colormipsearch.model.PublishedLMImage",
               "creationDate": dtm, "updateDate": dtm}
    return payload


def check_for_missing(result, obj):
    """ Check for missing data
        Keyword arguments:
          result: JACS result
          obj: objective data
        Returns:
          List of missing data
    """
    missing = []
    for req in ["driver", "gender", "slideCode"]:
        if req not in result[0]:
            missing.append(req)
    for req in ["anatomicalArea", "ownerKey", "readers", "writers"]:
        if req not in obj["tiles"][0]["lsmReferences"][0]:
            missing.append(req)
    return missing


def process_sample_result(result, pname, tile):
    """ Use a JACS sample result to find the Unisex CDM
        Keyword arguments:
          result: JACS result
          pname: publishing name
          tile: tile
        Returns:
          Payload
          CDM file path
    """
    sample = result[0]["_id"]
    obj = get_objective(result, sample)
    if not obj:
        return [None] * 2
    COUNT["jacs"] += 1
    prr = get_pipeline_run_result(obj, sample)
    if not prr:
        return [None] * 2
    missing = check_for_missing(result, obj)
    if missing:
        ERROR.write(f"Missing data from JACS for {sample}: {', '.join(missing)}\n")
        return [None] * 2
    payload = set_payload(pname, result, obj)
    payload["alignmentSpace"] = prr["name"].replace(" (CMTK)", "")
    if "Unisex_VNC" in payload["alignmentSpace"]:
        LOGGER.error(payload['alignmentSpace'])
    payload["tile"] = tile
    payload["sampleRef"] = "Sample#" + sample
    filepath = {}
    if "files" in prr:
        for iprod in ("Color Depth Projection ch1", "Visually Lossless Stack (e.g. H5J)"):
            if iprod in prr["files"]:
                file = "/".join([prr["filepath"], prr["files"][iprod]])
                filepath[getattr(PRODUCT.jacs, iprod)] = file
                if not exists(file):
                    LOGGER.error("File %s does not exist", file)
        if filepath:
            return payload, filepath
    ERROR.write(f"No Unisex Color Depth MIP in sample {sample}\n")
    COUNT["missing_cdm"] += 1
    return [None] * 2


def get_file_dict(files, publishing_name, result, payload, tile):
    """ Get a dictionary of files
        Keyword arguments:
          files: files from JACS
          result: JACS result
          payload: payload
          tile: tile
        Returns:
          File dictionary
    """
    fdict = {}
    for file in files:
        if file == "ColorDepthMip1":
            obj = "-".join([publishing_name, result[0]["slideCode"], result[0]["gender"],
                            payload["objective"], tile, payload["alignmentSpace"], "CDM_1.png"])
        else:
            obj = "-".join([publishing_name, result[0]["slideCode"], result[0]["gender"],
                            payload["objective"], tile, payload["alignmentSpace"],
                            "aligned_stack.h5j"])
        target = "/".join([TARGET, publishing_name, obj])
        COPY.write(f"{files[file]}\t{target}\n")
        fdict[file] = target.replace("s3://", "https://s3.amazonaws.com/")
    return fdict


def write_collection(coll, payload_list, icounter):
    """ Write a list of payloads to the MongoDB collection
        Keyword arguments:
          coll: MongoDB collection
          payload_list: list of payloads
          icounter: payload counter
        Returns:
          None
    """
    if ARG.WRITE:
        LOGGER.debug("Writing %d records", len(payload_list))
        result = coll.insert_many(payload_list)
        COUNT["insert"] += len(result.inserted_ids)
    else:
        COUNT["insert"] += icounter


def process_flew_rows(coll, flew_rows):
    """ Process representative images from a publishing database
        Keyword arguments:
          coll: MongoDB collection
          flew_rows: rows from FLEW database
        Returns:
          None
    """
    icounter = 0
    payload_list = []
    for flew in tqdm(flew_rows):
        publishing_name = flew['line']
        file = flew['name'].split('/')[-1]
        line = "_".join(file.split("-")[0].split("_", 4)[0:4])
        rows = get_sage_rows(line, file)
        if not rows:
            continue
        COUNT["sage"] += 1
        tile = "ventral_nerve_cord" if rows[0]["area"] == "VNC" else "brain"
        if not tile:
            LOGGER.error("Missing tile for %s %s", publishing_name, file)
            continue
        result = get_jacs_data(rows[0])
        if not result:
            continue
        payload, files = process_sample_result(result, publishing_name, tile)
        if not files:
            continue
        fdict = get_file_dict(files, publishing_name, result, payload, tile)
        payload["files"] = fdict
        if icounter == INSERT_BATCH:
            write_collection(coll, payload_list, icounter)
            icounter = 0
            payload_list = []
        payload_list.append(payload)
        icounter += 1
    if icounter:
        write_collection(coll, payload_list, icounter)
    COPY.close()
    ERROR.close()


def process_imagery():
    """ Find representative images from a publishing database
        Keyword arguments:
          None
        Returns:
          None
    """
    coll = DBM.publishedLMImage
    if ARG.WRITE:
        delete_existing(coll)
    try:
        CURSOR['flew'].execute(READ['EXT'])
        flew_rows = CURSOR['flew'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    COUNT["publishing"] = len(flew_rows)
    process_flew_rows(coll, flew_rows)
    print(f"Read from publishing:     {COUNT['publishing']}")
    print(f"Present in SAGE:          {COUNT['sage']}")
    print(f"Present in JACS:          {COUNT['jacs']}")
    print(f"Written to Mongo:         {COUNT['insert']}")
    print(f"SAGE errors:              {COUNT['sage_error']}")
    print(f"Missing objectives:       {COUNT['missing_obj']}")
    print(f"JACS errors:              {COUNT['jacs_error']}")
    print(f"Missing Unisex alignment: {COUNT['missing_unisex']}")
    print(f"Missing CDM:              {COUNT['missing_cdm']}")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find representative images in FLEW")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='Publishing manifold')
    PARSER.add_argument('--mongo', dest='MONGO', action='store',
                        default='prod', choices=['dev', 'prod', 'local'], help='Mongo manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to databases')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    REST = create_config_object("rest_services")
    PRODUCT = create_config_object("imagery_product")
    initialize_program()
    TIMESTAMP = time.strftime("%Y%m%dT%H%M%S")
    COPY = open(f"rep_{TIMESTAMP}_copy.order", 'w', encoding='ascii')
    ERROR = open(f"rep_{TIMESTAMP}_error.txt", 'w', encoding='ascii')
    process_imagery()
    sys.exit(0)
