''' neuronbridge_lib.py
    Function library for NeuronBridge processing
'''

import glob
import os.path
import re
from simple_term_menu import TerminalMenu

RELEASE_LIBRARY_BASE = "/groups/scicompsoft/informatics/data/release_libraries"


def get_all_s3_objects(s3c, **base_kwargs):
    """ Generator function to handle >1000 objects
        Keyword arguments:
          s3c: S3 client instance
          base_kwargs: arguments for list_objects_v2
    """
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3c.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):
            break
        continuation_token = response.get('NextContinuationToken')


def get_library(*args):
    """ Get a NeuronBridge library from provided configuration JSON
        Keyword arguments:
          cdm_bucket: "cdm_library" configuration JSON -or- bucket
          prefix: alignment template (optional)
        Returns:
          Library
    """
    print("Select a library:")
    cdmlist = list()
    if len(args) == 3:
        paginator = args[0].get_paginator('list_objects')
        result = paginator.paginate(Bucket=args[1], Prefix=args[2] + '/', Delimiter='/')
        for prefix in result.search('CommonPrefixes'):
            key = prefix.get('Prefix')
            if re.search(r".+/", key):
                cdmlist.append((key.split("/"))[1])
    else:
        cdm_libs = args[0]
        for cdmlib in cdm_libs:
            if cdm_libs[cdmlib]['name'] not in cdmlist:
                cdmlist.append(cdm_libs[cdmlib]['name'])
    terminal_menu = TerminalMenu(cdmlist)
    chosen = terminal_menu.show()
    return cdmlist[chosen].replace(' ', '_') if chosen is not None else None


def get_neuronbridge_version():
    """ Get a NeuronBridge version from the release directory
        Keyword arguments:
          None
        Returns:
          NeuronBridge version
    """
    if not os.path.isdir(RELEASE_LIBRARY_BASE):
        print("Directory %s does not exist" % (RELEASE_LIBRARY_BASE))
        return None
    version = [re.sub('.*/', '', path)
               for path in glob.glob(RELEASE_LIBRARY_BASE + '/v[0-9]*')]
    print("Select a NeuronBridge version:")
    terminal_menu = TerminalMenu(version)
    chosen = terminal_menu.show()
    return version[chosen] if chosen is not None else None


def get_template(s3_client, bucket):
    """ Get a NeuronBridge alignment template from the bucket
        Keyword arguments:
          s3_client: S3 client
          bucket: busket
        Returns:
          Alignment template
    """
    print("Select an alignment template:")
    paginator = s3_client.get_paginator('list_objects')
    result = paginator.paginate(Bucket=bucket, Delimiter='/')
    template = list()
    for prefix in result.search('CommonPrefixes'):
        key = prefix.get('Prefix')
        if re.search(r"JRC\d+.+/", key):
            template.append(key.replace("/", ""))
    terminal_menu = TerminalMenu(template)
    chosen = terminal_menu.show()
    return template[chosen] if chosen is not None else None
