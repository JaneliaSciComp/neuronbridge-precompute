''' create_ppp_sync_submitter.py
    Create a shell script to submit AWS S3 sync jobs to the cluster
    for PPP imagery.
'''

import argparse
import re
import os
import colorlog
import inquirer
import aws_s3_common.aws_s3_common as AW

# Configuration
MANIFOLDS = ['dev', 'prod', 'devpre', 'prodpre']
BASE = "/nrs/neuronbridge/ppp_imagery"


def get_dirlist(start=BASE, regex=None):
    """ Get a list of directories
        Keyword arguments:
          start: start directory
          regex: filtering regex
        Returns:
          List of directories
    """
    files = os.listdir(start)
    if regex:
        files = [file for file in files if os.path.isdir(start+'/' + file) \
                                           and re.search(regex, file)]
    else:
        files = [file for file in files if os.path.isdir(start+'/' + file)]
    return files


def create_commands():
    """ Produce a file with commands to run AWS syncs
        Keyword arguments:
          None
        Returns:
          None
    """
    # Get local paths
    paths = []
    versions = get_dirlist(BASE, "^v\d+")
    for version in versions:
        libs = get_dirlist("/".join([BASE, version]), "Fly")
        for lib in libs:
            paths.append("/".join([version, lib]))
    choices = [inquirer.List("dir",
                             "Choose a version/library",
                             paths)]
    source = inquirer.prompt(choices)["dir"]
    subsets = get_dirlist("/".join([BASE, source]), "^\d+$")
    # Get source prefix
    bucket = "janelia-ppp-match-" + ARG.MANIFOLD
    prefixes = AW.get_objects(bucket)
    prefixes = [prefix for prefix in prefixes if "JRC" in prefix]
    if len(prefixes) == 1:
        template = prefixes[0]
    else:
        choices = [inquirer.List("prefix",
                                 "Choose a template",
                                 prefixes)]
        template = inquirer.prompt(choices)["prefix"]
    print(f"Found {len(subsets)} subsets")
    # Write file
    with open("ppp_sync.sh", 'w', encoding='ascii') as output:
        for sub in subsets:
            source_dir = "/".join([BASE, source, sub])
            lib = source.split("/")[-1]
            s3_prefix = "/".join([template, lib, sub])
            output.write(f"bsub -J ppp_{sub} -n 4 -P neuronbridge "
                         + '"' + f"aws s3 sync {source_dir} "
                         + f"s3://{bucket}/{s3_prefix}"
                         + ' --only-show-errors"\n')

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Create PPP sync commands")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=MANIFOLDS, default="devpre", help='S3 manifold')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    create_commands()
