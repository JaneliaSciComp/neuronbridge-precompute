''' bucket_status.py
    Show status for every NeuronBridge library on AWS S3
'''
import json
import re
import boto3
from aws_s3_lib import bucket_stats, get_objects, get_prefixes


def read_object(bucket, key):
    ''' Read a "counts_denormalized" object and return the number of objects
        Keyword arguments:
          bucket: bucket name
          key: object key
        Returns:
          Object count (0 if error)
    '''
    try:
        data = S3.get_object(Bucket=bucket, Key=key)
    except Exception: # pylint: disable=W0718
        return 0
    contents = data['Body'].read().decode("utf-8")
    data = json.loads(contents)
    if "objectCount" not in data:
        return 0
    return data["objectCount"]


def process_template_cdm(bucket, template):
    """ Process a single template in a CDM bucket
        Keyword arguments:
          bucket: bucket
          template: template
        Returns:
          None
    """
    libraries = get_prefixes(bucket, template)
    for library in libraries:
        if "Test" in library:
            continue
        images = read_object(bucket, "/".join([template, library,
                                               "counts_denormalized.json"]))
        parts = get_prefixes(bucket, "/".join([template, library,
                                               "searchable_neurons"]))
        if parts:
            neurons = read_object(bucket, "/".join([template, library,
                                            "searchable_neurons",
                                            "counts_denormalized.json"]))
            arr = []
            for part in parts:
                if part.isnumeric():
                    arr.append(int(part))
            arr = sorted(arr)
            last = arr[-1] if arr else ""
            prefix = "/".join([template, library,"searchable_neurons", "KEYS", "0"])
            objs = get_objects(bucket, prefix)
            version = objs[-1].split(".")[-1].replace("_", ".") if objs[-1] and ".v" in objs[-1] \
                else ""
            neurons = f"{int(neurons):,}"
            last = f"{int(last):,}"
        else:
            last = neurons = version = ""
        print(f"{template:<25}  {library:<34}  {int(images):>7,}  {neurons:>7}  {last:>5}  {version:>7}")


def process_template_ppp(bucket, template):
    """ Process a single template in a PPP bucket
        Keyword arguments:
          bucket: bucket
          template: template
        Returns:
          None
    """
    libraries = get_prefixes(bucket, template)
    for library in libraries:
        divs = get_prefixes(bucket, "/".join([template, library]))
        print(f"{template:<25}  {library:<25}  {len(divs):^9}")


def process_data(bucket):
    """ Process a single data bucket
        Keyword arguments:
          bucket: bucket
        Returns:
          None
    """
    versions = get_prefixes(bucket)
    for ver in versions:
        print(ver)


def process_manifold(bucket, typ):
    """ Process a single CDM bucket
        Keyword arguments:
          bucket: bucket
          typ: "cdm", "ppp", or "data"
        Returns:
          None
    """
    if typ == "data":
        process_data(bucket)
        return
    templates = get_prefixes(bucket)
    for template in templates:
        if not template.startswith("JRC"):
            continue
        if typ == "cdm":
            process_template_cdm(bucket, template)
        else:
            process_template_ppp(bucket, template)


def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return f"{num:.1f}{unit}{suffix}"
        num /= 1024.0
    return "{num:.1f}P{suffix}"


def process_bucket(name):
    """ Create a header for a single bucket
        Keyword arguments:
          name: bucket
        Returns:
          None
    """
    if name == "janelia-flylight-color-depth":
        bstat = bucket_stats(bucket=name, profile="FlyLightPDSAdmin")
    else:
        bstat = bucket_stats(bucket=name)
    if not bstat['objects']:
        return False
    print(f"\n{name}: {bstat['objects']:,} objects, {humansize(bstat['size'])}")
    return True


def process_buckets():
    """ Process all NeuronBridge CDM buckets
        Keyword arguments:
          None
        Returns:
          None
    """
    first = True
    for suffix in ("-dev", "-devpre", "-prodpre", ""):
        if first:
            first = False
        else:
            print("\n" + "-"*93)
        # CDM
        name = "janelia-flylight-color-depth" + suffix
        if process_bucket(name):
            print(f"{'Template':<25}  {'Library':<34}  {'Images':<7}  {'Neurons':>7}  " \
                  + f"{'Part':<5}  {'Version':>7}")
            process_manifold(name, "cdm")
        # PPP
        name = "janelia-ppp-match" + (suffix if suffix else "-prod")
        if process_bucket(name):
            print(f"{'Template':<25}  {'Library':<25}  {'Divisions':<9}")
            process_manifold(name, "ppp")
        # Data
        name = "janelia-neuronbridge-data" + (suffix if suffix else "-prod")
        if process_bucket(name):
            process_manifold(name, "data")


if __name__ == '__main__':
    S3 = boto3.client('s3')
    process_buckets()
