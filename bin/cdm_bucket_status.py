''' cdm_bucket_status.py
    Show status for every NeuronBridge library on AWS S3
'''
import json
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
          tample: template
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
            last = arr[-1]
            prefix = "/".join([template, library,"searchable_neurons", "KEYS", "0"])
            objs = get_objects(bucket, prefix)
            version = objs[-1].split(".")[-1].replace("_", ".") if objs[-1] and ".v" in objs[-1] \
                else ""
        else:
            last = neurons = version = ""
        print(f"{template:<25}  {library:<34}  {images:>6}  {neurons:>7}  {last:>4}  {version:>7}")


def process_template_ppp(bucket, template):
    """ Process a single template in a PPP bucket
        Keyword arguments:
          bucket: bucket
          tample: template
        Returns:
          None
    """
    libraries = get_prefixes(bucket, template)
    for library in libraries:
        divs = get_prefixes(bucket, "/".join([template, library]))
        print(f"{template:<25}  {library:<25}  {len(divs):^9}")


def process_manifold(bucket, typ):
    """ Process a single CDM bucket
        Keyword arguments:
          bucket: bucket
          typ: "cdm" or "ppp"
        Returns:
          None
    """
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


def process_buckets():
    """ Process all NeuronBridge CDM buckets
        Keyword arguments:
          None
        Returns:
          None
    """
    first = True
    for suffix in ("-dev", "-devpre", "-prodpre", ""):
        name = "janelia-flylight-color-depth" + suffix
        bstat = bucket_stats(bucket=name, profile="" if suffix else "FlyLightPDSAdmin")
        if not bstat['objects']:
            continue
        if first:
            first = False
        else:
            print("\n" + "-"*93)
        print(f"\n{name}: {bstat['objects']:,} objects, {humansize(bstat['size'])}")
        print(f"{'Template':<25}  {'Library':<34}  {'Images':>6}  {'Neurons':>7}  " \
              + f"{'Part':>4}  {'Version':>7}")
        process_manifold(name, "cdm")
        name = 'janelia-ppp-match' + suffix
        if not suffix:
            name += "-prod"
        bstat = bucket_stats(bucket=name)
        if not bstat['objects']:
            continue
        print(f"\n{name}: {bstat['objects']:,} objects, {humansize(bstat['size'])}")
        print(f"{'Template':<25}  {'Library':<25}  {'Divisions':<9}")
        process_manifold(name, "ppp")

if __name__ == '__main__':
    S3 = boto3.client('s3')
    process_buckets()
