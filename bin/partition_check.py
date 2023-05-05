''' partition_check.py
    Find the last partition for every NeuronBridge library on AWS S3
'''
import json
import sys
import boto3
from aws_s3_lib import get_prefixes

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
    except Exception:
        return 0
    contents = data['Body'].read().decode("utf-8")
    data = json.loads(contents)
    if "objectCount" not in data:
        return 0
    return data["objectCount"]


def process_template(bucket, template):
    """ Process a single template in a bucket
        Keyword arguments:
          bucket: bucket
          tample: template
        Returns:
          None
    """
    manifold = bucket.split("-")[-1]
    if manifold == "depth":
        manifold = "prod"
    libraries = get_prefixes(bucket, template)
    for library in libraries:
        if "Test" in library:
            continue
        images = read_object(bucket, "/".join([template, library, "counts_denormalized.json"]))
        parts = get_prefixes(bucket, "/".join([template, library, "searchable_neurons"]))
        if parts:
            neurons = read_object(bucket, "/".join([template, library,
                                            "searchable_neurons", "counts_denormalized.json"]))
            arr = []
            for part in parts:
                if part.isnumeric():
                    arr.append(int(part))
            arr = sorted(arr)
            last = arr[-1]
        else:
            last = neurons = ""
        print(f"{manifold:<8}  {template:<25}  {library:<34}  {images:>6}  {neurons:>7}  {last:>4}")


def process_manifold(bucket):
    """ Process a single bucket
        Keyword arguments:
          bucket: bucket
        Returns:
          None
    """
    templates = get_prefixes(bucket)
    for template in templates:
        if not template.startswith("JRC"):
            continue
        process_template(bucket, template)


def process_buckets():
    """ Process all NeuronBridge cCDM buckets
        Keyword arguments:
          None
        Returns:
          None
    """
    print(f"{'Manifold':<8}  {'Template':<25}  {'Library':<34}  {'Images':>6}  {'Neurons':>7}  {'Part':>4}")
    for suffix in ("-dev", "-devpre", "-prodpre", ""):
        process_manifold("janelia-flylight-color-depth" + suffix)


if __name__ == '__main__':
    S3 = boto3.client('s3')
    process_buckets()
