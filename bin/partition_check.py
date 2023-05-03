''' partition_check.py
    Find the last partition for every NeuronBridge library on AWS S3
'''

from aws_s3_lib import get_prefixes

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
        parts = get_prefixes(bucket, "/".join([template, library, "searchable_neurons"]))
        if parts:
            arr = []
            for part in parts:
                if part.isnumeric():
                    arr.append(int(part))
            arr = sorted(arr)
            last = arr[-1]
        else:
            last = ""
        print(f"{manifold:<7}  {template:<25}  {library:<34}  {last}")


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
    for suffix in ("-dev", "-devpre", "-prodpre", ""):
        process_manifold("janelia-flylight-color-depth" + suffix)


if __name__ == '__main__':
    process_buckets()
