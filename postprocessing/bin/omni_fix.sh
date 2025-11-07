#!/bin/bash

if [ "$#" -lt 2 ]
then
  echo "Missing parameters"
  exit
fi
SLIDE=$1
SAMPLE=$2
#python3 ../../reports/bin/sample_status.py --slide ${SLIDE}

python3 delete_cdm.py --verbose --manifold prod --version 3.7.2 --library FlyLight_Split-GAL4_Omnibus_Broad --accept --write --template JRC2018_Unisex_20x_HR --sample ${SAMPLE}

python3 delete_cdm.py --verbose --manifold prod --version 3.7.2 --library FlyLight_Split-GAL4_Omnibus_Broad --accept --write --template JRC2018_VNC_Unisex_40x_DS --sample ${SAMPLE}

python3 ../../../FL-web/alps/postprocessing/bin/update_publishedimage_collection.py --database mbew --write --slide ${SLIDE}

python3 ../../bin/update_dynamodb_published_stacks.py --verbose --write --slide ${SLIDE}

#python3 ../../reports/bin/sample_status.py --slide ${SLIDE}
