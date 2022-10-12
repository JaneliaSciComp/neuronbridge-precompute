# NeuronBridge utility scripts

[![DOI](https://zenodo.org/badge/380281044.svg)](https://zenodo.org/badge/latestdoi/380281044)

Utility scripts for indexing and transferring of NeuronBridge-related files. 

## Programs

| Program | Description |
| ------- | ----------- |
| backcheck_publishedurl.py | Backcheck publishedURL MongoDB collection with neuronMetadata |
| check_published_image.py | Compare image FlyLight counts between a publishing database and MongoDB |
| denormalize_s3.py | Create denormalization files for imagery in AWS S3 bucket |
| update_dynamodb_published_stacks.py | Update janelia-neuronbridge-published-stacks DynamoDB table |
| update_dynamodb_published_versioned.py | Update janelia-neuronbridge-published-* DynamoDB table |
| update_dynamodb_publishing_doi.py | Update janelia-neuronbridge-publishing-doi DynamoDB table |
| upload_cdms.py | Create order file to upload DDMs and variants to AWS S3 |
| upload_ppp.py | Create order files to copy and upload DDMs and variants to AWS S3 |

## Deprecated programs

| Program | Description |
| ------- | ----------- |
| build_dynamodb.py | Used to build commands to update janelia-neuronbridge-published-* DynamoDB table |
| convert_neuron_tiffs.py | Convert searchable neuron TIFFs into PNGs. Replaced with AWS Lambda function. |
| copy_ppp_imagery.py | Copy PPP imagery between filesystems and upload to AWS S3. Replaced with upload_ppp.py. |
| create_ppp_thumbnails.py | Create/update PPP thumbnails. Replaced with AWS Lambda function. |
| populate_published.py | Update janelia-neuronbridge-published-* DynamoDB table. Replaced with update_dynamodb_published_versioned.py. |
