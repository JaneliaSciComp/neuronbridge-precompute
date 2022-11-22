# NeuronBridge utility programs

Utility programs for indexing and transferring of NeuronBridge-related files. 

## Programs

### Process implementation

| Program | Description |
| ------- | ----------- |
| denormalize_s3.py | Create denormalization files for imagery in AWS S3 bucket |
| create_ppp_sync_submitter.py | Create a shell script to submit AWS S3 sync jobs to the cluster |
| update_dynamodb_published_skeletons.py | Update the janelia-neuronbridge-skeletons DynamoDB table |
| update_dynamodb_published_stacks.py | Update janelia-neuronbridge-published-stacks DynamoDB table |
| update_dynamodb_published_versioned.py | Update janelia-neuronbridge-published-* DynamoDB table |
| update_dynamodb_publishing_doi.py | Update janelia-neuronbridge-publishing-doi DynamoDB table |
| upload_cdms.py | Create order file to upload DDMs and variants to AWS S3 |
| upload_ppp.py | Create order files to copy and upload DDMs and variants to AWS S3 |

### Diagnostics and reporting
| Program | Description |
| ------- | ----------- |
| backcheck_publishedurl.py | Backcheck publishedURL MongoDB collection with neuronMetadata |
| check_neuronmetadata.py | Check entries in jacs:emBody with nueronbridge:neuron |
| check_published_image.py | Compare image FlyLight counts between a publishing database and MongoDB |
| process_check.py | Check datasets/libraries in steps in the complete backend process |

## Deprecated programs

| Program | Description |
| ------- | ----------- |
| build_dynamodb.py | Used to build commands to update janelia-neuronbridge-published-* DynamoDB table |
| convert_neuron_tiffs.py | Convert searchable neuron TIFFs into PNGs. Replaced with AWS Lambda function. |
| copy_ppp_imagery.py | Copy PPP imagery between filesystems and upload to AWS S3. Replaced with upload_ppp.py. |
| create_ppp_thumbnails.py | Create/update PPP thumbnails. Replaced with AWS Lambda function. |
| populate_published.py | Update janelia-neuronbridge-published-* DynamoDB table. Replaced with update_dynamodb_published_versioned.py. |
| ppp_progress.py | Display PPP processing status |
