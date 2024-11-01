# NeuronBridge ETL programs

ETL programs for indexing and transferring of NeuronBridge-related files. 

## Programs

### Process implementation

| Program | Description |
| ------- | ----------- |
| add_supplementary_images.py | Add supplementary images to a dataset |
| create_ppp_sync_submitter.py | Create a shell script to submit AWS S3 sync jobs to the cluster |
| delete_cdm.py | Delete images/metadata for a slide code from NeuronBridge |
| find_gen1_representatives.py | Update publishedLMImage with imagery from FLEW |
| load_codex_to_mongo.py | Load FlyWire Codex to MongoDB |
| scrub_order.py | Scrub an order file of searchable_neurons images using a manifest |
| update_dynamodb_published_skeletons.py | Update the janelia-neuronbridge-skeletons DynamoDB table |
| update_dynamodb_published_stacks.py | Update janelia-neuronbridge-published-stacks DynamoDB table |
| update_dynamodb_published_versioned.py | Update janelia-neuronbridge-published-* DynamoDB table |
| update_dynamodb_publishing_doi.py | Update janelia-neuronbridge-publishing-doi DynamoDB table |
| upload_cdms.py | Create order file to upload DDMs and variants to AWS S3 |
| upload_ppp.py | Create order files to copy and upload DDMs and variants to AWS S3 |
| upload_precheck.py | Check data set prior to loading |

### Diagnostics and reporting
| Program | Description |
| ------- | ----------- |
| check_neuronmetadata.py | Reconcile entries jacs:emBody with nueronbridge:neuron |
| upload_precheck.py | Check for potential release issues, and optionally retag images |

## Deprecated programs

| Program | Description |
| ------- | ----------- |
| build_dynamodb.py | Used to build commands to update janelia-neuronbridge-published-* DynamoDB table |
| convert_neuron_tiffs.py | Convert searchable neuron TIFFs into PNGs. Replaced with AWS Lambda function. |
| copy_ppp_imagery.py | Copy PPP imagery between filesystems and upload to AWS S3. Replaced with upload_ppp.py. |
| create_ppp_thumbnails.py | Create/update PPP thumbnails. Replaced with AWS Lambda function. |
| populate_published.py | Update janelia-neuronbridge-published-* DynamoDB table. Replaced with update_dynamodb_published_versioned.py. |
| ppp_progress.py | Display PPP processing status |
