# NeuronBridge reporting programs

Programs for reporting/diagnostics

## Programs

| Program | Description |
| ------- | ----------- |
| backcheck_publishedurl.py | Backcheck publishedURL MongoDB collection with neuronMetadata |
| bucket_status.py | Show current status of AWS S3 CDM buckets |
| check_neuronmetadata.py | Reconcile entries jacs:emBody with nueronbridge:neuron |
| crosscheck_library.py | crosscheck samples in neuronMetadata and publishedURL |
| dataset_report.py | Report on status of EM data sets |
| find_changed_releases.py | Find samples whose releases have changed |
| find_duplicate_slides.py | Find slide codes with mutiple samples in neuronBridge |
| process_check.py | Check datasets/libraries in steps in the complete backend process |
| publishing_check.py | check sample IDs from a publishing database against the publishedURL table |
| sample_status.py | Show the status of a sample, slide code or body ID in SAGE, MongoDB, and AWS (S3 and DynamoDB) |
