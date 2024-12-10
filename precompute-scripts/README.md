# NeuronBridge Precompute Steps

The precompute pipeline require [nextflow](https://www.nextflow.io) and JDK 17 or newer (newer versions of nextflow will no longer run with JDK 8)

The full precompute pipeline has the following steps:
* Import the segmented CDMs into JACS.
* Import the CDM library from JACS into NeuronBridge
* Run color depth pixel matching algorithm
* Run color depth shape scoring algorithm
* Normalize color depth search scores
* Tag color depth matches for export
* Export color depth matches
* Upload color depth matches to AWS

The first two steps are needed only for importing color depth MIPs from JACs into NeuronBridge database. If all MIPs and libraries are already available in JACS the first step is not required but sometimes we have to import new segmented MIPs in JACS and only when all data is available in JACS database and filestore (`/nrs/jacs/jacsdata/filestore/system/ColorDepthMIPs`) we can import it into NeuronBridge

## Pre Step 1: Import the segmented CDMs into JACS

This process is needed to import new segmented MIPs or brand new MIPs (EM use case) into JACS. Practically in this step we copy segmented MIPs to JACS filestore (`/nrs/jacs/jacsData/filestore/system/ColorDepthMIPs`). In some cases (usually for new EM imports) we need to sync these new MIPs and create a new library in JACS if such library does not exist.

The import process for EM MIPs is slightly different from importing LM MIPs because EM data comes from an outside source whereas LM already exists in the JACS database.

For EM we import everything, which includes the display CDMs, the searchable CDMs, gradient images and RGB20x images. For LM we only import the segmented MIPs (searchable CDMs, gradient images and RGB20x images - the source MIPs are alredy in the JACS database)

So EM import will require `display_cdm_location` whereas LM import will require `jacs_url` and `jacs_authorization` parameters.

To only simulate the import you can use `dry_run` set to true and you can set it to false in the command line when you are ready to run the command.

Example LM import:
```
nextflow run workflows/pre-step0-create-jacs-library.nf \
    -params-file runs/raw-is-runs/vnc/import-libstore-is-vnc-mips.json \
    --jacs_url "https://workstation.int.janelia.org/SCSW/JACS2SyncServices/v2" \
    --jacs_authorization "<authorization>" \
    --dry_run false
```

Example EM import:
```
nextflow run workflows/pre-step0-create-jacs-library.nf \
    -params-file runs/manc-runs/import-libstore-manc-mips.json \
    --dry_run false
```

## Pre Step 2: Import the CDM library from JACS into NeuronBridge

This process imports CDMs from JACS libraries into NeuronBridge. This require access to a JACS server. The authorization parameter can be either a bearer token or an API key

```
nextflow run workflows/step0-import-cdms.nf \
    -params-file runs/raw-is-runs/vnc/import-neuronbridge-is-vnc-mips.json \
    --jacs_url "https://workstation.int.janelia.org/SCSW/JACS2SyncServices/v2" \
    --jacs_authorization "<authorization>" \
    --db_config db-config.properties
```

In the example above the database configuration parameter (`db_config`) references a file `db-config.properties`. This file must be manually created or the and it must point to the corresponding neuronbridge database. This applies everywhere where access to the NeuronBridge database is needed.

For development database you can run:
```
cat > db-config.properties <<EOF
MongoDB.Server=dev-mongodb1:27017,dev-mongodb2:27017,dev-mongodb3:27017
MongoDB.AuthDatabase=admin
MongoDB.Database=neuronbridge
MongoDB.Username=<enter username here>
MongoDB.Password=<enter password here>
MongoDB.ReplicaSet=rsDev
MongoDB.UseSSL=
MongoDB.Connections=500
MongoDB.ConnectionTimeoutMillis=
MongoDB.MaxConnecting=20
MongoDB.MaxConnectTimeSecs=
MongoDB.MaxConnectionIdleSecs=
MongoDB.MaxConnectionLifeSecs=
EOF
```

For production database you can run:
```
cat > db-config.properties <<EOF
MongoDB.Server=jacs-mongodb1:27017,jacs-mongodb2:27017,jacs-mongodb3:27017
MongoDB.AuthDatabase=admin
MongoDB.Database=neuronbridge
MongoDB.Username=<enter username here>
MongoDB.Password=<enter password here>
MongoDB.ReplicaSet=rsProd
MongoDB.UseSSL=
MongoDB.Connections=500
MongoDB.ConnectionTimeoutMillis=
MongoDB.MaxConnecting=500
MongoDB.MaxConnectTimeSecs=
MongoDB.MaxConnectionIdleSecs=30
MongoDB.MaxConnectionLifeSecs=120
EOF
```

## Step 1: Run color depth pixel matching algorithm

This process performs a color depths search between the specified masks and targets MIPs. Usually the masks are EM MIPs and the targets are LM MIPs but an EM to EM or LM to LM match is also possible. The masks and targets MIPs are defined by the corresponding `masks_library` and `targets_library` parameters. This process can be partitioned in multiple jobs and you have an option to run a range of jobs using `first_job` and `last_job` parameters or a specific list of jobs using `job_list` (job ids are 1-based and `first_job` and `last_job` parameters are inclusive). These jobs can run on a local host that has access both the database and the JACS filestore, on SciComp's kubernetes cluster or on Janelia's LSF cluster. The job size is determined by `cds_mask_batch_size` and `cds_target_batch_size`

```
nextflow run workflows/step1-cds.nf \
    -params-file runs/raw-is-runs/vnc/cds-manc-israw.json \
    --job_list 1
```

## Step 2: Run color depth shape scoring algorithm

This process calculates an shape based score for existing pixel matches. Because the shape scoring algorithm is "expensive" for each mask we only score the top 300 published names in terms of pixel match scores that match the mask's MIP.

```
nextflow run workflows/step2-gradscore.nf \
    -params-file runs/raw-is-runs/vnc/gradscore-manc-israw.json \
    --first_job 1 --last_job 1
```

## Step 4: Tag data

Tagging will help us track NeuronBridge data versions. To tag simply run:
```
nextflow run workflow/step4-tag.nf \
    --anatomical_area brain \
    --db_config db-config.properties
```
and/or
```
nextflow run workflow/step4-tag.nf \
    --anatomical_area vnc \
    --db_config db-config.properties
```

## Step 6. Normalize color depth search scores

Typically the shape scoring algorithm also normalizes the score, so this step is needed when we only compute the shape scores for a subset of a library. If only a subset of a library MIPs is selected (based on command line parameters) than these may skew the normalized score because the scores have to be normalized with respect to the entire library.

```
nextflow run workflows/step6-normalize-gradscore.nf \
    --db_config db-config.properties \
    --anatomical_area brain \
    --masks_library flyem_hemibrain_1_2_1 \
    --targets_library "flylight_gen1_mcfo_published,flylight_annotator_gen1_mcfo_published"
```

## Step 7: Export color depth matches

NeuronBridge requires all its metadata to be exported from the database to the file system and then uploaded to AWS. This step performs the first part - export data from the internal database to the file system. Before starting the export make sure that all the imagery has been already uploaded to AWS because this step will only output entries for which the image files were marked as exported to AWS - it will not check the AWS but it relies on a collection that specifies all files uploaded to AWS. Currently there are 8 total export operations - 3 per anatomical area (brain or VNC) to export color depth (CDM) and patch per pixel (PPPM) match
 * EM_CD_MATCHES
 * LM_CD_MATCHES
 * EM_PPP_MATCHES
and 2 that export data for both anatomical areas (brain and VNC) - one for brain+vnc EM MIPs and one for brain+vnc LM MIPs
 * EM_MIPS
 * LM_MIPS

## Step 8: Validate export data
Running export data validation requires a conda environment. Check (NeuronBridge python tools)[https://github.com/JaneliaSciComp/neuronbridge-python.git] how to setup the conda environment

```
nextflow run workflows/step8-validate-export.nf
```

## Step 9: Upload color depth matches to AWS
Before uploading to AWS you need to setup your AWS credentials as nextflow secrets:
```
nextflow secrets set AWS_ACCESS_KEY <youraccesskey>
nextflow secrets set AWS_SECRET_KEY <yoursecret>
```
Upload the MIPs with:
```
nextflow run workflows/step9-upload.nf \
         --dry_run false \
         --upload_anatomical_areas brain+vnc \
         --upload_type "EM_MIPS,LM_MIPS"
```

Upload the matches with:
```
nextflow run workflows/step8-upload.nf \
         --dry_run false \
         --upload_anatomical_areas "brain,vnc" \
         --upload_type "EM_CD_MATCHES,LM_CD_MATCHES,EM_PPP_MATCHES"
```