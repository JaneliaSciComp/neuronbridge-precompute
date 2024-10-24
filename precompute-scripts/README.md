# NeuronBridge Precompute Steps

Running the entire precompute pipeline requires the following steps:
* Import the segmented CDMs into JACS.
* Import the CDM library from JACS into NeuronBridge
* Run color depth pixel matching algorithm
* Run color depth shape scoring algorithm
* Normalize color depth search scores
* Tag color depth matches for export
* Export color depth matches
* Upload color depth matches to AWS

The first two steps are needed in order to import color depth MIPs from JACs into NeuronBridge database. If all MIPs and libraries are already available in JACS the first step is not required but sometimes we have to import new segmented MIPs in JACS and only when all data is available in JACS database and filestore (`/nrs/jacs/jacsdata/filestore/system/ColorDepthMIPs`) we can import it into NeuronBridge

## Step 1: Import the segmented CDMs into JACS

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

## Step 2: Import the CDM library from JACS into NeuronBridge

This process imports CDMs from JACS libraries into NeuronBridge. This require access to a JACS server.

```
nextflow run workflows/step0-import-cdms.nf \
    -params-file runs/raw-is-runs/vnc/import-neuronbridge-is-vnc-mips.json \
    --jacs_url "https://workstation.int.janelia.org/SCSW/JACS2SyncServices/v2" \
    --jacs_authorization "<authorization>"
```


## Step 3: Run color depth pixel matching algorithm

This process performs a color depths search between the specified masks and targets MIPs. Usually the masks are EM MIPs and the targets are LM MIPs but an EM to EM or LM to LM match is also possible. The masks and targets MIPs are defined by the corresponding `masks_library` and `targets_library` parameters. This process can be partitioned in multiple jobs and you have an option to run a range of jobs using `first_job` and `last_job` parameters or a specific list of jobs using `job_list` (job ids are 1-based and `first_job` and `last_job` parameters are inclusive). These jobs can run on a local host that has access both the database and the JACS filestore, on SciComp's kubernetes cluster or on Janelia's LSF cluster. The job size is determined by `cds_mask_batch_size` and `cds_target_batch_size`

```
nextflow run workflows/step1-cds.nf \
    -params-file runs/raw-is-runs/vnc/cds-manc-israw.json \
    --job_list 1
```

## Step 4: Run color depth shape scoring algorithm

This process calculates an shape based score for existing pixel matches. Because the scoring algorithm is "expensive" for each mask MIP we pick the top 300 published names that match the MIP and we only compute the scores for the corresponding pixel matches.

```
nextflow run workflows/step2-gradscore.nf \
    -params-file runs/raw-is-runs/vnc/gradscore-manc-israw.json \
    --first_job 1 --last_job 1
```