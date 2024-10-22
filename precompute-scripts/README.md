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

## Step 1 Import segmentation into JACS filestore

This process is needed to import segmented MIPs or brand new MIPs (EM use case) into JACS.

The import process for EM MIPs is slightly different from importing LM MIPs. EM data comes from an outside source whereas LM already exists in the JACS database.

For EM we import everything, which includes the display CDMs, the searchable CDMs, gradient images and RGB20x images. For LM we only import the segmented MIPs (searchable CDMs, gradient images and RGB20x images - the source MIPs are alredy in the JACS database)

So EM import will require `display_cdm_location` whereas LM import will require `jacs_url` and `jacs_authorization` parameters.

To only simulate the import you can use `dry_run` set to true and you can set it to false in the command line when you are ready to run the command.

Example LM import:
```
nextflow run workflows/pre-step0-create-jacs-library.nf \
    -params-file runs/raw-is-runs/vnc/import-libstore-is-vnc-mips.json \
    --jacs_url "http://10.40.2.131:8800/api/rest-v2" \
    --jacs_authorization "APIKEY MyKey" \
    --dry_run false
```

Example EM import:
```
nextflow run workflows/pre-step0-create-jacs-library.nf \
    -params-file runs/manc-runs/import-libstore-manc-mips.json \
    --dry_run false
```
