## Run Color Depth Search

To run color depth search set the following environment variables to the correct values in .env:

```
MASKS_LIBRARY=
TARGETS_LIBRARY=
MASKS_COUNT=
TARGETS_COUNT=
```

To run it on the grid set, otherwise everything will run on the localhost
```
RUN_CMD=gridRun
```

then for brain run:
`submit-cds.sh brain`
or for VNC run:
`submit-cds.sh vnc`


## Run Color Depth Search with Nextflow
If you have nextflow installed you can run it using
`run-cds-workflow.sh brain [<additional_cds_args>]`
or
`run-cds-workflow.sh vnc [<additional_cds_args>]`
