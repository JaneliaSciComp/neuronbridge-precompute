## Run Gradient Score

To run color depth search set the following environment variables to the correct values in .env:
```
# TOTAL_MASK_NEURONS is the number of distinct neurons - defaults to MASKS_COUNT
TOTAL_MASK_NEURONS=
NEURONS_PER_JOB=
```

To run gradient scoring process use:
`submit-normalize-ga.sh <anatomical-area>`
where anatomical-area is: brain or vnc
