# Chains Snakemake workflow

[![Snakemake](https://img.shields.io/badge/snakemake-≥3.12.0-brightgreen.svg)](https://snakemake.bitbucket.io)

This is a [Snakemake](https://snakemake.bitbucket.io) workflow for building ensembles of hydrologic model simulations from various climate forcings.
The workflow implements a "chain-of-models" configuration that includes reading from existing downscaling datasets, meteorological disaggregation, hydrologic modeling, and streamflow routing.
This work was developed specifically to support the [Quantitative Hydrologic Storylines](https://storylines.readthedocs.io/en/latest/) project.

## Authors

* Joe Hamman (@jhamman)

## Usage

### Step 1: Install workflow

If you simply want to use this workflow, download and extract the [latest release](https://github.com/jhamman/chains/releases).
If you intend to modify and further develop this workflow, fork this repository. Please consider providing any generally applicable modifications via a pull request.

In any case, if you use this workflow in a paper, don't forget to give credits to the authors by citing the URL of this repository and, if available, its DOI (see above).

### Step 2: Configure workflow

Configure the workflow according to your needs via editing the file `config.yaml`.

### Step 3: Execute workflow

Test your configuration by performing a dry-run via

    snakemake -n

Execute the workflow locally via

    snakemake --cores $N

using `$N` cores or run it in a cluster environment via

    snakemake --cluster qsub --jobs 100

or

    snakemake --drmaa --jobs 100

See the [Snakemake documentation](https://snakemake.readthedocs.io) for further details.

## Testing

Tests cases are in the subfolder `.test`. They should be executed via continuous integration with Travis CI. -- TODO
