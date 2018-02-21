# The main entry point of your workflow.
# After configuring, running snakemake -n in a clone of this repository should
# successfully execute a dry-run of the workflow.
# snakemake -j 4 --cluster-config cluster.json --cluster "qsub -l walltime={cluster.walltime} -l select={cluster.n} -A {cluster.account} -q {cluster.queue} -j oe" --use-conda

import os

# Configuration options (probably should move to another file)
GCMS = ['access13', 'canesm', 'cesm', 'cnrm', 'gfdl', 'giss', 'ipsl', 'miroc5', 'mri', 'noresm']
SCENS = ['rcp45', 'rcp85']
HIST_YEARS = list(range(1950, 2006))
RCP_YEARS = list(range(2006, 2100))
icar_filename = '/glade/p/ral/RHAP/trude/conus_icar/qm_data/{gcm}_{scen}_exl_conv.nc'


dirs = {}
dirs['root'] = '/glade2/scratch2/jhamman/testflow/'
dirs['metsim'] = os.path.join(dirs['root'], 'metsim')
dirs['config'] = os.path.join(dirs['root'], 'config')

for k, d in dirs.items():
    os.makedirs(d, exist_ok=True)

metsim_prefix = 'metsim_{gcm}_{scen}_'
metsim_fname = os.path.join(
    dirs['metsim'], metsim_prefix + '{year}0101-{year}1231.nc'),

# configfile: "config.json"
# ------------------------------------------------------------------------------

# utilities


def metsim_run_inputs(wildcards):
    out = {}
    out['config'] = os.path.join(
        dirs['config'],
        'metsim.{wcs.gcm}.{wcs.scen}.{wcs.year}.cfg'.format(wcs=wildcards))
    if wildcards.year == 2006:
        out['state'] = make_metsim_statename(wildcards.gcm, 'hist',
                                             int(wildcards.year) - 1)
    return out


def make_metsim_statename(gcm, scen, year):
    prefix = metsim_prefix.format(gcm=gcm, scen=scen) + str(year)
    return os.path.join(dirs['metsim'], prefix + '.nc')

# ------------------------------------------------------------------------------

# rules

localrules: all, metsim_pre

rule all:
    input:
        expand(metsim_fname, gcm=GCMS, scen=['hist'], year=HIST_YEARS),
        expand(metsim_fname, gcm=GCMS, scen=SCENS, year=RCP_YEARS)


rule metsim_run:
    input:
        unpack(metsim_run_inputs)
    output:
        filename = metsim_fname,
        state = make_metsim_statename('{gcm}', '{scen}', '{year}')
    # uncomment if you want to run metsim from this conda environment
    # conda:
    #     "envs/metsim.yaml"
    shell:
        # uncomment to debug (instead of running metsim)
        # "touch {output.filename};"
        # "touch {output.state};"
        # "echo ms -v -n {input.config}"
        "ms -n 10 {input.config}"


rule metsim_pre:
    input:
        template = "templates/newman_metsim_template.cfg",
    params:
        gcm = '{gcm}', scen = '{scen}', year = '{year}'
    output:
        os.path.join(dirs['config'], 'metsim.{gcm}.{scen}.{year}.cfg')
    run:
        forcing = icar_filename.format(**params)
        out_state = make_metsim_statename(params.gcm, params.scen, params.year)

        if params.year in ['1950', '2006']:
            in_state = make_metsim_statename(params.gcm, 'hist',
                                             int(params.year) - 1)
        else:
            in_state = forcing

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output[0], 'w') as f:
            f.write(template.format(year=params.year,
                                    forcing=forcing,
                                    metsim_dir=dirs['metsim'],
                                    prefix=metsim_prefix.format(**params),
                                    input_state=in_state,
                                    output_state=out_state))
