import glob
import logging
import os
import shutil

from itertools import product

from datetime import datetime

import pandas as pd
import xarray as xr

from dask.distributed import Client

from tools.utilities import make_case_readme, log_to_readme

configfile: "/glade/u/home/jhamman/projects/storylines/storylines_workflow/config.yml"


case_dirs = ['configs', 'disagg_data', 'hydro_data', 'routing_data',
             'downscaling_data', 'logs']


def get_year_range(years):
    return list(range(years['start'], years['stop'] + 1))


def hydro_forcings(wcs):
    force_timestep = config['HYDROLOGY'][wcs.model]['force_timestep']

    if 'vic' in wcs.model.lower():
        years = get_year_range(config['SCEN_YEARS'][wcs.scen])
        return [VIC_FORCING.format(year=year, gcm=wcs.gcm, scen=wcs.scen,
                                   dsm=wcs.dsm,
                                   disagg_method=wcs.disagg_method,
                                   disagg_ts=force_timestep)
                for year in years]
    elif 'prms' in wcs.model.lower():
        return PRMS_FORCINGS.format(disagg_ts=force_timestep, **wcs)
    else:
        raise NotImplementedError


def hydro_executable(wcs):
    return config['HYDROLOGY'][wcs.model]['executable']


def hydro_template(wcs):
    return config['HYDROLOGY'][wcs.model]['template']


# Workflow bookends
onsuccess:
    print('Workflow finished, no error')

onerror:
    print('An error occurred')

onstart:
    print('starting now')

# Readme / documentation
CASEDIR = os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}')
README = os.path.join(CASEDIR, 'readme.md')
BENCHMARK = os.path.join(CASEDIR, 'benchmark.txt')

# Downscaling temporary filenames
DOWNSCALING_DIR = os.path.join(CASEDIR, 'downscaling_data')
DOWNSCALING_DATA = os.path.join(
    DOWNSCALING_DIR, 'downscale.{gcm}.{scen}.{dsm}.{year}.nc')
DOWNSCALING_LOG = os.path.join(
    CASEDIR, 'logs',
    'downscale.{gcm}.{scen}.{dsm}.{year}.%Y%m%d-%H%M%d.log.txt')


CONFIGS_DIR = os.path.join(CASEDIR, 'configs')

# Metsim Filenames
DISAGG_DIR = os.path.join(CASEDIR, 'disagg_data')
DISAGG_CONFIG = os.path.join(
    CONFIGS_DIR, 'config.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.{year}.cfg')
DISAGG_LOG = os.path.join(
    CASEDIR, 'logs', 'disagg.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.{year}.%Y%m%d-%H%M%d.log.txt')
METSIM_STATE = os.path.join(
    DISAGG_DIR, 'state.{gcm}.{scen}.{dsm}.{disagg_method}.{year}1231.nc')
DISAGG_PREFIX = 'force.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}'
DISAGG_OUTPUT = os.path.join(DISAGG_DIR,
                             DISAGG_PREFIX + '_{year}0101-{year}1231.nc')


DISAGG_OUTPUT_PRMS = os.path.join(
    DISAGG_DIR,
    'forcing.prms.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.nc')  # All in one file

# Hydrology
HYDRO_OUT_DIR = os.path.join(CASEDIR, 'hydro_data')

HYDRO_OUTPUT = os.path.join(
    HYDRO_OUT_DIR, 'hist.{model_id}.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.{outstep}.nc')
HYDRO_CONFIG = os.path.join(
    CONFIGS_DIR, 'config.{model_id}.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.txt')
HYDRO_LOG = os.path.join(
    CASEDIR, 'logs', 'hydro.{model_id}.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.%Y%m%d-%H%M%d.log.txt')

# VIC Filenames

# e.g.state.vic.MRI-CGCM3.hist.bcsd.metsim.vic_mpr.20051231_00000.nc
VIC_STATE_PREFIX = os.path.join(
    HYDRO_OUT_DIR, 'state.vic.{gcm}.{scen}.{dsm}.{disagg_method}.{model}')
VIC_STATE = os.path.join(
    HYDRO_OUT_DIR, 'state.vic.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.nc')
VIC_FORCING = os.path.join(
    DISAGG_DIR,
    'forcing.vic.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.{year}.nc')  # No months/days
VIC_OUT_PREFIX = 'hist.vic.{gcm}.{scen}.{dsm}.{disagg_method}.{model}'

# PRMS Forcings
PRMS_FORCINGS = os.path.join(
    DISAGG_DIR, 'forcing.prms.{gcm}.{scen}.{dsm}.{disagg_method}.nc')  # No months/days
PRMS_OUTPUT = os.path.join(
    HYDRO_OUT_DIR, 'hist.prms.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.nc')
PRMS_STATE = os.path.join(
    HYDRO_OUT_DIR, 'state.prms.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.bin')

# Dummy files
NULL_STATE = 'null_state'  # for runs that don't get an initial state file

# Default Values
DEFAULT_WIND = 1.
TINY_TEMP = 1e-10

# Constants
KELVIN = 273.16
SEC_PER_DAY = 86400
MM_PER_IN = 25.4

NOW = datetime.now()
# Rules
# -----------------------------------------------------------------------------
localrules: readme, setup_casedirs, hydrology_models, disagg_methods, config_hydro_models, disagg_configs

include: "downscaling.snakefile"
include: "metsim.snakefile"
include: "vic.snakefile"
include: "prms.snakefile"

# readme / logs
rule readme:
    input:
        expand(README,
               gcm=config['GCMS'], scen=config['SCENARIOS'],
               dsm=config['DOWNSCALING_METHODS'])


rule setup_casedirs:
    output: README
    run:
        case_dir = os.path.dirname(output[0])
        for d in case_dirs:
            os.makedirs(os.path.join(case_dir, d), exist_ok=True)

        make_case_readme(wildcards,
                         os.path.join(case_dir, 'readme.md'),
                         disagg_methods=config['DISAGG_METHODS'],
                         hydro_methods=config['HYDRO_METHODS'],
                         routing_methods=config['ROUTING_METHODS'])


def filter_combinator(combinator, blacklist):
    # https://stackoverflow.com/a/41185568/1757464
    def filtered_combinator(*args, **kwargs):
        for wc_comb in combinator(*args, **kwargs):
            # Use frozenset instead of tuple
            # in order to accomodate
            # unpredictable wildcard order
            d = dict(wc_comb)
            if d['model_id'] in d['model']:
                yield frozenset(wc_comb)
            # if frozenset(wc_comb) not in blacklist:
            #
            #     yield wc_comb
    return filtered_combinator


blacklist = []  # TODO filter out missing gcms (or implement a subselection method)
filtered_product = filter_combinator(product, blacklist)


rule config_hydro_models:
    input:
        expand(HYDRO_CONFIG, filtered_product,
               gcm=config['GCMS'], scen=config['SCENARIOS'],
               disagg_method=config['DISAGG_METHODS'],
               dsm=config['DOWNSCALING_METHODS'],
               model=list(config['HYDRO_METHODS'].keys()),
               model_id=set(config['HYDRO_METHODS'].values()))

rule make_vic_forcings:
    input:
        [expand(VIC_FORCING, filtered_product,
                gcm=config['GCMS'], scen=scen,
                disagg_method=config['DISAGG_METHODS'],
                dsm=config['DOWNSCALING_METHODS'],
                model=list(config['HYDRO_METHODS'].keys()),
                disagg_ts=['60'],
                model_id=set(config['HYDRO_METHODS'].values()),
                year=get_year_range(config['SCEN_YEARS'][scen]))
         for scen in config['SCENARIOS']]

# Hydrologic Models
rule hydrology_models:
    input:
        expand(HYDRO_OUTPUT, filtered_product,
               gcm=config['GCMS'], scen=config['SCENARIOS'],
               disagg_method=config['DISAGG_METHODS'],
               dsm=config['DOWNSCALING_METHODS'],
               model=list(config['HYDRO_METHODS'].keys()),
               model_id=set(config['HYDRO_METHODS'].values()),
               outstep=['daily', 'monthly'])


# Hydrologic Models
rule hydrology_models_obs:
    input:
        expand(HYDRO_OUTPUT, filtered_product,
               gcm=['obs'], scen=['obs_hist'],
               dsm=config['OBS_FORCING'].keys(),
               disagg_method=config['DISAGG_METHODS'],
               model=list(config['HYDRO_METHODS'].keys()),
               model_id=set(config['HYDRO_METHODS'].values()),
               outstep=['daily', 'monthly'])


# Disaggregation methods
rule downscaling_data:
    input:
        [expand(DOWNSCALING_DATA,
                gcm=config['GCMS'], scen=scen,
                dsm=config['DOWNSCALING_METHODS'],
                year=get_year_range(config['SCEN_YEARS'][scen]))
         for scen in config['SCENARIOS']]

rule disagg_configs:
    input:
        [expand(DISAGG_CONFIG,
                gcm=config['GCMS'], scen=scen,
                dsm=config['DOWNSCALING_METHODS'],
                disagg_ts=[60, 1440],
                disagg_method=config['DISAGG_METHODS'],
                year=get_year_range(config['SCEN_YEARS'][scen]))
         for scen in config['SCENARIOS']]

rule disagg_methods:
    input:
        [expand(DISAGG_OUTPUT,
                gcm=config['GCMS'], scen=scen,
                dsm=config['DOWNSCALING_METHODS'],
                disagg_ts=[60, 1440],
                disagg_method=config['DISAGG_METHODS'],
                year=get_year_range(config['SCEN_YEARS'][scen]))
         for scen in config['SCENARIOS']]



rule dummy_state:
    output: NULL_STATE
    shell: "touch {output}"
