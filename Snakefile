import glob
import os

from itertools import product

import pandas as pd
import xarray as xr

import dask.multiprocessing
from dask_jobqueue import PBSCluster
from dask.distributed import Client

from tools.utilities import make_case_readme, log_to_readme

configfile: "/glade/u/home/jhamman/projects/storylines/storylines_workflow/config.yml"


case_dirs = ['configs', 'disagg_data', 'hydro_data', 'routing_data',
             'downscaling_data', 'logs']


def get_year_range(years):
    return list(range(years['start'], years['stop'] + 1))


def inverse_lookup(d, key):
    for k, v in d.items():
        if v == key:
            return k
    raise KeyError(key)


def maybe_make_cfg_list(obj):
    if isinstance(obj, str) or not hasattr(obj, '__iter__'):
        return obj
    elif len(obj) == 1:
        return obj[0]
    return '%s' % ', '.join(obj)


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


def prms_forcings_from_disagg(wcs):
    years = get_year_range(config['SCEN_YEARS'][wcs.scen])
    force_timestep = '1440'
    return [DISAGG_OUTPUT.format(year=year, gcm=wcs.gcm, scen=wcs.scen,
                                 dsm=wcs.dsm,
                                 disagg_method=wcs.disagg_method,
                                 disagg_ts=force_timestep)
            for year in years]


# def prms_forcings(wcs):
#     force_timestep = config['HYDROLOGY'][wcs.model]['force_timestep']
#     return PRMS_FORCINGS.format(disagg_ts=force_timestep, **wcs)


def get_downscaling_data(wcs):
    gcm = inverse_lookup(config['DOWNSCALING'][wcs.dsm]['gcms'], wcs.gcm)

    if wcs.dsm == 'bcsd' and wcs.scen == 'hist':
        scen = 'rcp45'
    elif wcs.dsm == 'loca' and wcs.scen == 'hist':
        scen = 'historical'
    else:
        scen = wcs.scen

    pattern = config['DOWNSCALING'][wcs.dsm]['data'].format(
        gcm=gcm, scen=scen, year=wcs.year)

    files = glob.glob(pattern)

    if not files:
        print('wcs: %s, pattern: %s' % (wcs, pattern))
        raise RuntimeError('Failed to find any files')

    return files


def metsim_state(wcs):
    year = int(wcs.year)
    scen = wcs.scen
    if year == 2006:
        scen = 'hist'
    if wcs.dsm == 'bcsd' and scen == 'hist':
        scen = 'rcp45'

    return DOWNSCALING_DATA.format(dsm=wcs.dsm, gcm=wcs.gcm, scen=scen,
                                   year=year - 1)


def hydro_executable(wcs):
    return config['HYDROLOGY'][wcs.model]['executable']


def hydro_template(wcs):
    return config['HYDROLOGY'][wcs.model]['template']


def start_dask(config):
    cluster = PBSCluster(queue=config['dask']['queue'],
                         walltime=config['dask']['worker_walltime'],
                         project=config['__default__']['account'],
                         resource_spec=config['dask']['select'],
                         interface=config['dask']['interface'],
                         threads=config['dask']['threads'],
                         processes=config['dask']['processes'])
    client = Client(cluster)
    client.write_scheduler_file(DASK_SCHEDULER)

    cluster.start_workers(config['dask']['initial_workers'])
    if config['dask'].get('adapt', None):
        cluster.adapt(**config['dask']['adapt'])

    return cluster, config


def try_to_close_client(client):
    try:
        client.close(1)
    except TimeoutError:
        pass


# Workflow bookends
onsuccess:
    print('Workflow finished, no error')

onerror:
    print('An error occurred')

onstart:
    print('starting now')


DASK_SCHEDULER = os.path.join(
    config['workdir'], config['dask']['scheduler_file'])
DASK_SCHEDULER = None

# Readme / documentation
CASEDIR = os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}')
README = os.path.join(CASEDIR, 'readme.md')

# Downscaling temporary filenames
DOWNSCALING_DATA = os.path.join(CASEDIR, 'downscaling_data',
                                'forcing_{gcm}.{scen}.{dsm}.{year}.nc')


CONFIGS_DIR = os.path.join(CASEDIR, 'configs')

# Metsim Filenames
DISAGG_DIR = os.path.join(CASEDIR, 'disagg_data')
DISAGG_CONFIG = os.path.join(
    CONFIGS_DIR, 'config.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.{year}.cfg')
METSIM_STATE = os.path.join(
    DISAGG_DIR, 'state.{gcm}.{scen}.{dsm}.{disagg_method}.{year}1231.nc')
DISAGG_PREFIX = 'force.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}'
DISAGG_OUTPUT = os.path.join(DISAGG_DIR,
                             DISAGG_PREFIX + '_{year}0101-{year}1231.nc')

DISAGG_OUTPUT_PRMS = os.path.join(
    DISAGG_DIR,
    'forcing.vic.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.nc')  # All in one file

# Hydrology
HYDRO_OUT_DIR = os.path.join(CASEDIR, 'hydro_data')
HYDRO_OUTPUT = os.path.join(
    HYDRO_OUT_DIR, 'hist.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.{outstep}.nc')

HYDRO_OUTPUT = os.path.join(
    HYDRO_OUT_DIR, 'hist.{model_id}.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.{outstep}.nc')
HYDRO_CONFIG = os.path.join(
    CONFIGS_DIR, 'config.{model_id}.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.txt')

# VIC Filenames
VIC_STATE = os.path.join(
    HYDRO_OUT_DIR, 'state.{gcm}.{scen}.{dsm}.{disagg_method}.{model}.{date}.nc')
VIC_FORCING = os.path.join(
    DISAGG_DIR,
    'forcing.vic.{gcm}.{scen}.{dsm}.{disagg_method}.{disagg_ts}.{year}.nc')  # No months/days

# PRMS Forcings
PRMS_FORCINGS = os.path.join(
    DISAGG_DIR, 'forcing.prms.{gcm}.{scen}.{dsm}.{disagg_method}.nc')  # No months/days

# Default Values
DEFAULT_WIND = 1.
TINY_TEMP = 1e-10

# Constants
KELVIN = 273.16
SEC_PER_DAY = 86400
# Rules
# -----------------------------------------------------------------------------
localrules: readme, setup_casedirs, config_metsim, config_vic, config_prms, hydrology_models, disagg_methods, rename_hydro_forcings_for_vic

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

# Hydrologic Models
rule hydrology_models:
    input:
        expand(HYDRO_OUTPUT, filtered_product,
               gcm=config['GCMS'], scen=config['SCENARIOS'],
               disagg_method=config['DISAGG_METHODS'],
               dsm=config['DOWNSCALING_METHODS'],
               model=list(config['HYDRO_METHODS'].keys()),
               model_id=set(config['HYDRO_METHODS'].values()),
               outstep=['daily'])  # , 'monthly'])


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



rule rename_hydro_forcings_for_vic:
    input: DISAGG_OUTPUT
    output: temp(VIC_FORCING)
    shell:
        "ln -s {input} {output}"


rule reformat_prms_forcings:
    input: prms_forcings_from_disagg
    output: PRMS_FORCINGS
    run:
        client = Client(n_workers=threads, threads_per_worker=threads)

        ds = xr.open_mfdataset(list(input), autoclose=True,
                               engine='netcdf4')
        ds.load().to_netcdf(output[0])

        try_to_close_client(client)


rule config_vic:
    input:
        hydro_forcings,
        template = hydro_template
    output:
        config = HYDRO_CONFIG.replace('{model_id}', 'vic')
    run:
        options = {}
        options['startyear'] = config['SCEN_YEARS'][wildcards.scen]['start']
        options['startmonth'] = '01'
        options['startday'] = '01'
        options['endyear'] = config['SCEN_YEARS'][wildcards.scen]['stop']
        options['endmonth'] = '12'
        options['endday'] = '31'
        options['calendar'] = 'standard'  # TODO: read from metsim output
        options['domain'] = config['domain']

        options['out_state'] = VIC_STATE.format(
            date='%s-12-31-82800' % options['startyear'], **wildcards)
        options['stateyear'] = options['endyear']
        options['statemonth'] = options['endmonth']
        options['stateday'] = options['endday']
        options['forcings'] = VIC_FORCING.format(
            year='', **wildcards)[:-3]

        options['parameters'] = config['HYDROLOGY'][wildcards.model]['parameters']
        options['result_dir'] = HYDRO_OUT_DIR.format(**wildcards)

        if wildcards.scen == 'hist':
            options['init_state'] = ''

        else:
            kwargs = dict(wildcards)
            kwargs['scen'] = 'hist'
            options['init_state'] = VIC_STATE.format(date='yyyy-12-31-82800', **kwargs)

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))


rule run_vic:
    input:
        hydro_forcings,
        vic_exe = hydro_executable,
        config = HYDRO_CONFIG.replace('{model_id}', 'vic')
    output:
        HYDRO_OUTPUT.replace('{model_id}', 'vic')
    shell:
        "mpiexec_mpt {input.vic_exe} -g {input.config}"


# forcings: /glade/p/ral/RHAP/naoki/storylines/prms/project/analog_regression_1.NCAR_WRF_50km.access13.hist/r1/input/analog_regression_1.NCAR_WRF_50km.access13.hist_1.data
rule config_prms:
    input:
        forcing = hydro_forcings,
        template = hydro_template
    output:
        config = HYDRO_CONFIG.replace('{model_id}', 'prms')
    run:
        options = {}

        options['out_prefix'] = HYDRO_OUT_DIR.format(**wildcards)
        options['param_file'] = config['HYDROLOGY'][wildcards.model]['parameters']
        options['forcing'] = input.forcing
        options['t_max'] = 't_max'
        options['t_min'] = 't_max'
        options['prec'] = 'prec'
        options['shortwave'] = options['shortwave']
        options['input_state'] = 'MISSING'
        options['output_state'] = 'MISSING'

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))

rule run_prms:
    input:
        PRMS_FORCINGS,
        config = HYDRO_CONFIG.replace('{model_id}', 'prms'),
        prms_exe = hydro_executable
    output:
        HYDRO_OUTPUT.replace('{model_id}', 'prms')
    shell:
        "touch {output}"


# rule run_fuse:
#     output:
#         HYDRO_OUTPUT
#     shell:
#         "touch {output}"
#
#
# rule run_summa:
#     output:
#         HYDRO_OUTPUT
#     shell:
#         "touch {output}"


rule process_downscaling:
    input:
        readme = README,
        files = get_downscaling_data,
    output:
        temp(DOWNSCALING_DATA)
    threads: 4
    run:

        def loca_preproc(ds):
            if 'latitude' in ds.coords:
                ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
            return ds

        def process_downscaling_dataset(input_files, output_file, kind, times,
                                        like=None, rename=None):

            if like:
                like = xr.open_dataset(like)

            if kind == 'loca':
                preproc = loca_preproc
            else:
                preproc = None

            ds = xr.open_mfdataset(input_files, autoclose=True,
                                   preprocess=preproc,
                                   engine='netcdf4', chunks={'time': 50})

            if rename:
                try:
                    ds = ds.rename(
                        {v: k for k, v in rename.items()})[list(rename)]
                except ValueError:
                    print(ds)
                    raise
            ds = ds.sel(time=times)

            # drop bound variables
            drops = []
            for v in ['lon_bnds', 'lat_bnds', 'time_bnds']:
                if v in ds or v in ds.coords:
                    drops.append(v)
            ds = ds.drop(drops)

            if like:
                ds = ds.reindex_like(like, method='nearest')

            if 'wind' not in ds:
                ds['wind'] = xr.full_like(ds['prec'], DEFAULT_WIND)
                ds['wind'].attrs['units'] = 'm s-1'
                ds['wind'].attrs['long_name'] = 'wind speed'

            if kind == 'loca':
                # normalize units
                ds['prec'] = ds['prec'] * SEC_PER_DAY
                ds['prec'].attrs['units'] = 'mm d-1'
                ds['prec'].attrs['long_name'] = 'precipitation'

                ds['t_max'] = ds['t_max'] - KELVIN
                ds['t_max'].attrs['units'] = 'C'

            # quality control checks
            ds['t_max'] = ds['t_max'].where(ds['t_max'] > ds['t_min'],
                                            ds['t_min'] + TINY_TEMP)
            ds['prec'] = ds['prec'].where(ds['prec'] < 0, 0.)
            ds['wind'] = ds['wind'].where(ds['wind'] < 0, 0.)

            if like and 'mask' in like:
                ds = ds.where(like['mask'])

            ds = ds.load()

            # TODO: save the original attributes and put them back

            # make sure time index in dataset cover the full period
            dates = pd.date_range(times.start, times.stop, freq='D')
            # fill leap day using linear interpolation
            ds = ds.resample(time='1D', keep_attrs=True).mean().interpolate_na(
                dim='time', limit=2)
            # fill in missing days at the end or begining of the record
            if ds.dims['time'] != len(dates):
                ds = ds.reindex(time=dates, method='ffill')

            # TODO, update time encoding to use common units for all files
            ds.to_netcdf(output_file, engine='h5netcdf',
                         unlimited_dims=['time'])

        log_to_readme('Processing downscaling data for %s' % str(wildcards),
                      input.readme)

        times = slice('%s-01-01' % wildcards.year, '%s-12-31' % wildcards.year)
        rename = config['DOWNSCALING'][wildcards.dsm]['variables']

        if DASK_SCHEDULER is not None:
            client = Client(scheduler_file=DASK_SCHEDULER)
            print(client, flush=True)
            future = client.submit(process_downscaling_dataset, input.files,
                                   output[0], wildcards.dsm, times,
                                   like=config['domain'],
                                   rename=rename).result()
        else:
            client = Client(n_workers=threads, threads_per_worker=threads)
            process_downscaling_dataset(
                input.files, output[0], wildcards.dsm, times,
                like=config['domain'], rename=rename)
            try_to_close_client(client)


rule run_metsim:
    input:
        readme = README,
        config = DISAGG_CONFIG,
        forcing = DOWNSCALING_DATA,
        state = metsim_state
    output: DISAGG_OUTPUT
    threads: 36
    # conda: "envs/metsim.yaml"
    shell: "ms -n {threads} {input.config}"

rule config_metsim:
    input:
        README,
        config['DISAGG']['metsim']['template'],
        forcing = DOWNSCALING_DATA,
        state = metsim_state
    output:
        config = DISAGG_CONFIG
    run:
        log_to_readme('Configuring MetSim for %s' % str(wildcards), input[0])

        data_dir = DISAGG_DIR.format(**wildcards)
        out_state = METSIM_STATE.format(**wildcards)

        prefix = DISAGG_PREFIX.format(**wildcards)

        time_step = int(wildcards.disagg_ts)
        out_vars = config['DISAGG']['metsim'][time_step]['out_vars']
        in_vars = config['DOWNSCALING'][wildcards.dsm]['variables']
        forcing = maybe_make_cfg_list(input.forcing)
        input_state = maybe_make_cfg_list(input.state)
        domain = config['domain']

        with open(input[1], 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(forcing=forcing,
                                    metsim_dir=data_dir,
                                    prefix=prefix,
                                    input_state=input_state,
                                    output_state=out_state,
                                    out_vars=out_vars,
                                    domain=domain,
                                    time_step=time_step,
                                    **wildcards, **in_vars))
