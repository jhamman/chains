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


def try_to_close_client(client):
    try:
        client.close(1)
    except TimeoutError:
        pass


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


def loca_preproc(ds):
    if 'latitude' in ds.coords:
        ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    return ds


def process_downscaling_dataset(input_files, output_file, kind, times,
                                like=None, rename=None):

    if like:
        like = xr.open_dataset(like)

    if 'loca' in kind.lower():
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

    if 'loca' in kind.lower():
        # "normalize" units
        ds['prec'] = ds['prec'] * SEC_PER_DAY
        ds['prec'].attrs['units'] = 'mm d-1'
        ds['prec'].attrs['long_name'] = 'precipitation'

        for v in ['t_max', 't_min']:
            ds[v] = ds[v] - KELVIN
            ds[v].attrs['units'] = 'C'

    # quality control checks
    ds['t_max'] = ds['t_max'].where(ds['t_max'] > ds['t_min'],
                                    ds['t_min'] + TINY_TEMP)
    ds['prec'] = ds['prec'].where(ds['prec'] > 0, 0.)
    ds['wind'] = ds['wind'].where(ds['wind'] > 0, 0.)

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


def vic_init_state(wcs):
    if wcs.scen == 'hist':
        state = NULL_STATE
    else:
        kwargs = dict(wcs)
        kwargs['scen'] = 'hist'
        kwargs['date'] = '{:4d}-12-31-82800'.format(
            config['SCEN_YEARS'][wcs.scen]['start'])
        state = VIC_STATE.format(**kwargs)
    return state


def prms_init_state(wcs):
    if wcs.scen == 'hist':
        state = NULL_STATE
    else:
        kwargs = dict(wcs)
        kwargs['scen'] = 'hist'
        state = PRMS_STATE.format(**kwargs)
    return state


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
localrules: readme, setup_casedirs, config_metsim, config_vic, config_prms, hydrology_models, disagg_methods, rename_hydro_forcings_for_vic, config_hydro_models, disagg_configs, prms_data_file,

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
        "ln -sr {input} {output}"


rule reformat_prms_forcings:
    input:
        prms_forcings_from_disagg,
    output: PRMS_FORCINGS
    benchmark: BENCHMARK
    run:
        from prms import read_grid_file, extract_nc

        grid_file = config['HYDROLOGY']['prms_default']['grid_file']

        grid_df = read_grid_file(grid_file)

        extract_nc(input, grid_df, output[0])


rule config_vic:
    input:
        hydro_forcings,
        template = hydro_template,
        in_state = vic_init_state
    output:
        config = HYDRO_CONFIG.replace('{model_id}', 'vic')
    run:
        options = dict(config['HYDROLOGY'][wildcards.model])
        options['out_prefix'] = VIC_OUT_PREFIX.format(**wildcards)
        options['startyear'] = config['SCEN_YEARS'][wildcards.scen]['start']
        options['startmonth'] = '01'
        options['startday'] = '01'
        options['endyear'] = config['SCEN_YEARS'][wildcards.scen]['stop']
        options['endmonth'] = '12'
        options['endday'] = '31'
        options['calendar'] = 'standard'  # TODO: read from metsim output
        options['domain'] = config['domain']

        options['out_state'] = VIC_STATE_PREFIX.format(**wildcards)
        options['stateyear'] = options['endyear']
        options['statemonth'] = options['endmonth']
        options['stateday'] = options['endday']
        options['forcings'] = VIC_FORCING.format(
            year='',
            disagg_ts=config['HYDROLOGY'][wildcards.model]['force_timestep'],
            **wildcards)[:-3]

        options['result_dir'] = HYDRO_OUT_DIR.format(**wildcards)

        if wildcards.scen == 'hist':
            options['init_state'] = ''
        else:
            kwargs = dict(wildcards)
            kwargs['scen'] = 'hist'
            options['init_state'] = 'INIT_STATE ' + input.in_state.format(
                **kwargs)

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))

rule dummy_state:
    output: NULL_STATE
    shell: "touch {output}"


rule run_vic:
    input:
        hydro_forcings,
        vic_init_state,
        vic_exe = hydro_executable,
        config = HYDRO_CONFIG.replace('{model_id}', 'vic')
    output:
        HYDRO_OUTPUT.replace('{model_id}', 'vic').replace(
            '{outstep}', 'daily'),
        HYDRO_OUTPUT.replace('{model_id}', 'vic').replace(
            '{outstep}', 'monthly'),
        state = VIC_STATE
    benchmark: BENCHMARK
    log:
        NOW.strftime(HYDRO_LOG.replace('{model_id}', 'vic'))
    run:
        # run VIC
        # TODO: the -n 36 should go away at some point
        shell("module load intel impi netcdf && mpirun -n 36 {input.vic_exe} -g {input.config} > {log} 2>&1")

        # rename output files
        for freq, end in [('daily', '.daily.{:4d}-01-01.nc'),
                          ('monthly', '.monthly.{:4d}-01.nc')]:
            dirname = HYDRO_OUT_DIR.format(**wildcards)
            suffix = end.format(config['SCEN_YEARS'][wildcards.scen]['start'])
            infile = os.path.join(dirname, VIC_OUT_PREFIX.format(
                model_id='vic', **wildcards) + suffix)
            outfile = HYDRO_OUTPUT.format(model_id='vic', outstep=freq,
                                          **wildcards)
            # rename
            print('%s-->%s' % (infile, outfile), flush=True)
            shutil.move(infile, outfile)

        # rename output state (just dropping the date-string)
        try:
            infile = glob.glob(VIC_STATE_PREFIX.format(**wildcards) + '*.nc')[0]
        except IndexError:
            raise IndexError(VIC_STATE_PREFIX.format(**wildcards) + '*.nc',
                             'failed to return a valid state file')
        outfile = output.state
        print('%s-->%s' % (infile, outfile), flush=True)
        shutil.move(infile, outfile)


# forcings: /glade/p/ral/RHAP/naoki/storylines/prms/project/analog_regression_1.NCAR_WRF_50km.access13.hist/r1/input/analog_regression_1.NCAR_WRF_50km.access13.hist_1.data
rule config_prms:
    input:
        forcing = PRMS_FORCINGS,
        template = hydro_template
    output:
        config = HYDRO_CONFIG.replace('{model_id}', 'prms')
    run:
        options = {}

        options['startyear'] = config['SCEN_YEARS'][wildcards.scen]['start']
        options['endyear'] = config['SCEN_YEARS'][wildcards.scen]['stop']

        options['out_prefix'] = PRMS_OUTPUT.format(
            **wildcards).replace('.nc', '')  # prms adds the .nc suffix
        options['data_file'] = config['HYDROLOGY'][wildcards.model]['data_file']
        options['param_file'] = config['HYDROLOGY'][wildcards.model]['parameters']
        options['forcing'] = input.forcing
        options['write_state_file'] = 1
        options['output_state'] = PRMS_STATE.format(**wildcards)

        if wildcards.scen == 'hist':
            options['use_init_state_file'] = 0
            options['input_state'] = NULL_STATE
        else:
            options['use_init_state_file'] = 1
            state_kws = dict(wildcards)
            state_kws['scen'] = 'hist'
            options['input_state'] = PRMS_STATE.format(**state_kws)

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))


rule prms_data_file:
    output: 'templates/prms_data_file.txt'
    run:
        header = '''////////////////////////////////////////////////////////////
// runoff = cfs
////////////////////////////////////////////////////////////
runoff 1
########################################
'''
        dates = pd.date_range('1900-01-01', '2100-12-31')
        lines = [d.strftime('%Y %m %d 0 0 0 0') for d in dates]
        with open(output[0], 'w') as f:
            f.write(header)
            f.write('\n'.join(lines))

rule run_prms:
    input:
        PRMS_FORCINGS,
        prms_init_state,
        config = HYDRO_CONFIG.replace('{model_id}', 'prms'),
        prms_exe = hydro_executable
    output:
        PRMS_OUTPUT,
        PRMS_STATE
    benchmark: BENCHMARK
    log:
        NOW.strftime(HYDRO_LOG.replace('{model_id}', 'vic'))
    shell:
        "{input.prms_exe} {input.config} > {log} 2>&1"


rule post_process_prms:
    input:
        PRMS_OUTPUT
    output:
        daily = HYDRO_OUTPUT.replace('{model_id}', 'prms').replace(
            '{outstep}', 'daily'),
        monthly = HYDRO_OUTPUT.replace('{model_id}', 'prms').replace(
            '{outstep}', 'monthly')
    benchmark: BENCHMARK
    run:
        import xarray as xr
        from prms import read_grid_file, extract_nc

        grid_file = config['HYDROLOGY']['prms_default']['grid_file']

        grid_df = read_grid_file(grid_file).set_index(['lat', 'lon'])

        ds = xr.open_dataset(input[0])

        # invert the hru mapping
        ds.coords['hru_id'] = grid_df.index
        ds_grid = ds.unstack('hru_id')

        for v, da in ds_grid.data_vars.items():
            if da.attrs['units'] == 'in/d':
                da = da * MM_PER_IN
                da.attrs['units'] = 'mm d-1'
            elif da.attrs['units'] == 'in':
                da = da * MM_PER_IN
                da.attrs['units'] = 'mm'
            else:
                print('unknown unit conversion')
            ds_grid[v] = da

        # save daily file
        ds_grid.to_netcdf(output.daily, unlimited_dims=['time'])

        # save monthly file
        dsm = ds_grid.resample(time='MS').mean()
        dsm.to_netcdf(output.monthly, unlimited_dims=['time'])


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


rule downscaling:
    input:
        readme = README,
        files = get_downscaling_data,
    output:
        temp(DOWNSCALING_DATA)
    threads: 36
    log: NOW.strftime(DOWNSCALING_LOG)
    benchmark: BENCHMARK
    run:

        logging.basicConfig(filename=str(log), level=logging.DEBUG)
        logging.info('Processing downscaling data for %s' % str(wildcards))
        log_to_readme('Processing downscaling data for %s' % str(wildcards),
                      input.readme)

        times = slice('%s-01-01' % wildcards.year, '%s-12-31' % wildcards.year)
        rename = config['DOWNSCALING'][wildcards.dsm]['variables']

        # client = Client(n_workers=threads)
        # logging.debug(client)
        process_downscaling_dataset(
            input.files, output[0], wildcards.dsm, times,
            like=config['domain'], rename=rename)
        # try_to_close_client(client)


rule run_metsim:
    input:
        readme = README,
        config = DISAGG_CONFIG,
        forcing = DOWNSCALING_DATA,
        state = metsim_state
    output: DISAGG_OUTPUT
    log: NOW.strftime(DISAGG_LOG)
    threads: 36
    # conda: "envs/metsim.yaml"
    benchmark: BENCHMARK
    shell: "/glade/u/home/jhamman/anaconda/envs/storylines/bin/ms -n {threads} {input.config} > {log} 2>&1"

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
