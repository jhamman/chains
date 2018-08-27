import pandas as pd
import xarray as xr

import copy

localrules: prms_data_file, config_prms


def prms_init_state(wcs):
    # print('init state %s' % wcs)
    if wcs.scen in ['hist', 'obs_hist']:
        state = NULL_STATE
    else:
        kwargs = dict(wcs)
        kwargs['scen'] = 'hist'
        # print('init state 2 --> %s' % wcs)
        state = PRMS_STATE.format(**kwargs)
    return state


def prms_forcings_from_disagg(wcs):
    # print('prms forcing wildcards', wcs)
    years = get_year_range(config['SCEN_YEARS'][wcs.scen])
    # print(years)
    force_timestep = '1440'
    return [DISAGG_OUTPUT.format(year=year, gcm=wcs.gcm, scen=wcs.scen,
                                 dsm=wcs.dsm,
                                 disagg_method=wcs.disagg_method,
                                 disagg_ts=force_timestep)
            for year in years]


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


rule reformat_prms_forcings:
    input:
        prms_forcings_from_disagg,
    output: PRMS_FORCINGS
    # benchmark: BENCHMARK
    run:
        from tools.prms import read_grid_file, extract_nc

        grid_file = config['HYDROLOGY']['prms_default']['grid_file']

        grid_df = read_grid_file(grid_file)

        extract_nc(input, grid_df, output[0])


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

        if wildcards.scen in ['hist', 'obs_hist']:
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


rule run_prms:
    input:
        PRMS_FORCINGS,
        prms_init_state,
        config = HYDRO_CONFIG.replace('{model_id}', 'prms'),
        prms_exe = hydro_executable
    output:
        PRMS_OUTPUT,
        PRMS_STATE
    # benchmark: BENCHMARK
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
    # benchmark: BENCHMARK
    run:
        import xarray as xr
        from tools.prms import read_grid_file, extract_nc

        grid_file = config['HYDROLOGY']['prms_default']['grid_file']

        grid_df = read_grid_file(grid_file).set_index(['lat', 'lon'])

        ds = xr.open_dataset(input[0], decode_cf=False)
        if 'missing_value' in ds['time'].attrs:
            del ds['time'].attrs['missing_value']
        ds = xr.decode_cf(ds)

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
        print(ds_grid, output.daily, flush=True)
        ds_grid.load().to_netcdf(output.daily, engine='h5netcdf',
                                 unlimited_dims=['time'])

        # save monthly file
        dsm = ds_grid.resample(time='MS').mean()
        dsm.to_netcdf(output.monthly, unlimited_dims=['time'])
