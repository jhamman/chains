import glob
import logging

import numpy as np
import pandas as pd
import xarray as xr

import dask

def inverse_lookup(d, key):
    for k, v in d.items():
        if v == key:
            return k
    raise KeyError(key)


def get_downscaling_data(wcs):
    dsm_type = config['DOWNSCALING_METHODS'][wcs.dsm]
    if dsm_type in config['OBS_FORCING']:
        # historical / obs data
        pattern = config['OBS_FORCING'][dsm_type]['data'] # .format(year=wcs.year)
    else:
        # downscaling_data
        gcm = inverse_lookup(config['DOWNSCALING'][dsm_type]['gcms'], wcs.gcm)

        if dsm_type == 'bcsd' and wcs.scen == 'hist':
            scen = 'rcp45'
        elif dsm_type == 'loca' and wcs.scen == 'hist':
            scen = 'historical'
        else:
            scen = wcs.scen

        pattern = config['DOWNSCALING'][dsm_type]['data'].format(
            gcm=gcm, scen=scen, dsm=wcs.dsm)
    print(pattern)
    files = sorted(glob.glob(pattern))

    if not files:
        raise RuntimeError('Failed to find any files for pattern %s' % pattern)

    return files


def cast_to_float(ds):
    return ds.astype(np.float32)

def loca_preproc(ds):
    if 'latitude' in ds.coords:
        ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    return cast_to_float(ds)


def process_downscaling_dataset(input_files, output_file, kind, times,
                                like=None, rename=None, chunks=None):

    variables = ['prec', 'wind', 't_max', 't_min']
    if like:
        like = xr.open_dataset(like, engine='netcdf4').load()

    if 'loca' in kind.lower() or 'maurer' in kind.lower():
        preproc = loca_preproc
    else:
        preproc = cast_to_float

    ds = xr.open_mfdataset(sorted(input_files),
                           preprocess=preproc,
                           engine='netcdf4').load()

    print('renaming', flush=True)
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
    print('reindexing', flush=True)
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

    print('masking', flush=True)
    if like and 'mask' in like:
        ds = ds.where(like['mask'])

    ds = ds[variables]

    print('loading %0.1fGB' % (ds.nbytes / 1e9), flush=True)
    print(ds, flush=True)
    ds = ds.load().transpose('time', 'lat', 'lon')

    # TODO: save the original attributes and put them back

    print('filling missing days', flush=True)
    # make sure time index in dataset cover the full period
    times = ds.indexes['time']
    start_year, stop_year = times.year[0], times.year[-1]
    dates = pd.date_range(start=f'{start_year}-01-01',
                          end=f'{stop_year}-12-31', freq='D')
    if ds.dims['time'] != len(dates):
        # fill in missing days at the end or begining of the record
        # removed: reindex(time=dates, method='ffill', copy=False)
        if isinstance(ds.indexes['time'], xr.CFTimeIndex):
            ds['time'] = ds.indexes['time'].to_datetimeindex()
        ds = ds.reindex(time=dates, method='nearest', copy=False)

    encoding = {}
    if chunks is not None:
        chunksizes = (len(ds.time), chunks['lat'], chunks['lon'])
        for var in variables:
            encoding[var] = dict(chunksizes=chunksizes)

    print('writing', flush=True)
    # TODO, update time encoding to use common units for all files
    ds.to_netcdf(output_file, engine='netcdf4', encoding=encoding)


rule downscaling:
    input:
        readme = README,
        files = get_downscaling_data,
    output:
        temp(DOWNSCALING_DATA)
    # threads: 36
    log: NOW.strftime(DOWNSCALING_LOG)
    # benchmark: BENCHMARK
    run:
        dask.config.set(scheduler='single-threaded')
        logging.basicConfig(filename=str(log), level=logging.DEBUG)
        logging.info('Processing downscaling data for %s' % str(wildcards))
        log_to_readme('Processing downscaling data for %s' % str(wildcards),
                      input.readme)

        start_year = config['SCEN_YEARS'][wildcards.scen]['start']
        stop_year = config['SCEN_YEARS'][wildcards.scen]['stop']
        if 'hist' in wildcards.scen:
            start_year = int(start_year) - 1
        times = slice('%s-01-01' % start_year, '%s-12-31' % stop_year)
        dsm_type = config['DOWNSCALING_METHODS'][wildcards.dsm]
        if dsm_type in config['DOWNSCALING']:
            rename = config['DOWNSCALING'][dsm_type]['variables']
        else:
            rename = config['OBS_FORCING'][dsm_type]['variables']

        process_downscaling_dataset(
            input.files, output[0], wildcards.dsm, times,
            like=config['domain'], rename=rename,
            chunks=config['DISAGG_CHUNKS'])
