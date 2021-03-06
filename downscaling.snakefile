import glob
import logging

import numpy as np
import pandas as pd
import xarray as xr

import dask

vars_to_drop = ['bounds_longitude', 'bounds_latitude',
                'latitude_bnds', 'longitude_bnds', 'nb2']

pre_rename = {'latitude': 'lat', 'longitude': 'lon'}


def inverse_lookup(d, key):
    for k, v in d.items():
        if v == key:
            return k
    raise KeyError(key)


def get_downscaling_data(wcs):
    if 'hist' in wcs.scen:
        # get one year before start of simulation for metsim
        start_offset = -1
    else:
        start_offset = 0
    years = get_year_range(config['SCEN_YEARS'][wcs.scen],
                           start_offset=start_offset)

    if wcs.dsm in config['OBS_FORCING']:
        # historical / obs data
        template = config['OBS_FORCING'][wcs.dsm]['data']
        scen = wcs.scen
    else:
        # downscaling_data
        gcm = inverse_lookup(config['DOWNSCALING'][wcs.dsm]['gcms'], wcs.gcm)
        template = config['DOWNSCALING'][wcs.dsm]['data']

        if wcs.dsm == 'bcsd' and wcs.scen == 'hist':
            scen = 'rcp45'
        elif wcs.dsm == 'loca' and wcs.scen == 'hist':
            scen = 'historical'
        else:
            scen = wcs.scen

    # get a pattern without wildcards (could still have glob patterns)
    patterns = set([template.format(gcm=gcm, scen=scen, dsm=wcs.dsm, year=year)
                    for year in years])

    # get actual filepaths
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    files.sort()

    # make sure all these files exist
    missing = []
    for file in files:
        if not os.path.isfile(file):
            missing.append(file)
    if missing:
        raise RuntimeError(
            'Failed to find any files for template %s. '
            'Missing: %s' % (pattern, '\n'.join(missing)))

    return files

def preproc(ds):
    for var in vars_to_drop:
        if var in ds:
            ds = ds.drop(var)
    for key, val in pre_rename.items():
        if key in ds:
            ds = ds.rename({key: val})

    lons = ds['lon'].values
    ds['lon'].values[lons > 180] -= 360.
    return ds.astype(np.float32)


def process_downscaling_dataset(input_files, output_file, kind, times,
                                like=None, rename=None, chunks=None):

    variables = ['prec', 'wind', 't_max', 't_min']
    if like:
        print('like %s' % like)
        like = xr.open_dataset(like, engine='netcdf4').load()

    ds = xr.open_mfdataset(sorted(input_files),
                           combine='by_coords',
                           concat_dim='time',
                           preprocess=preproc,
                           engine='netcdf4').load()

    print('renaming', flush=True)
    if rename:
        for key, val in rename.items():
            if val in ds:
                ds = ds.rename({val: key})

    ds = ds.sel(time=times)

    # drop bound variables
    drops = []
    for v in ['lon_bnds', 'lat_bnds', 'time_bnds']:
        if v in ds or v in ds.coords:
            drops.append(v)
    ds = ds.drop(drops)
    print('reindexing like %s' % like, flush=True)

    if like:
        ds = ds.reindex_like(like, method='nearest', tolerance=0.001)

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
    if 't_max' not in ds:
        print('calculating t_max')
        ds['t_max'] = ds['t_mean'] + 0.5 * ds['t_range']
    if 't_min' not in ds:
        print('calculating t_min')
        ds['t_min'] = ds['t_mean'] - 0.5 * ds['t_range']

    # quality control checks
    ds['t_max'] = ds['t_max'].where(ds['t_max'] > ds['t_min'],
                                    ds['t_min'] + TINY_TEMP)
    ds['prec'] = ds['prec'].where(ds['prec'] > 0, 0.)
    ds['wind'] = ds['wind'].where(ds['wind'] > 0, 0.)

    print('masking', flush=True)
    # if like and 'mask' in like:
    #     ds = ds.where(like['mask'])

    ds = ds[variables].astype(np.float32)

    print('loading %0.1fGB' % (ds.nbytes / 1e9), flush=True)
    print(ds, flush=True)
    ds = ds.transpose('time', 'lat', 'lon')  # .load()

    # TODO: save the original attributes and put them back

    # make sure time index in dataset cover the full period
    times = ds.indexes['time']
    start_year, stop_year = times.year[0], times.year[-1]
    dates = pd.date_range(start=f'{start_year}-01-01',
                          end=f'{stop_year}-12-31', freq='D')
    if ds.dims['time'] != len(dates):
        print('filling missing days', flush=True)
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

    print('writing %s:\n%s' % (output_file, ds), flush=True)
    # TODO, update time encoding to use common units for all files
    ds.to_netcdf(output_file, engine='netcdf4', encoding=encoding)


rule downscaling:
    input:
        readme = README,
        files = get_downscaling_data,
    output:
        DOWNSCALING_DATA
    log: NOW.strftime(DOWNSCALING_LOG)
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
        if wildcards.dsm in config['DOWNSCALING']:
            rename = config['DOWNSCALING'][wildcards.dsm]['variables']
        else:
            rename = config['OBS_FORCING'][wildcards.dsm]['variables']

        process_downscaling_dataset(
            input.files, output[0], wildcards.dsm, times,
            like=config['domain'], rename=rename,
            chunks=config['DISAGG_CHUNKS'])
