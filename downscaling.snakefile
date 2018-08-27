import glob
import logging

import numpy as np
import pandas as pd
import xarray as xr


def inverse_lookup(d, key):
    for k, v in d.items():
        if v == key:
            return k
    raise KeyError(key)


def get_downscaling_data(wcs):
    if wcs.dsm in config['OBS_FORCING']:
        # historical / obs data
        pattern = config['OBS_FORCING'][wcs.dsm]['data'].format(year=wcs.year)
    else:
        # downscaling_data
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
        raise RuntimeError('Failed to find any files for pattern %s' % pattern)

    return files


def loca_preproc(ds):
    if 'latitude' in ds.coords:
        ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    return ds


def process_downscaling_dataset(input_files, output_file, kind, times,
                                like=None, rename=None):

    if like:
        like = xr.open_dataset(like)

    if 'loca' in kind.lower() or 'maurer' in kind.lower():
        preproc = loca_preproc
    else:
        preproc = None

    ds = xr.open_mfdataset(sorted(input_files), autoclose=True,
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

    ds = ds.astype(np.float32).load()

    # TODO: save the original attributes and put them back

    # make sure time index in dataset cover the full period
    dates = pd.date_range(times.start, times.stop, freq='D')
    # fill leap day using linear interpolation
    ds = ds.resample(time='1D', keep_attrs=True).mean().interpolate_na(
        dim='time', limit=2)
    # fill in missing days at the end or begining of the record
    if ds.dims['time'] != len(dates):
        ds = ds.reindex(time=dates, method='ffill', copy=False).reindex(
            time=dates, method='nearest', copy=False)

    # TODO, update time encoding to use common units for all files
    ds.to_netcdf(output_file, engine='h5netcdf',
                 unlimited_dims=['time'])


rule downscaling:
    input:
        readme = README,
        files = get_downscaling_data,
    output:
        temp(DOWNSCALING_DATA)
    threads: 36
    log: NOW.strftime(DOWNSCALING_LOG)
    # benchmark: BENCHMARK
    run:

        logging.basicConfig(filename=str(log), level=logging.DEBUG)
        logging.info('Processing downscaling data for %s' % str(wildcards))
        log_to_readme('Processing downscaling data for %s' % str(wildcards),
                      input.readme)

        times = slice('%s-01-01' % wildcards.year, '%s-12-31' % wildcards.year)
        if wildcards.dsm in config['DOWNSCALING']:
            rename = config['DOWNSCALING'][wildcards.dsm]['variables']
        else:
            rename = config['OBS_FORCING'][wildcards.dsm]['variables']

        process_downscaling_dataset(
            input.files, output[0], wildcards.dsm, times,
            like=config['domain'], rename=rename)
