from __future__ import print_function
import time as tm
from getpass import getuser
import pandas as pd
import xarray as xr
import numpy as np

now = tm.ctime(tm.time())
user = getuser()

attrs = {'pr': {'units': 'in',
                'long_name': 'precipitation'},
         'tmin': {'units': 'F',
                  'long_name': 'minimum daily temperature'},
         'tmax': {'units': 'F',
                  'long_name': 'maximum daily temperature'},
         'sw': {'units': 'Langley d-1',
                'long_name': 'shortwave flux'}}

encoding = {'pr': {'_FillValue': 0.},
            'tmin': {'_FillValue': -99.},
            'tmax': {'_FillValue': -99.},
            'sw': {'_FillValue': 0.}}


def read_grid_file(filename):
    '''
    Read one column from text file
    '''
    df = pd.read_csv(filename, sep='\t',
                     header=None, names=['hru', 'lat', 'lon'])
    return df


def extract_nc(ncin, grid_df, ncout,
               varnames=['prec', 't_max', 't_min', 'shortwave']):
    '''
    Parameters
    ----------
    ncin : str
        input netCDF
    grid_df: Pandas.DataFrame
        list of selected grid hru, lat, lon
    ncout : str
        output subset netCDF
    varnames : list
        variables to subset
    '''
    print('opening %s' % ncin)
    ds = xr.open_mfdataset(ncin)
    print(ds)

    print('subseting and then loading')
    # subset the dataset now
    lats = xr.Variable('hru', grid_df['lat'])
    lons = xr.Variable('hru', grid_df['lon'])
    print(lats, lons, ds[varnames], flush=True)
    subset = ds[varnames].sel(lat=lats, lon=lons)

    subset.coords['hru'] = xr.Variable(
        'hru', np.arange(1, len(grid_df['hru']) + 1))
    subset['hru'].attrs = {'description': 'HRU ID'}

    print('unit conversion and masking')
    # unit coversions and some masking
    for vi, varname in enumerate(varnames):
        if varname == 'prec':
            # mm --> in
            subset['prec'] *= 0.0393701
            subset['prec'] = subset['prec'].where(subset['prec'] >= 0)
        elif varname in ['t_min', 't_max']:
            # C --> F
            subset[varname] = subset[varname] * 1.8 + 32.0
        elif varname == 'shortwave':
            # W m-2 --> Langley/day
            factor = 86400.0 / 41868.0
            subset['shortwave'] *= factor
            subset['shortwave'] = subset['shortwave'].where(
                subset['shortwave'] >= 0)
        else:
            raise ValueError('unknown varname: %s' % varname)

    # rename variables
    subset = subset.rename({'prec': 'pr',
                            't_min': 'tmin',
                            't_max': 'tmax',
                            'shortwave': 'sw'})
    # reorder dimension
    subset = subset.transpose('time', 'hru')
    # drop some variables
    subset = subset.drop(['lat', 'lon'])
    subset['pr'] = subset['pr'].astype(np.float32)
    subset['tmax'] = subset['tmax'].astype(np.float32)
    subset['tmin'] = subset['tmin'].astype(np.float32)
    subset['sw'] = subset['sw'].astype(np.float32)

    for varname in subset.data_vars:
        subset[varname].attrs = attrs[varname]
        subset[varname].encoding = encoding[varname]
        subset[varname].encoding['dtype'] = 'f4'

    # Write subset
    print('writing %s' % ncout)
    subset.attrs['history'] += '\nSubset for PRMS: {0} by {1}'.format(
        now, user)
    subset.to_netcdf(ncout, format='NETCDF4', unlimited_dims=['time'])
