# import dask.array as da
import gc
import gzip
import os
import sys
import tempfile
import urllib
from datetime import timedelta
from urllib.request import urlopen

import astral
import geopandas
import numpy as np
import pandas as pd
import xarray as xr
from astral.sun import sun
from shapely.geometry import mapping
from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")

def read_shapefile():
    return geopandas.read_file(os.path.join(os.getcwd(), 'data', 'NERFC', "NERFC_shifted.shp"))


def set_empty_nc(day):
    print('\nCreating a fresh mrms day netcdf\n')
    shapefile = read_shapefile()
    URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate//PrecipRate_00.00_{str(day.year)}{str("{:02d}".format(day.month))}{str("{:02d}".format(day.day))}-000000.grib2.gz'
    compressed_file = urllib.request.urlopen(URL).read()
    with tempfile.NamedTemporaryFile(suffix=".grib2") as f:
        f.write(gzip.decompress(compressed_file))
        xx = xr.load_dataset(f.name, engine='cfgrib', decode_coords="all")
        xx.rio.write_crs("epsg:4326", inplace=True)
        clipped = xx.rio.clip(shapefile.geometry.apply(mapping), shapefile.crs, drop=True)
        clipped['unknown'].data[~np.isnan(clipped['unknown'].data)] = 0
        clipped['unknown'].data = np.nan_to_num(clipped['unknown'].data)
        return clipped, clipped


def create_sunrise(day_mrms):
    longitude = day_mrms['longitude'].data
    latitude = day_mrms['latitude'].data
    _sunrise_mrms, _sunset_mrms = day_mrms.copy(), day_mrms.copy()
    _sunrise_mrms['unknown'].data = _sunrise_mrms['unknown'].data.astype('str')
    _sunset_mrms['unknown'].data = _sunset_mrms['unknown'].data.astype('str')
    for lat_idx, lat in tqdm(enumerate(latitude), total=len(latitude)):
        for lon_idx, lon in enumerate(longitude):
            city = astral.LocationInfo('name', 'region', 'UTC', lat, lon - 360)
            try:
                _sunrise_mrms['unknown'].data[lat_idx, lon_idx] = str(
                sun(city.observer, date=pd.to_datetime(day_mrms.time.data.max()))['sunrise'] - timedelta(hours=4))
                _sunset_mrms['unknown'].data[lat_idx, lon_idx] = str(
                sun(city.observer, date=pd.to_datetime(day_mrms.time.data.max()))['sunset'] - timedelta(hours=4))
            except ValueError: # sunrise gets todays date, sunset gets a date in 2000
                _sunrise_mrms['unknown'].data[lat_idx, lon_idx] = '2030-01-01 00:00:00'
                _sunset_mrms['unknown'].data[lat_idx, lon_idx] = '2000-01-01 00:00:00'


    _sunrise_mrms['unknown'].data = _sunrise_mrms['unknown'].data.astype('datetime64[s]')
    _sunset_mrms['unknown'].data = _sunset_mrms['unknown'].data.astype('datetime64[s]')
    return _sunrise_mrms, _sunset_mrms


if __name__ == "__main__":
    # day from arg
    os.chdir('/home/eohl/Documents/projects/TrendAnalysis')
    day = pd.to_datetime(sys.argv[1]).date()
    clean = sys.argv[2]
    clean = True if clean == 'True' else False

    shapefile = read_shapefile()
    day_mrms, light_mrms = set_empty_nc(day)
    # save to netcdf
    day_mrms.to_netcdf(f'data/mrms/{str(day)}.nc')
    light_mrms.to_netcdf(f'data/sunshine/{str(day)}.nc')
    # if clean is true, delete the light data
    if clean:
        if os.path.exists(f'data/sunshine/sunrise_{str(day)}.nc'):
            os.remove(f'data/sunshine/sunrise_{str(day)}.nc')
        if os.path.exists(f'data/sunshine/sunset_{str(day)}.nc'):
            os.remove(f'data/sunshine/sunset_{str(day)}.nc')
    # if existing, load light data
    # if os.path.exists(f'data/sunshine/{str(day)}.nc'):
    #     light_mrms = xr.load_dataset(f'data/sunshine/{str(day)}.nc')

    sunrise_mrms, sunset_mrms = create_sunrise(day_mrms)
    # save to netcdf in sunshine folder
    sunrise_mrms.unknown.attrs.pop('units')
    sunset_mrms.unknown.attrs.pop('units')
    sunrise_mrms.to_netcdf(f'data/sunshine/sunrise_{str(day)}.nc')
    sunset_mrms.to_netcdf(f'data/sunshine/sunset_{str(day)}.nc')
    print(f'\nFinished {str(day)}\n')
    gc.collect()
    sys.exit(0)
