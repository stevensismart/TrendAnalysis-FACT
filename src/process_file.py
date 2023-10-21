# import dask.array as da
import gc
import gzip
import os
import sys
import tempfile
import urllib
from datetime import timedelta
from urllib.request import urlopen

import geopandas
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import mapping
import warnings
warnings.filterwarnings("ignore")

def read_shapefile():
    return geopandas.read_file(os.path.join(os.getcwd(), 'data', 'NERFC', "NERFC_shifted.shp"))


if __name__ == '__main__':
    _file = sys.argv[1]
    os.chdir('/home/eohl/Documents/projects/TrendAnalysis')
    day = pd.to_datetime(sys.argv[2]).date()
    shapefile = read_shapefile()
    sunrise_mrms = xr.open_dataset(f'data/sunshine/sunrise_{str(day)}.nc')
    sunset_mrms = xr.open_dataset(f'data/sunshine/sunset_{str(day)}.nc')

    URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate//{_file}'
    compressed_file = urllib.request.urlopen(URL).read()
    filetime = URL.split('//')[-1].split('_')[-1].split('.')[0]
    with tempfile.NamedTemporaryFile(suffix=".grib2") as f:
        f.write(gzip.decompress(compressed_file))
        xx = xr.load_dataset(f.name, engine='cfgrib')
        xx.rio.write_crs("epsg:4326", inplace=True)
        clipped = xx.rio.clip(shapefile.geometry.apply(mapping), shapefile.crs, drop=True)
        clipped['unknown'].data[np.isnan(clipped['unknown'].data)] = 0
        # negative values gets 0
        clipped['unknown'].data[clipped['unknown'].data < 0] = 0
        # load day_mrms
        day_mrms = xr.open_dataset(f'data/mrms/{str(day)}.nc')
        xr.backends.file_manager.FILE_CACHE.clear()
        # Concatenate daily precipitation
        day_mrms['unknown'].data = np.add(np.nan_to_num(clipped['unknown'].data), day_mrms['unknown'].data)
        # save day to netcdf
        day_mrms.to_netcdf(f'data/mrms/_{str(day)}.nc')

        currenttime = pd.to_datetime(filetime) - timedelta(hours=4)
        currenttime_sup_sunrise = currenttime >= sunrise_mrms['unknown'].data
        currenttime_inf_sunset = currenttime <= sunset_mrms['unknown'].data
        # load light_mrms
        light_mrms = xr.open_dataset(f'data/sunshine/{str(day)}.nc')
        if np.any(currenttime_sup_sunrise) and np.any(currenttime_inf_sunset):
            light_mrms['unknown'].data[np.logical_and(currenttime_sup_sunrise, currenttime_inf_sunset)] = np.add(
                np.nan_to_num(clipped['unknown'].data[np.logical_and(currenttime_sup_sunrise, currenttime_inf_sunset)]),
                light_mrms['unknown'].data[np.logical_and(currenttime_sup_sunrise, currenttime_inf_sunset)])
        # save step to netcdf
        xr.backends.file_manager.FILE_CACHE.clear()
        light_mrms.to_netcdf(f'data/sunshine/_{str(day)}.nc')
        # clean garbage
        del xx, clipped, currenttime_sup_sunrise, currenttime_inf_sunset, light_mrms, day_mrms
        # clean memory
        gc.collect()
