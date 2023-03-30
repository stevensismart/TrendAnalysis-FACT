import gzip
import xarray as xr
import numpy as np
from urllib.request import urlopen
import urllib
import os
import pandas as pd
import re
import rioxarray
from tqdm import tqdm
import tempfile
import geopandas
from shapely.geometry import mapping
from datetime import datetime, timedelta
import astral
from astral.sun import sun


def _shift_shapefile(_file: str = "/home/eohl/Documents/projects/TrendAnalysis/data/NERFC/NERFC.shp"):
    os.system(
        f'/home/eohl/anaconda3/envs/ncl_stable/bin/ogr2ogr {_file[:-4]}_shifted.shp {_file[:-4]}.shp -dialect sqlite -sql "SELECT ShiftCoords(geometry,360,0) FROM {_file.split("/")[-1][:-4]}"')


def read_shapefile():
    return geopandas.read_file(os.path.join(os.getcwd(), 'data', 'NERFC', "NERFC_shifted.shp"))


def set_empty_nc(day):
    print('\nCreating a fresh mrms day netcdf\n')
    shapefile = read_shapefile()
    URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate//PrecipRate_00.00_{str(day.year)}{str("{:02d}".format(day.month))}{str("{:02d}".format(day.day))}-000000.grib2.gz'
    compressed_file = urllib.request.urlopen(URL).read()
    with tempfile.NamedTemporaryFile(suffix=".grib2") as f:
        f.write(gzip.decompress(compressed_file))
        xx = xr.load_dataset(f.name, engine='cfgrib')
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
            _sunrise_mrms['unknown'].data[lat_idx, lon_idx] = str(
                sun(city.observer, date=pd.to_datetime(day_mrms.time.data.max()))['sunrise']- timedelta(hours=4))
            _sunset_mrms['unknown'].data[lat_idx, lon_idx] = str(
                sun(city.observer, date=pd.to_datetime(day_mrms.time.data.max()))['sunset']- timedelta(hours=4))
    _sunrise_mrms['unknown'].data = _sunrise_mrms['unknown'].data.astype('datetime64[s]')
    _sunset_mrms['unknown'].data = _sunset_mrms['unknown'].data.astype('datetime64[s]')
    return _sunrise_mrms, _sunset_mrms



def grabber(day):
    shapefile = read_shapefile()
    day_mrms, light_mrms = set_empty_nc(day)
    sunrise_mrms, sunset_mrms = create_sunrise(day_mrms)

    main_URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate/'
    response = str(urllib.request.urlopen(main_URL).read().decode('utf-8'))
    compressed_files = re.findall(r"PrecipRate.*?grib2.gz", response)

    for _file in tqdm(compressed_files, desc=f"processing mrms day {day}..."):
        URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate//{_file}'
        compressed_file = urllib.request.urlopen(URL).read()
        filetime = URL.split('//')[-1].split('_')[-1].split('.')[0]
        with tempfile.NamedTemporaryFile(suffix=".grib2") as f:
            f.write(gzip.decompress(compressed_file))
            xx = xr.load_dataset(f.name, engine='cfgrib')
            xx.rio.write_crs("epsg:4326", inplace=True)
            clipped = xx.rio.clip(shapefile.geometry.apply(mapping), shapefile.crs, drop=True)
            clipped['unknown'].data[np.isnan(clipped['unknown'].data)] = 0
            # Concatenate daily precipitation
            day_mrms['unknown'].data = np.add(np.nan_to_num(clipped['unknown'].data), day_mrms['unknown'].data)
            # Sunrise & Sunset
            currenttime = pd.to_datetime(filetime) - timedelta(hours=4)
            currenttime_sup_sunrise = currenttime >=sunrise_mrms['unknown'].data
            currenttime_inf_sunset = currenttime <= sunset_mrms['unknown'].data
            if  np.any(currenttime_sup_sunrise) and np.any(currenttime_inf_sunset):
                light_mrms['unknown'].data[np.logical_and(currenttime_sup_sunrise, currenttime_inf_sunset)] = np.add(np.nan_to_num(clipped['unknown'].data[np.logical_and(currenttime_sup_sunrise, currenttime_inf_sunset)]), light_mrms['unknown'].data[np.logical_and(currenttime_sup_sunrise, currenttime_inf_sunset)])

    day_mrms.to_netcdf(f'data/daily/{str(day)}.nc')
    light_mrms.to_netcdf(f'data/sunshine/{str(day)}.nc')