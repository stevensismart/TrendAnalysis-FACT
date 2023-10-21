import xarray as xr
from datetime import datetime
from pytz import timezone
from astral.sun import sun
import numpy as np
from tqdm import tqdm
import urllib
from urllib.request import urlopen
import tempfile
import re
import pandas as pd


# Define the latitude and longitude of the United States boundaries
min_lon, max_lon = -125, -66
min_lat, max_lat = 24, 50

# Define the timezone for sunrise and sunset calculations
us_timezone = timezone('US/Eastern')

# Load and accumulate the rain intensities
day_matrix = None
night_matrix = None
datetime.date(pd.to_datetime('2014/11/01 00:00'))

main_URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate/'
response = str(urllib.request.urlopen(main_URL).read().decode('utf-8'))
compressed_files = re.findall(r"PrecipRate.*?grib2.gz", response)

for _file in tqdm(compressed_files, desc=f"processing mrms day {day}..."):
    URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate//{_file}'
    compressed_file = urllib.request.urlopen(URL).read()
    filetime = URL.split('//')[-1].split('_')[-1].split('.')[0]
    with tempfile.NamedTemporaryFile(suffix=".grib2") as f:
        f.write(gzip.decompress(compressed_file))
        dataset = xr.load_dataset(f.name, engine='cfgrib')
        dataset.rio.write_crs("epsg:4326", inplace=True)

        # Extract the rain intensity variable (adjust the variable name if needed)
        rain_intensity = dataset['unknown']


        # Get the timestamp for the current time step
        timestamp = rain_intensity.time['time'].values.astype('datetime64[s]')

        timestamp_local = timestamp.astype(datetime)

        # Calculate the sunrise and sunset times for the current date and location
        s = sun(date=timestamp_local.date(), lat=max_lat, lon=max_lon)
        sunrise = s['sunrise'].astimezone(us_timezone)
        sunset = s['sunset'].astimezone(us_timezone)

        # Check if the current time is during the day or night
        if sunrise <= timestamp_local <= sunset:
            # Daytime accumulation
            if day_matrix is None:
                day_matrix = np.zeros_like(rain_intensity[t].values)
            day_matrix += rain_intensity[t].values
        else:
            # Nighttime accumulation
            if night_matrix is None:
                night_matrix = np.zeros_like(rain_intensity[t].values)
            night_matrix += rain_intensity[t].values

    # Close the NetCDF file
    dataset.close()