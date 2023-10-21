import sys
import pandas as pd
import re
import urllib.request
import os
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    os.chdir('/home/eohl/Documents/projects/TrendAnalysis')
    day = pd.to_datetime(sys.argv[1]).date()
    main_URL = f'https://mtarchive.geol.iastate.edu/{str(day.year)}/{str("{:02d}".format(day.month))}/{str("{:02d}".format(day.day))}/mrms/ncep/PrecipRate/'
    response = str(urllib.request.urlopen(main_URL).read().decode('utf-8'))
    compressed_files = re.findall(r"PrecipRate.*?grib2.gz", response)
    # drop duplicates
    compressed_files = list(dict.fromkeys(compressed_files))
    # save list to a txt file, each member in a new line
    with open(f'data/mrms/list_of_files_{str(day)}.txt', 'w') as f:
        for item in compressed_files:
            f.write("%s\n" % item)


