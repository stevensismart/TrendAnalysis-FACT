#!/bin/bash

# create sunrise and sunset files
/home/eohl/anaconda3/envs/TrendAnalysis/bin/python -W ignore /home/eohl/Documents/projects/TrendAnalysis/src/sunrise_sunset.py $1 True

# generate list of files to be processed
/home/eohl/anaconda3/envs/TrendAnalysis/bin/python -W ignore /home/eohl/Documents/projects/TrendAnalysis/src/list_of_files.py $1

# Initialize an empty array
my_list=()

# Read the file line by line and append each line to the array
while IFS= read -r line; do
  my_list+=("$line")
done < /home/eohl/Documents/projects/TrendAnalysis/data/mrms/list_of_files_$1.txt

# Print the array elements (values)
for item in "${my_list[@]}"; do
  /home/eohl/anaconda3/envs/TrendAnalysis/bin/python -W ignore /home/eohl/Documents/projects/TrendAnalysis/src/process_file.py $item $1
  rm /home/eohl/Documents/projects/TrendAnalysis/data/mrms/$1.nc
  mv /home/eohl/Documents/projects/TrendAnalysis/data/mrms/_$1.nc /home/eohl/Documents/projects/TrendAnalysis/data/mrms/$1.nc

  rm /home/eohl/Documents/projects/TrendAnalysis/data/sunshine/$1.nc
  mv /home/eohl/Documents/projects/TrendAnalysis/data/sunshine/_$1.nc /home/eohl/Documents/projects/TrendAnalysis/data/sunshine/$1.nc
  echo $item
done