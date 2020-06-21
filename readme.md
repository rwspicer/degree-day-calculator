# Readme

Tools for calculating freezing and thawing degree days from monthly data. 

The method can be used by calling the code from python as demonstrated in the 
accompanying jupyter notebook (examples.ipynb), or by using the utility 
`python ddc/utility.py` to see the utility help (also included at bottom of 
file)

This project is licensed under the MIT licence. This project includes a copy of 
multigrids, and code based on  `atm.tools.calc_degree_days.py` from the 
[atm project](https://github.com/ua-snap/arctic_thermokarst_model) which is 
licensed under the MIT licence. 

## Command Line Utility Help
```
Utility for calculating the freezing and thawing degree-days and saving
them as tiffs

Flags
-----
--in-temperature:  path
    path to directory containing monthly air temperature files. When sorted 
    (by python sorted function) these files should be in chronological 
    order.
--out-fdd: path
    path to save freezing degree-day tiff files at
--out-tdd: path
    path to save thawing degree-day tiff files at
--start-year: int
    the start year for the data
--num-processes: int
    Number of processes to use when calculating degree-days. Defaults 
    to one.
--mask-val: int
    No data value in input tiff data. Defaults to -9999.
--verbosity: string
    "log", "warn" for logging all messages, or only warn messages. If
    not used no messages are printed.
--sort-method
    "default" or "snap" to specify the method for sorting method used
    for loading tiff files, default uses pythons `sorted` function, 
    snap uses a function that sorts the files named via snaps month/
    year naming  convention (...01_1901.tif, ...01_1902.tif, ..., 
    ...12_2005.tif, ...12_2006.tif) to year/month order.

Examples
--------

Calculate freezing and thawing degree-days from monthly temperature and save 
results in ./fdd, and ./tdd

python ddc/utility.py 
    --in-temperature=../data/V1/temperature/monthly/SP/v1/tiff/ 
    --out-fdd=./fdd --out-tdd=./tdd --start-year=1901 --mask-val=-9999 
    --num-processes=6 --verbose=log

Calculate freezing and thawing degree-days from monthly temperature from snap
tas_mean_C_AK_CAN_AR5_5modelAvg_rcp45_01_2006-12_2100 data and save results
in ./fdd, and ./tdd

python ddc/utility.py 
    --in-temperature=../tas_mean_C_AK_CAN_AR5_5modelAvg_rcp45_01_2006-12_2100
    --out-fdd=./fdd --out-tdd=./tdd --start-year=2006 --mask-val=-9999 
    --num-processes=6 --verbose=log --sort_method=snap
```
