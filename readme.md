# Readme

Tools for calculating freezing and thawing degree days from monthly data. 

Utility for calculating the freezing and thawing degree-days and saving
them as tiffs. Uses as spline based method to find roots from monthly 
temperature data and calculate fdd and tdd by integration between roots.
if roots cannot be calculated a fallback method is used by fixing seasons 
for either summer and winter, and integrating positive or negative values 
to find tdd and fdd.

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
them as tiffs. Uses as spline based method to find roots from monthly 
temperature data and calculate fdd and tdd by integration between roots.
if roots cannot be calculated a fallback method is used by fixing seasons 
for either summer and winter, and integrating positive or negative values 
to find tdd and fdd.


Flags
-----
--in-temperature:  path
    path to directory containing monthly air temperature files. When sorted 
    (by python sorted function) these files should be in chronological 
    order.
--start-year: int
    the start year of data being processed
--out-directory: path
    if this flag is used a unified output directory will be created with 
    sub-directories for: fdd, tdd, roots, and logs. Other out- flags are 
    ignored. Either --out-directory or --out-tdd and --out-fdd, 
    are required.
--out-fdd: path 
    path to save freezing degree-day tiff files at. Ignored if  
    --out-directory is used. Either --out-directory or --out-tdd and 
    --out-fdd, are required.
--out-tdd: path
    path to save thawing degree-day tiff files at. Ignored if  
    --out-directory is used. Either --out-directory or --out-tdd and 
    --out-fdd, are required.
--logging-dir: path
    Optional directory to keep "logging files". Ignored if  --out-directory 
    is used
--out-roots: path
    Optional directory to save roots files. Ignored if  --out-directory is 
    used
--out-format: 'tiff', 'multigrid', or 'both'
    Optional, default tiff. output format 
--num-processes: int
    Optional, Default 1. Number of processes to use when calculating 
    degree-days. 
--mask-val: int 
    Optional, Default None. Nodata value in input tiff data.    
--mask-comp: 'eq','ne', 'lt', 'gt', 'lte', 'gte'
    Optional, Default 'eq'. Comparison for masking bad data 
    'eq' uses '==' ,'ne' uses '!=', 'lt' uses '<', 'gt'  uses '>', 
    'lte' uses '<=', 'gte' uses '>='
--recalc-mask-file: Path
    Optional, Defaults None, Path to npy file with a 2d array of 0s and 1s
    where values of 1 represent pixels to recalculate
--verbosity: "log", "warn", or not provided
    Optional, Defaults to not provided. 'log' for logging all messages, or 
    'warn' for only warn messages. If not provided most messages are not
    printed.
--sort-method: "default" or "snap" 
    Optional, Default "default". The method for sorting method used
    for loading tiff files, default uses pythons `sorted` function, 
    snap uses a function that sorts the files named via snaps month/
    year naming  convention (...01_1901.tif, ...01_1902.tif, ..., 
    ...12_2005.tif, ...12_2006.tif) to year/month order.
--start-at: int
    Optional, Default 0. index to star-at on resuming processing
--save-temp-monthly: bool
    Optional, Default False. If True save temporary monthly data state
--always-fallback: bool
    Optional, Default False. If True fallback method is always used.

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
    --num-processes=6 --verbose=log --sort-method=snap
```
