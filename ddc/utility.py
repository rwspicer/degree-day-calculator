"""
Degree-Day Calculator
---------------------

CLI utility for calculating degree-days from monthly data
"""
import glob
import os
from datetime import datetime

import numpy as np

from calc_degree_days import calc_gird_degree_days, create_day_array
from multigrids.tools import load_and_create, get_raster_metadata
from multigrids import TemporalGrid
import CLILib

from multiprocessing import Manager, Lock

def utility ():
    """Utility for calculating the freezing and thawing degree-days and saving
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
    
    Example
    -------

    Calculate freezing and thawing degree-days from monthly temperature

    python ddc/utility.py 
        --in-temperature=../data/V1/temperature/monthly/SP/v1/tiff/ 
        --out-fdd=./fdd --out-tdd=./tdd --start-year=1901 --mask-val=-9999 
        --num-processes=6 --verbose=log

    """
 
   
    try:
        arguments = CLILib.CLI([
            '--in-temperature',
            '--out-fdd',
            '--out-tdd',
            '--start-year'
            
            ],
            ['--num-processes', '--mask-val', '--verbose']
        
        )
    except (CLILib.CLILibHelpRequestedError, CLILib.CLILibMandatoryError) as E:
        print (E)
        print(utility.__doc__)
        return

    if  arguments['--verbose'] == 'log':
        verbosity = 2
    elif arguments['--verbose'] == 'warn':
        verbosity = 1
    else:
        verbosity = 0

    if verbosity >= 2:
        print('Seting up input...')
    

    start_year = int(arguments['--start-year'])
    months = [
        '01','02','03','04','05','06',
        '07','08','09','10','11','12',
    ]

    num_processes = int(arguments['--num-processes']) \
        if arguments['--num-processes']  else 1
    num_years = len(
            glob.glob(os.path.join(arguments['--in-temperature'],'*.tif')) 
        ) 
    num_years = num_years // 12

    years = [start_year + yr for yr in range(num_years)]


    temporal_grid_keys = [] 
    for yr in years: 
        for mn in months: 
            temporal_grid_keys.append(str(yr) + '-' + str(mn)) 

    load_params = {
            "method": "tiff",
            "directory": arguments['--in-temperature'],
        }
    create_params = {
        "name": "monthly temperatures",
        # "start_timestep": int(arguments['--start-year']),
        "grid_names": temporal_grid_keys
        
    }
    monthly_temps = load_and_create(load_params, create_params)
    
    ex_raster = glob.glob(
            os.path.join(arguments['--in-temperature'],'*.tif')
        )[0]
    raster_metadata = get_raster_metadata(ex_raster)
    monthly_temps.config['raster_metadata'] = raster_metadata

    mask_val = -9999
    if arguments['--mask-val'] is None:
        mask_val = int(arguments['--mask-val'])


    idx = monthly_temps.grids == mask_val
    monthly_temps.grids[idx] = np.nan


    grid_shape = monthly_temps.config['grid_shape']
    # out_shape = (num_years, grid_shape[0], grid_shape[1])
    fdd = TemporalGrid(
        grid_shape[0], grid_shape[1], num_years, 
        start_timestep=start_year,
        dataset_name = 'fdd',
        mode='w+'
    )
    tdd = TemporalGrid(
        grid_shape[0], grid_shape[1], num_years, 
        start_timestep=start_year,
        dataset_name = 'tdd',
        mode='w+'
    )

    
    days = create_day_array( 
        [datetime.strptime(d, '%Y-%m') for d in temporal_grid_keys] 
    )
    shape = monthly_temps.config['memory_shape']

    manager = Manager()



    log = manager.dict() 
   
    log.update(
        {'Element Messages': manager.list() , 'Spline Errors': manager.list()}
    )
    log['verbose'] = verbosity

    print(monthly_temps.grids.shape, fdd.grids.shape)
    calc_grid_degree_days(
            days, 
            monthly_temps.grids, 
            tdd.grids, 
            fdd.grids, 
            grid_shape, 
            num_process = num_processes,
            log=log
        )


    for item in log["Spline Errors"]:
        words = item.split(' ')
        location = int(words[-1])
        print ( grid_shape)
        row, col = np.unravel_index(location, grid_shape)  

        msg = ' '.join(words[:-1])
        msg += ' at row:' + str(row) + ', col:' + str(col) + '.'
        print(msg)

    
    fdd.config['raster_metadata'] = raster_metadata
    fdd.config['dataset_name'] = 'freezing degree-day'
    fdd.save_all_as_geotiff(arguments['--out-fdd'])

    tdd.config['raster_metadata'] = raster_metadata
    tdd.config['dataset_name'] = 'thawing degree-day'
    tdd.save_all_as_geotiff(arguments['--out-tdd'])


## fix this

# calculating degree days for element 53670. ~57.12% complete.
# calculating degree days for element 53671. ~57.12% complete.
# calculating degree days for element 53672. ~57.12% complete.
# calculating degree days for element 53673. ~57.12% complete.
# calculating degree days for element 53674. ~57.12% complete.
# calculating degree days for element 53675. ~57.13% complete.
# calculating degree days for element 53676. ~57.13% complete.
# calculating degree days for element 53677. ~57.13% complete.
# /Users/rwspicer/miniconda3/envs/ddc/lib/python3.7/site-packages/scipy/interpolate/fitpack2.py:232: UserWarning: 
# The maximal number of iterations maxit (set to 20 by the program)
# allowed for finding a smoothing spline with fp=s has been reached: s
# too small.
# There is an approximation returned but the corresponding weighted sum
# of squared residuals does not satisfy the condition abs(fp-s)/s < tol.
#   warnings.warn(message)


    
utility()


