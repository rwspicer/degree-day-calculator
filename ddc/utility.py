"""
Degree-Day Calculator
---------------------

CLI utility for calculating degree-days from monthly data
"""
import glob
import os, sys
from datetime import datetime

import numpy as np

from calc_degree_days import calc_grid_degree_days, create_day_array
from multigrids.tools import load_and_create, get_raster_metadata
from multigrids import TemporalGrid
import CLILib

from multiprocessing import Manager, Lock

from sort import sort_snap_files

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
    --sort-method
        "default" or "snap" to specify the method for sorting method used
        for loading tiff files, default uses pythons `sorted` function, 
        snap uses a function that sorts the files named via snaps month/
        year naming  convention (...01_1901.tif, ...01_1902.tif, ..., 
        ...12_2005.tif, ...12_2006.tif) to year/month order.
    --logging-dir
        Optional directory to keep "logging files"
    --out-roots
        Optional directory to save roots files
    --out-format:
        output format 'tiff' or 'multigrid'

    
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

    """
 
    
    try:
        arguments = CLILib.CLI([
            '--in-temperature',
            '--out-fdd',
            '--out-tdd',
            '--start-year'
            
            
            ],
            [
                '--num-processes', 
                '--mask-val', 
                '--verbose',
                '--temp-dir',
                '--sort-method',
                '--logging-dir',
                '--out-roots',
                '--out-format',
                '--temp-data',
                '--start-at'
            ]
        
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

    sort_method = "Using default sort function"
    sort_fn = sorted

    if   not arguments['--sort-method'] is None and \
        arguments['--sort-method'].lower() == 'snap':
        sort_method = "Using SNAP sort function"
        sort_fn = sort_snap_files
    elif not arguments['--sort-method'] is None and\
        arguments['--sort-method'].lower() != "default":
        print("invalid --sort-method option")
        print("run utility.py --help to see valid options")
        print("exiting")
        return

    os.chdir(os.path.split(arguments['--temp-data'])[0])
    if verbosity >= 2:
       
        print('Seting up input...')
        print('\t', sort_method)
    
    try: 
        os.makedirs(arguments['--out-fdd'])
    except:
        pass
    try: 
        os.makedirs(arguments['--out-tdd'])
    except:
        pass

    if arguments['--out-roots']:
        try: 
            os.makedirs(arguments['--out-roots'])
        except:
            pass
    

    start_year = int(arguments['--start-year'])
    # months = [
    #     '01','02','03','04','05','06',
    #     '07','08','09','10','11','12',
    # ]

    num_processes = int(arguments['--num-processes']) \
        if arguments['--num-processes']  else 1
    
    ## in temps are a multigrid
    if os.path.isfile(arguments['--in-temperature']):
        print('in file', arguments['--in-temperature'])
        monthly_temps = TemporalGrid(arguments['--in-temperature'])
        print(monthly_temps)
        num_years = monthly_temps.config['num_timesteps'] // 12
        raster_metadata  = monthly_temps.config['raster_metadata'] 
    else: ## in temps are directory
        num_years = len(
            glob.glob(os.path.join(arguments['--in-temperature'],'*.tif')) 
        ) 
        num_years = num_years // 12

        years = [start_year + yr for yr in range(num_years)]

        temporal_grid_keys = [] 
        for yr in years: 
            for mn in range(1,13):
                temporal_grid_keys.append('%d-%02d' % (yr,mn) ) 

        load_params = {
                "method": "tiff",
                "directory": arguments['--in-temperature'],
                "sort_func": sort_fn,
                "verbose": True if verbosity >= 2 else False
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

        mask_val = -3.39999999999999996e+38 ## TODO fix mask val
    #    if arguments['--mask-val'] is None:
    #        mask_val = int(arguments['--mask-val'])

        idx = monthly_temps.grids < -1000
        monthly_temps.grids[idx] = np.nan


    grid_shape = monthly_temps.config['grid_shape']
    print('a', grid_shape[0], grid_shape[1], num_years)
    # out_shape = (num_years, grid_shape[0], grid_shape[1])
    if  os.path.isfile(os.path.join(arguments['--out-fdd'], 'fdd.yml')):
        fdd = TemporalGrid(os.path.join(arguments['--out-fdd'], 'fdd.yml'))
    else:
        fdd = TemporalGrid(
            grid_shape[0], grid_shape[1], num_years, 
            start_timestep=start_year,
            dataset_name = 'fdd',
            mode='w+'
        )
        fdd.config['raster_metadata'] = raster_metadata
        fdd.config['dataset_name'] = 'freezing degree-day'
        fdd.save(os.path.join(arguments['--out-fdd'], 'fdd.yml'))
        fdd = TemporalGrid(os.path.join(arguments['--out-fdd'], 'fdd.yml'))
    print('b')
    if  os.path.isfile(os.path.join(arguments['--out-tdd'], 'tdd.yml')):
        tdd = TemporalGrid(os.path.join(arguments['--out-tdd'], 'tdd.yml'))
    else:
        
        tdd = TemporalGrid(
            grid_shape[0], grid_shape[1], num_years, 
            start_timestep=start_year,
            dataset_name = 'tdd',
            mode='w+'
        )
        tdd.config['raster_metadata'] = raster_metadata
        tdd.config['dataset_name'] = 'thawing degree-day'
        tdd.save(os.path.join(arguments['--out-tdd'], 'tdd.yml'))
        tdd = TemporalGrid(os.path.join(arguments['--out-tdd'], 'tdd.yml'))

    roots = TemporalGrid(
        grid_shape[0], grid_shape[1], num_years*2, 
        # start_timestep=start_year,
        dataset_name = 'tdd',
        mode='w+'
    )


    
    days = create_day_array( 
        [ datetime.strptime(d, '%Y-%m') for d in list(
            monthly_temps.config['grid_name_map'].keys()
            )
        ] 
    )
    shape = monthly_temps.config['memory_shape']

    manager = Manager()

    print('SETUP COMPLETE')

    log = manager.dict() 
   
    log.update(
        {'Element Messages': manager.list() , 'Spline Errors': manager.list()}
    )
    log['verbose'] = verbosity

    print('starting')
#     print(monthly_temps.grids.shape, fdd.grids.shape)
    calc_grid_degree_days(
            days, 
            monthly_temps.grids, 
            tdd.grids, 
            fdd.grids, 
            grid_shape, 
            start= int(arguments['--start-at']) if arguments['--start-at'] else 0,
            num_process = num_processes,
            log=log,
            roots_grid=roots.grids,
            logging_dir = arguments['--logging-dir']
        )


    for item in log["Spline Errors"]:
        words = item.split(' ')
        location = int(words[-1])
        print ( grid_shape)
        row, col = np.unravel_index(location, grid_shape)  

        msg = ' '.join(words[:-1])
        msg += ' at row:' + str(row) + ', col:' + str(col) + '.'
        print(msg)

    try: 
        os.makedirs(arguments['--out-fdd'])
    except:
        pass
    try: 
        os.makedirs(arguments['--out-tdd'])
    except:
        pass

    if arguments['--out-roots']:
        try: 
            os.makedirs(arguments['--out-roots'])
        except:
            pass
    
    fdd.config['raster_metadata'] = raster_metadata
    fdd.config['dataset_name'] = 'freezing degree-day'

    if arguments['--out-format'] is None or \
            arguments['--out-format'] == 'tiff' :
        fdd.save_all_as_geotiff(arguments['--out-fdd'])
    elif arguments['--out-format'] == 'multigrid':
        fdd.config['command-used-to-create'] = ' '.join(sys.argv)
        fdd.save(os.path.join(arguments['--out-fdd'], 'fdd.yml'))

    tdd.config['raster_metadata'] = raster_metadata
    tdd.config['dataset_name'] = 'thawing degree-day'
    # tdd.save_all_as_geotiff(arguments['--out-tdd'
    if arguments['--out-format'] is None or \
            arguments['--out-format'] == 'tiff' :
        tdd.save_all_as_geotiff(arguments['--out-tdd'])
        

    elif arguments['--out-format'] == 'multigrid':
        tdd.config['command-used-to-create'] = ' '.join(sys.argv)
        tdd.save(os.path.join(arguments['--out-tdd'], 'tdd.yml'))

    if arguments['--out-roots']:
        roots.config['raster_metadata'] = raster_metadata
        roots.config['dataset_name'] = 'roots'
        # roots.save_all_as_geotiff(arguments['--out-roots'])
        if arguments['--out-format'] is None or \
            arguments['--out-format'] == 'tiff' :
            roots.save_all_as_geotiff(arguments['--out-roots'])
        elif arguments['--out-format'] == 'multigrid':
            roots.config['command-used-to-create'] = ' '.join(sys.argv)
            roots.save(os.path.join(arguments['--out-roots'], 'roots.yml'))

        # roots.save('./out_roots.yml')


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


