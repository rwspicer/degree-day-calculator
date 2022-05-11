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
from spicebox import CLILib

from multiprocessing import Manager, Lock

from sort import sort_snap_files

def create_or_load_dataset(
        data_path, grid_shape, num_years, start_year, name, raster_metadata
    ):
    """create or load an existing dataset
    """
    if  os.path.isfile(data_path):
        
        grids = TemporalGrid(data_path)
    else:
        grids = TemporalGrid(
            grid_shape[0], grid_shape[1], num_years, 
            start_timestep=start_year,
            # dataset_name = 'fdd',
            mode='w+'
        )
        grids.config['raster_metadata'] = raster_metadata
        grids.config['dataset_name'] = name
        grids.save(data_path)
        grids = TemporalGrid(data_path)
    return grids

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
    --start-at: int
        index to star-at on resuming processing

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
        flags = {
            '--in-temperature': 
                {'required': True, 'type': str}, 
            '--out-directory':  
                {
                    'required': False, 
                    'type': str, 
                    # 'default': Non
                }, 
            '--out-fdd': 
                {'required': False, 'type': str},
            "--out-tdd": 
                {'required': False, 'type': str},
            "--start-year": 
                {'required': True, 'type': int}, 

            "--num-processes": 
                {'required': False, 'default': 1, 'type': int },
            '--mask-val':  ## still broken
                {'required': False, 'type': int, 'default': -9999},
            '--verbose': 
                {
                    'required': False, 'type': str, 'default': '', 
                    'accepted-values': ['', 'log', 'warn']
                },
            '--sort-method': 
                {
                    'required': False, 'type': str, 'default':'default',
                    'accepted-values': ['default', 'snap']
                },

            '--logging-dir': 
                {'required': False, 'type': str },
            '--out-roots': 
                {'required': False, 'type': str, 'default': './temp-roots' },
            '--out-format': 
                {
                    'required': False, 'default': 'tiff', 'type': str, 
                    'accepted-values':['tiff','multigrid', 'both']
                },
            '--start-at': 
                {'required': False, 'type': int, 'default': 0 },
        }

        arguments = CLILib.CLI(flags)
    except (CLILib.CLILibHelpRequestedError, CLILib.CLILibMandatoryError) as E:
        print (E)
        print(utility.__doc__)
        return

    # print(arguments)
    # sys.exit(0)

    verbosity = {'log':2, 'warn':1, '':0}[arguments['--verbose']]
    
    sort_method = "Using default sort function"
    sort_fn = sorted

    if  arguments['--sort-method'].lower() == 'snap':
        sort_method = "Using SNAP sort function"
        sort_fn = sort_snap_files
    elif arguments['--sort-method'].lower() != "default":
        print("invalid --sort-method option")
        print("run utility.py --help to see valid options")
        print("exiting")
        return

    if verbosity >= 2:
        print('Seting up input...')
        print('\t', sort_method)

    if arguments['--out-directory']:
        out_fdd = os.path.join(arguments['--out-directory'], 'fdd')
        out_tdd = os.path.join(arguments['--out-directory'], 'tdd')
        out_roots = os.path.join(arguments['--out-directory'], 'roots')
        logging_dir = os.path.join(arguments['--out-directory'], 'logs')
    
    elif arguments['--out-fdd'] and arguments['--out-tdd']:
        out_fdd = arguments['--out-fdd']
        out_tdd = arguments['--out-tdd']
        out_roots = arguments['--out-roots']
        logging_dir = arguments['--logging-dir']
    else:
        print('Out directories  not specified. Use either:\n')
        print('    --out-directory for a unified output directory\n')
        print('  OR\n')
        print('    --out-fdd, --out-tdd, --out-roots(optional) to specify\n')
        print('    individual directories\n')
    
    
    for out_dir in out_fdd, out_tdd, out_roots:
        if out_dir:
            try: 
                os.makedirs(out_dir)
            except:
                pass

    start_year = int(arguments['--start-year'])

    num_processes = int(arguments['--num-processes'])
    
    if os.path.isfile(arguments['--in-temperature']):
        print('in file', arguments['--in-temperature'])
        monthly_temps = TemporalGrid(arguments['--in-temperature'])
        print(monthly_temps)
        num_years = monthly_temps.config['num_timesteps'] // 12
        raster_metadata  = monthly_temps.config['raster_metadata'] 
    else:
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
                "verbose": True if verbosity >= 2 else False,
                "filename": 'temp-in-temperature.data',
            }
        create_params = {
            "name": "monthly temperatures",
            # "start_timestep": int(arguments['--start-year']),
            "grid_names": temporal_grid_keys
            
        }
        # print(load_params)
        # print(create_params)

        monthly_temps = load_and_create(load_params, create_params)
        
        ex_raster = glob.glob(
                os.path.join(arguments['--in-temperature'],'*.tif')
            )[0]
        raster_metadata = get_raster_metadata(ex_raster)
        monthly_temps.config['raster_metadata'] = raster_metadata

        mask_val = -3.39999999999999996e+38 # TODO fix mask value feature
    #    if arguments['--mask-val'] is None:
    #        mask_val = int(arguments['--mask-val'])

        idx = monthly_temps.grids < -1000
        monthly_temps.grids[idx] = np.nan
        monthly_temps.config['num_timesteps'] = \
            monthly_temps.config['num_grids']

    grid_shape = monthly_temps.config['grid_shape']



    # data_path, grid_shape, num_years, start_year, name, raster_metadata

    fdd = create_or_load_dataset(
        os.path.join(out_fdd, 'fdd.yml'), 
        grid_shape, 
        num_years, 
        start_year, 
        'freezing degree-day', 
        raster_metadata
    )

    tdd = create_or_load_dataset(
        os.path.join(out_tdd, 'tdd.yml'), 
        grid_shape, 
        num_years, 
        start_year, 
        'thawing degree-day', 
        raster_metadata
    )

    roots = create_or_load_dataset(
        os.path.join(out_roots, 'roots.yml'), 
        grid_shape, 
        num_years * 2, 
        1901, 
        'spline-roots', 
        raster_metadata
    )

    days = create_day_array( 
        [ datetime.strptime(d, '%Y-%m') for d in list(
            monthly_temps.config['grid_name_map'].keys()
            )
        ] 
    )
    shape = monthly_temps.config['memory_shape']

    manager = Manager()  
    log = manager.dict() 
    log.update(
        {'Element Messages': manager.list() , 'Spline Errors': manager.list()}
    )
    log['verbose'] = verbosity

    print('starting')
    # print(fdd.grids.filename)
    # print(tdd.grids.filename)
    # print(roots.grids.filename)
    # print(monthly_temps.grids.filename)

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
            logging_dir = logging_dir
        )

    for item in log["Spline Errors"]:
        words = item.split(' ')
        location = int(words[-1])
        print ( grid_shape)
        row, col = np.unravel_index(location, grid_shape)  

        msg = ' '.join(words[:-1])
        msg += ' at row:' + str(row) + ', col:' + str(col) + '.'
        print(msg)

    # print(flags)
    if arguments['--out-format'] in ['tiff', 'both']:
        tdd.save_all_as_geotiff(out_tdd)
        fdd.save_all_as_geotiff(out_fdd)
        if out_roots != flags['--out-roots']['default']:
            roots.save_all_as_geotiff(out_roots)
    
    if arguments['--out-format'] == 'tiff':
        os.remove(os.path.join(out_tdd, 'tdd.yml'))
        filename = tdd.grids.filename
        os.remove(tdd.filter_file) if tdd.filter_file else None
        os.remove(tdd.mask_file) if tdd.mask_file else None
        del(tdd)
        os.remove(filename)

        os.remove(os.path.join(out_fdd, 'fdd.yml'))
        filename = fdd.grids.filename
        os.remove(fdd.filter_file) if fdd.filter_file else None
        os.remove(fdd.mask_file) if fdd.mask_file else None
        del(fdd)
        os.remove(filename)

        os.remove(os.path.join(out_roots, 'roots.yml'))
        filename = roots.grids.filename
        os.remove(roots.filter_file) if roots.filter_file else None
        os.remove(roots.mask_file) if roots.mask_file else None
        del(roots)
        # print(filename)
        os.remove(filename)
        
        

    if arguments['--out-format'] in ['multigrid','both']:
        fdd.config['command-used-to-create'] = ' '.join(sys.argv)
        tdd.config['command-used-to-create'] = ' '.join(sys.argv)
        roots.config['command-used-to-create'] = ' '.join(sys.argv)
        # tdd.save_all_as_geotiff(arguments['--out-tdd'])
        # fdd.save_all_as_geotiff(arguments['--out-fdd'])
        # if arguments['--out-roots']:
        #     roots.save_all_as_geotiff(arguments['--out-roots'])

    # if arguments['--out-format'] is None or \
    #         arguments['--out-format'] == 'tiff' :
    #     tdd.save_all_as_geotiff(arguments['--out-tdd'])
        
    # elif arguments['--out-format'] == 'multigrid':
    #     tdd.config['command-used-to-create'] = ' '.join(sys.argv)
    #     tdd.save(os.path.join(arguments['--out-tdd'], 'tdd.yml'))

    # if arguments['--out-roots']:
    #     roots.config['raster_metadata'] = raster_metadata
    #     roots.config['dataset_name'] = 'roots'
    #     # roots.save_all_as_geotiff(arguments['--out-roots'])
    #     if arguments['--out-format'] is None or \
    #         arguments['--out-format'] == 'tiff' :
    #         roots.save_all_as_geotiff(arguments['--out-roots'])
    #     elif arguments['--out-format'] == 'multigrid':
    #         roots.config['command-used-to-create'] = ' '.join(sys.argv)
    #         roots.save(os.path.join(arguments['--out-roots'], 'roots.yml'))

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

if __name__ == '__main__':
    utility()
