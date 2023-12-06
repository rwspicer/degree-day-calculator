"""
Degree-Day Calculator
---------------------
CLI utility for calculating degree-days from monthly data
"""
import glob
import os, sys

from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np

from calc_degree_days import calc_grid_degree_days#, create_day_array
from multigrids.tools import load_and_create, get_raster_metadata
from multigrids import TemporalGrid
from spicebox import CLILib
from __init__ import __version__

from multiprocessing import Manager, Lock, Process, active_children

from sort import sort_snap_files

import fill

def create_or_load_dataset(
        data_path, grid_shape, num_years, start_year, name, raster_metadata
    ):
    """create or load an existing dataset
    """
    if  os.path.isfile(data_path):
        
        grids = TemporalGrid(data_path)
        grids.config['degree-day-calculator-version'] = __version__
        grids.save(data_path)
    else:
        grids = TemporalGrid(
            grid_shape[0], grid_shape[1], num_years, 
            start_timestep=start_year,
            # dataset_name = 'fdd',
            mode='w+',
            save_to=data_path, 
        )
        grids.config['raster_metadata'] = raster_metadata
        grids.config['dataset_name'] = name
        grids.config['degree-day-calculator-version'] = __version__
        grids.save(data_path)
        grids = TemporalGrid(data_path)
        for ts in grids.timestep_range():
            # print(ts)
            grids[ts] = np.nan
    return grids

def write_results_worker(tdd, fdd, roots, temp_dir):
    while True:
        try:
            _file = glob.glob(os.path.join(temp_dir,'*.npz'))[0]
            data = np.load(_file)
        except:
            continue
        row, col = [int(d) for d in os.path.split(_file)[1].split('.')[:2]]
        tdd[:,row,col] = data['tdd']
        fdd[:,row,col] = data['fdd']
        roots[:,row,col] = data['roots']
        del(data)
        try:
            os.remove(_file)
        except:
            pass
            
def utility ():
    """Utility for calculating the freezing and thawing degree-days and saving
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
    """
    try:
        flags = {
            '--in-temperature': 
                {'required': False, 'type': str, 'default': ''}, 
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
                {'required': False, 'type': int},
            '--mask-comp':
                {
                    'required': False, 'default': 'eq', 'type': str, 
                    'accepted-values':['eq','ne', 'lt', 'gt', 'lte', 'gte']
                },
            '--recalc-mask-file': {'required':False, 'type':str},
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
            '--save-temp-monthly':
                {'required': False, 'type': bool, 'default': False },
            '--always-fallback':  {'required': False, 'type': bool, 'default': False },
            '--fill-holes': {'required': False, 'type': bool, 'default': False },
            '--valid-area': {
                'required': False, 'type': str, 'default': '',   
            },
            '--area-type': {
                'required': False, 'type': str, 
                'default': 'aoi',  
                'accepted-values': ['aoi', 'exact'] 
            },
            '--hole-fill-method' : {
                'required': False, 'type': str, 
                'default': 'by-interpolation',  
                'accepted-values': ['by-interpolation'] 
            },
            '--reset-bad-cells' : {
                'required': False, 'type': bool, 'default': True
            },
            '--kernel-size' :{'required': False, 'type': int, 'default': 1},
            
        }

        arguments = CLILib.CLI(flags)
    except (CLILib.CLILibHelpRequestedError, CLILib.CLILibMandatoryError) as E:
        print (E)
        print(utility.__doc__)
        return

    # print(arguments)
    # sys.exit(0)

    verbosity = {'log':2, 'warn':1, '':0}[arguments['--verbose']]

    if arguments['--fill-holes']:
        if not os.path.exists(os.path.join(out_fdd, 'fdd.yml')) and \
                not os.path.exists(os.path.join(out_tdd, 'tdd.yml')):
            print('Degree-day data missing/')
            sys.exit()
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
        fill.sub_utility(fdd, tdd, arguments, log = {})

        if arguments['--out-format'] in ['tiff', 'both']:
            tdd.save_all_as_geotiff(out_tdd)
            fdd.save_all_as_geotiff(out_fdd)
            
            
        sys.exit()

    
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
        # print(glob.glob(arguments['--in-temperature']))
        load_params = {
                "method": "tiff",
                "directory": arguments['--in-temperature'],
                "sort_func": sort_fn,
                "verbose": True if verbosity >= 2 else False,
                "filename": 'temp-in-temperature.data',
            }
        create_params = {
            "name": "monthly temperatures",
            "grid_names": temporal_grid_keys,
            "start_timestep": datetime(arguments['--start-year'],1,1),
            "delta_timestep": relativedelta(months=1)
            
        }

        monthly_temps = load_and_create(load_params, create_params)
        
        ex_raster = glob.glob(
                os.path.join(arguments['--in-temperature'],'*.tif')
            )[0]
        raster_metadata = get_raster_metadata(ex_raster)
        monthly_temps.config['raster_metadata'] = raster_metadata

        if not arguments['--mask-val'] is None:
            mask = arguments['--mask-val']
            if arguments['--mask-comp']   == 'eq':
                no_data_idx =     monthly_temps.grids == mask
            elif arguments['--mask-comp'] == 'ne':
                no_data_idx =     monthly_temps.grids != mask
            elif arguments['--mask-comp'] == 'lt':
                no_data_idx =     monthly_temps.grids < mask
            elif arguments['--mask-comp'] == 'gt':
                no_data_idx =     monthly_temps.grids > mask
            elif arguments['--mask-comp'] == 'lte':
                no_data_idx =     monthly_temps.grids <= mask
            elif arguments['--mask-comp'] == 'gte':
                no_data_idx =     monthly_temps.grids >= mask   
            # elif arguments['--mask-comp'] == 'map':
            #     no_data_idx = monthly_temps.grids == 0          
            monthly_temps.grids[no_data_idx] = np.nan

        
            
        monthly_temps.config['num_timesteps'] = \
            monthly_temps.config['num_grids']
        
        monthly_temps.save('temp-monthly-temperature-data.yml')

    recalc_mask = None
    if not arguments['--recalc-mask-file'] is None:
        recalc_mask = np.load(arguments['--recalc-mask-file']).astype(int) == 1
    
    grid_shape = monthly_temps.config['grid_shape']

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
        0, 
        'spline-roots', 
        raster_metadata
    )
    roots.config['delta_timestep'] = "varies"

    # days = create_day_array( 
    #     [ datetime.strptime(d, '%Y-%m') for d in list(
    #         monthly_temps.config['grid_name_map'].keys()
    #         )
    #      ] 
    # )

    manager = Manager()  
    log = manager.dict() 
    log.update(
        {'Element Messages': manager.list() , 'Spline Errors': manager.list()}
    )
    log['verbose'] = verbosity

    try:
        os.makedirs(logging_dir)
    except:
        pass


    temp_dir = 'temp_dd_arrays'
    try:
        os.makedirs(temp_dir)
    except: 
        pass
    writer = Process(
        target=write_results_worker, 
        args=(tdd, fdd, roots, temp_dir),
        name = 'writer')
    writer.start()

    print('starting')
    # print(fdd.grids.filename)
    # print(tdd.grids.filename)
    # print(roots.grids.filename)
    # print(monthly_temps.grids.filename)
    data = {
        'monthly-temperature': monthly_temps,
        'tdd': tdd,
        'fdd': fdd,
        'roots': roots
    }
    init_pids = set([p.pid for p in active_children()])
    calc_grid_degree_days (
        data,
        start = int(arguments['--start-at']) if arguments['--start-at'] else 0, 
        num_process = num_processes,
        log=log, 
        logging_dir=logging_dir,
        use_fallback=arguments['--always-fallback'],
        recalc_mask = recalc_mask,
        temp_dir = temp_dir,
        init_pids = init_pids
    )
    # calc_grid_degree_days(
    #         days, 
    #         monthly_temps.grids, 
    #         tdd.grids, 
    #         fdd.grids, 
    #         grid_shape, 
    #         start= int(arguments['--start-at']) if arguments['--start-at'] else 0,
    #         num_process = num_processes,
    #         log=log,
    #         roots_grid=roots.grids,
    #         logging_dir = logging_dir
    #     )


    print('Waiting for results to be written')
    while(len(glob.glob(os.path.join(temp_dir, '*.npz'))) != 0):
        continue

    writer.kill()
    try:
        os.remove(temp_dir)
    except:
        pass

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
        # print(os.path.join(out_fdd, 'fdd.yml'))
        fdd.save(os.path.join(out_fdd, 'fdd.yml'))
        tdd.save(os.path.join(out_tdd, 'tdd.yml'))
        roots.save(os.path.join(out_roots, 'roots.yml'))

    if not arguments['--save-temp-monthly']:
        for file in glob.glob('temp-monthly-temperature-data.*'):
            os.remove(file)

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
