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

from multiprocessing import Manager, Lock

from sort import sort_snap_files

import fill


def update_arguments (arguments):

    if arguments['--in-temperature'] is None and \
            arguments['--temperature-data'] is None and \
            arguments['--fill-holes']:
        print("Either '--in-temperature' or '--temperature-data' must be set")
        print("when '--fill-holse is False")
        sys.exit()
    elif arguments['--in-temperature'] and \
            arguments['--temperature-data'] is None:
        ##old alais is used
        arguments.args['--temperature-data'] = arguments['--in-temperature']
    elif arguments['--in-temperature'] and arguments['--temperature-data']:
        print("set either '--in-temperature' or '--temperature-data' not both")
        sys.exit()

    if arguments['--out-directory'] is None:
        if arguments['--out-fdd'] is None and \
                arguments['--fdd-data'] is None:
            print("Either '--out-fdd' or '--fdd-data' must be set")
            sys.exit()
        elif arguments['--out-fdd'] and \
                arguments['--fdd-data'] is None:
            ##old alais is used
            arguments.args['--fdd-data'] = arguments['--out-fdd']
        elif arguments['--out-fdd'] and arguments['--fdd-data']:
            print("set either '--out-fdd' or '--fdd-data' not both")
            sys.exit()
        
        if arguments['--out-tdd'] is None and \
                arguments['--tdd-data'] is None:
            print("Either '--out-tdd' or '--tdd-data' must be set")
            sys.exit()
        elif arguments['--out-tdd'] and \
                arguments['--tdd-data'] is None:
            ##old alais is used
            arguments.args['--tdd-data'] = arguments['--out-tdd']
        elif arguments['--out-tdd'] and arguments['--tdd-data']:
            print("set either '--out-tdd' or '--tdd-data' not both")
            sys.exit()

    # print(arguments)



def create_or_load_dataset(
        data_path, grid_shape, num_years, start_year, name, raster_metadata,
        do_not_create = False
    ):
    """create or load an existing dataset
    """
    if  os.path.isfile(data_path):
        
        grids = TemporalGrid(data_path)
        grids.config['degree-day-calculator-version'] = __version__
        grids.save(data_path)
    elif not do_not_create:
        grids = TemporalGrid(
            grid_shape[0], grid_shape[1], num_years, 
            start_timestep=start_year,
            # dataset_name = 'fdd',
            mode='w+'
        )
        grids.config['raster_metadata'] = raster_metadata
        grids.config['dataset_name'] = name
        grids.config['degree-day-calculator-version'] = __version__
        grids.save(data_path)
        grids = TemporalGrid(data_path)
        for ts in grids.timestep_range():
            # print(ts)
            grids[ts] = np.nan
    else:
        raise IOError('Could not create or load grid')
    return grids


def configure_paths(arguments):
    """
    """
   
    #     print('\t', sort_method)

    paths = { 
        'temperature': arguments['--temperature-data'],
        'fdd': None,
        'tdd':  None,
        'roots':  None,
        'logging':  None,
    }

    if arguments['--out-directory']:
        paths['fdd'] = os.path.join(arguments['--out-directory'], 'fdd')
        paths['tdd'] = os.path.join(arguments['--out-directory'], 'tdd')
        paths['roots'] = os.path.join(arguments['--out-directory'], 'roots')
        paths['logging'] = os.path.join(arguments['--out-directory'], 'logs')
    
    elif arguments['--fdd-data'] and arguments['--tdd-data']:
        paths['fdd'] = arguments['--fdd-data']
        paths['tdd'] = arguments['--tdd-data']
        paths['roots'] = arguments['--out-roots']
        paths['logging'] = arguments['--logging-dir']
    else:
        print('Out directories  not specified. Use either:\n')
        print('    --out-directory for a unified output directory\n')
        print('  OR\n')
        print('    --out-fdd, --out-tdd, --out-roots(optional) to specify\n')
        print('    individual directories\n')

    return paths


def load_datasets(paths, arguments):


    data = {}

    if not arguments['--fill-holes']:
        start_year = int(arguments['--start-year'])

        # num_processes = int(arguments['--num-processes'])

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

        if arguments['--verbose'] >= 2:
            print('Setting up input...')

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
                    "verbose": True if arguments['--verbose'] >= 2 else False,
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
        
        grid_shape = monthly_temps.config['grid_shape']

        data['temperature'] = monthly_temps
        do_not_create = False
    else:
        grid_shape = None
        num_years = None
        start_year = None
        raster_metadata = None
        do_not_create = True


    data['fdd'] = create_or_load_dataset(
        os.path.join(paths['fdd'], 'fdd.yml'), 
        grid_shape, 
        num_years, 
        start_year, 
        'freezing degree-day', 
        raster_metadata,
        do_not_create
    )

    data['tdd'] = create_or_load_dataset(
        os.path.join(paths['tdd'], 'tdd.yml'), 
        grid_shape, 
        num_years, 
        start_year, 
        'thawing degree-day', 
        raster_metadata,
        do_not_create
    )

    if not arguments['--fill-holes']:
        data['roots'] = create_or_load_dataset(
            os.path.join(paths['roots'], 'roots.yml'), 
            grid_shape, 
            num_years * 2, 
            0, 
            'spline-roots', 
            raster_metadata
        )
        data['roots'].config['delta_timestep'] = "varies"

    return data
