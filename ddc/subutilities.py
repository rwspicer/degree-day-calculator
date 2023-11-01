"""
Subutilities and processes
--------------------------
"""
import glob
import os, sys

from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np

from calc_degree_days import calc_grid_degree_days#, create_day_array
from multigrids.tools import load_and_create, get_raster_metadata
from multigrids import TemporalGrid
from __init__ import __version__

from multiprocessing import Manager, Lock, Process


from sort import sort_snap_files

import fill

from spicebox import raster



def start_write_results_worker(arguments, config, data):

    tdd = data['tdd'] 
    fdd = data['fdd']
    roots = data['roots'],
    temp_dir = config['temp_results_dir']
    sm_name = config['sm_name']
    writer = Process(
        target=write_results_loop, 
        args=(tdd, fdd, roots, temp_dir, sm_name))
    writer.start()
    return writer

def setup_directories(arguments, config, data):

    config['directories']['fdd'] = arguments['--out-fdd']
    config['directories']['tdd'] = arguments['--out-tdd']
    config['directories']['roots']  = arguments['--out-roots']
    config['directories']['logs'] = arguments['--logging-dir']
 
    for name, out_dir in config['directories'].items():
        if out_dir:
            try: 
                os.makedirs(out_dir)
            except:
                pass

def fill_holes(arguments, config, data):
    if config['fdd_new'] or config['tdd_new']:
        print('Degree-day data missing/')
        raise IOError('Degree-day data missing for filling holes')
    method = fill.by_interpolation
    locations = raster.load_raster(arguments['--valid-area'])[0]

    locations[np.isnan(locations)] = 0
    locations[ locations>1 ]  = 1
    locations = locations.astype(int)
    if arguments['--area-type'] == 'aoi':
        sample = data['fdd'][data['fdd'].config['start_timestep']]
        
        locations = np.logical_and(locations, np.isnan(sample))


    print("Filling Holes in FDD data")
    method(
        data['fdd'], locations, config['log'], 
        func=np.nanmean, 
        reset_locations = arguments['--reset-bad-cells'],
        loc_type = 'map', 
        k_size = arguments['--kernel-size']
    )
    print("Filling Holes in TDD data")
    method(
        data['tdd'], locations, config['log'], 
        func=np.nanmean, 
        reset_locations = arguments['--reset-bad-cells'],
        loc_type = 'map', 
        k_size = arguments['--kernel-size']
    )

def setup_input_data (arguments, config, data):

    if os.path.isfile(arguments['--in-temperature']):
        print('in file', arguments['--in-temperature'])
        data['monthly-temperature'] = TemporalGrid(arguments['--in-temperature'])
        print(data['monthly-temperature'])
        # num_years = data['monthly-temperature'].config['num_timesteps'] // 12
        # raster_metadata  = data['monthly-temperature'].config['raster_metadata'] 
    else:
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
        
        num_years = len(
            glob.glob(os.path.join(arguments['--in-temperature'],'*.tif')) 
        ) 
        num_years = num_years // 12

        years = [config['start_year'] + yr for yr in range(num_years)]

        temporal_grid_keys = [] 
        for yr in years: 
            for mn in range(1,13):
                temporal_grid_keys.append('%d-%02d' % (yr,mn) ) 
        # print(glob.glob(arguments['--in-temperature']))
        load_params = {
                "method": "tiff",
                "directory": arguments['--in-temperature'],
                "sort_func": sort_fn,
                "verbose": True if config['log']['verbose'] >= 2 else False,
                "filename": 'temp-in-temperature.data',
            }
        create_params = {
            "name": "monthly temperatures",
            "grid_names": temporal_grid_keys,
            "start_timestep": datetime(arguments['--start-year'],1,1),
            "delta_timestep": relativedelta(months=1)
            
        }

        data['monthly-temperature'] = load_and_create(load_params, create_params)
        
        ex_raster = glob.glob(
                os.path.join(arguments['--in-temperature'],'*.tif')
            )[0]
        raster_metadata = get_raster_metadata(ex_raster)
        data['monthly-temperature'].config['raster_metadata'] = raster_metadata

        if not arguments['--mask-val'] is None:
            mask = arguments['--mask-val']
            if arguments['--mask-comp']   == 'eq':
                no_data_idx =     data['monthly-temperature'].grids[0] == mask
            elif arguments['--mask-comp'] == 'ne':
                no_data_idx =     data['monthly-temperature'].grids[0] != mask
            elif arguments['--mask-comp'] == 'lt':
                no_data_idx =     data['monthly-temperature'].grids[0] < mask
            elif arguments['--mask-comp'] == 'gt':
                no_data_idx =     data['monthly-temperature'].grids[0] > mask
            elif arguments['--mask-comp'] == 'lte':
                no_data_idx =     data['monthly-temperature'].grids[0] <= mask
            elif arguments['--mask-comp'] == 'gte':
                no_data_idx =     data['monthly-temperature'].grids[0] >= mask           
            
            for gn in range(data['monthly-temperature'].grids.shape[0]):
                data['monthly-temperature'].grids[gn][no_data_idx] = np.nan


        
            
        data['monthly-temperature'].config['num_timesteps'] = \
            data['monthly-temperature'].config['num_grids']
        
        data['monthly-temperature'].save('temp-monthly-temperature-data.yml')

def setup_output_data (arguments, config, data):

    grid_shape = data['monthly-temperature'].config['grid_shape']
    raster_metadata = data['monthly-temperature'].config['raster_metadata']
    num_years = data['monthly-temperature'].config['num_timesteps'] // 12

    data['fdd'], config['fdd_new'] = create_or_load_dataset(
        config['directories']['fdd'], 
        grid_shape, 
        num_years, 
        config['start_year'], 
        'freezing degree-day', 
        raster_metadata
    )

    data['tdd'], config['tdd_new'] = create_or_load_dataset(
        config['directories']['tdd'], 
        grid_shape, 
        num_years, 
        config['start_year'], 
        'thawing degree-day', 
        raster_metadata
    )

    data['roots'], config['roots_new'] = create_or_load_dataset(
        config['directories']['roots'], 
        grid_shape, 
        num_years * 2, 
        0, 
        'spline-roots', 
        raster_metadata
    )
    data['roots'].config['delta_timestep'] = "varies"

def calculate(arguments, config, data):
    num_processes = int(arguments['--num-processes'])

    recalc_mask = None
    if not arguments['--recalc-mask-file'] is None:
        recalc_mask = np.load(arguments['--recalc-mask-file']).astype(int) == 1
    
    start_at = int(arguments['--start-at']) if arguments['--start-at'] else 0

    calc_grid_degree_days(
            data,
            start = start_at, 
            num_process = num_processes,
            log=config['log'], 
            logging_dir=config['directories']['logs'],
            use_fallback=arguments['--always-fallback'],
            recalc_mask = recalc_mask,
            temp_dir = config['temp_results_dir'],
            shared_memory = config['shared-mem']
    )
    
    for item in config['log']["Spline Errors"]:
        words = item.split(' ')
        location = int(words[-1])
        grid_shape = data['monthly-temperature'].config['grid_shape']
        # print (grid_shape)
        row, col = np.unravel_index(location, grid_shape)  

        msg = ' '.join(words[:-1])
        msg += ' at row:' + str(row) + ', col:' + str(col) + '.'
        print(msg)
    
def save_results(arguments, config, data):

    if arguments['--out-format'] in ['tiff', 'both']:
        tif_path = lambda x: os.path.join(config['directories'][x], 'tiff')
        data['tdd'].save_all_as_geotiff(tif_path('fdd'))
        data['fdd'].save_all_as_geotiff(tif_path('tdd'))
        data['roots'].save_all_as_geotiff(tif_path('roots'))
    
    if arguments['--out-format'] == 'tiff':
        yml_path = lambda x: os.path.join(config['directories'][x], '%s.yml' % x)
        os.remove(yml_path('tdd'))
        tdd = data['tdd']
        filename = tdd.grids.filename
        os.remove(tdd.filter_file) if tdd.filter_file else None
        os.remove(tdd.mask_file) if tdd.mask_file else None
        del(tdd)
        os.remove(filename)

        os.remove(yml_path('fdd'))
        fdd = data['fdd']
        filename = fdd.grids.filename
        os.remove(fdd.filter_file) if fdd.filter_file else None
        os.remove(fdd.mask_file) if fdd.mask_file else None
        del(fdd)
        os.remove(filename)

        os.remove(yml_path('roots'))
        roots = data['roots']
        filename = roots.grids.filename
        os.remove(roots.filter_file) if roots.filter_file else None
        os.remove(roots.mask_file) if roots.mask_file else None
        del(roots)
        # print(filename)
        os.remove(filename)
        
        

    if arguments['--out-format'] in ['multigrid','both']:
        mg_path = lambda x: os.path.join(
            config['directories'][x], 'multigrid', '%s.yml' % x
        )
        fdd.config['command-used-to-create'] = ' '.join(sys.argv)
        tdd.config['command-used-to-create'] = ' '.join(sys.argv)
        roots.config['command-used-to-create'] = ' '.join(sys.argv)
        # print(os.path.join(out_fdd, 'fdd.yml'))
        fdd.save(mg_path('fdd'))
        tdd.save(mg_path('tdd'))
        roots.save(mg_path('roots'))

    if not arguments['--save-temp-monthly']:
        for file in glob.glob('temp-monthly-temperature-data.*'):
            os.remove(file)

