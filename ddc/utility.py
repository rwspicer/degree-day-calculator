"""
Degree-Day Calculator
---------------------
CLI utility for calculating degree-days from monthly data
"""
import glob
import os


from multigrids import TemporalGrid
from spicebox import CLILib
from __init__ import __version__

from multiprocessing import Manager
from multiprocessing.shared_memory import SharedMemory


import subutilities

def create_or_load_dataset(
        data_path, grid_shape, num_years, start_year, name, raster_metadata
    ):
    """create or load an existing dataset
    """
    if  os.path.isfile(data_path):
        
        grids = TemporalGrid(data_path)
        grids.config['degree-day-calculator-version'] = __version__
        grids.save(data_path)
        is_new = False
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
        is_new = True
    return grids, is_new

def utility ():
    """Utility for calculating the freezing and thawing degree-days and saving
    them as tiffs. Uses as spline based method to find roots from monthly 
    temperature data and calculate fdd and tdd by integration between roots.
    if roots cannot be calculated a fallback method is used by fixing seasons 
    for either summer and winter, and integrating positive or negative values 
    to find tdd and fdd.
    """
    try:
        flags = {   
            '--in-temperature':  # not required of --method == 'fill'
                {'required': False, 'type': str, 'default': ''},
            '--out-fdd': {'required': True, 'type': str},
            '--out-tdd': {'required': True, 'type': str},
            '--out-roots': {'required': True, 'type': str},
            '--logging-dir': {'required': True, 'type': str},

            "--start-year": {'required': True, 'type': int}, 
            
            ## INPUT STUFF
            '--sort-method': {
                    'required': False, 'type': str, 'default':'default',
                    'accepted-values': ['default', 'snap']
            },
            '--mask-val':  {'required': False, 'type': int}, ## NEEDS TESTING
            '--mask-comp':{
                    'required': False, 'default': 'eq', 'type': str, 
                    'accepted-values':['eq','ne', 'lt', 'gt', 'lte', 'gte']
            },
            '--save-temp-monthly': {
                'required': False, 'type': bool, 'default': False 
            },
            
            ## CALCULATION STUFF
            '--method': {
                'required': False, 'type': str, 'default': 'spline',
                'accepted-values': ['full', 'spline', 'fill']
            },
            "--num-processes": 
                {'required': False, 'default': 1, 'type': int },
            '--recalc-mask-file': {'required':False, 'type':str},
            '--start-at': 
                {'required': False, 'type': int, 'default': 0 },
            '--always-fallback':  {
                'required': False, 'type': bool, 'default': False
                },

            ## HOLE FILLING
            # '--hole-fill-method' : { # Not needed at this point
            #     'required': False, 'type': str, 
            #     'default': 'by-interpolation',  
            #     'accepted-values': ['by-interpolation'] 
            # },
             '--valid-area': {
                'required': False, 'type': str, 'default': '',   
            },
            '--area-type': {
                'required': False, 'type': str, 
                'default': 'aoi',  
                'accepted-values': ['aoi', 'exact'] 
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

    ## GENERAL SETUP
    config = {
        'directories': {}, 
        'method': 'full'
    }
    data = {}

    verbosity = {'log':2, 'warn':1, '':0}[arguments['--verbose']]

    manager = Manager()  
    config['log'] = manager.dict() 
    config['log'].update(
        {'Element Messages': manager.list() , 'Spline Errors': manager.list()}
    )
    config['log']['verbose'] = verbosity



    ## LOAD DATA
    subutilities.setup_directories(arguments,config,data)
    subutilities.setup_input_data(arguments,config,data)
    subutilities.setup_output_data(arguments,config,data)
    
    ## CALCULATE
    if arguments['--method'] in ['full', 'spline']:
        config['sm-name'] = 'dcc-memory'
        config['shared-mem'] = SharedMemory(
            name=config['sm-name'], size=128, create=True
        )
        worker = subutilities.write_results_worker(arguments,config,data)
        subutilities.calculate(arguments,config,data)
        worker.join()
        
        # populated now so not new
        config['fdd_new'] = False
        config['tdd_new'] = False
        config['roots_new'] = False
 
    if arguments['--method'] in ['full', 'fill']:
        subutilities.fill_holes(arguments, config, data)


    ## WRITE RESULTS
    subutilities.save_results(arguments,config,data)
    


if __name__ == '__main__':
    utility()
