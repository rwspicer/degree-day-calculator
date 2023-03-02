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

from multiprocessing import Manager, Lock

from sort import sort_snap_files

import fill

import subutilities

def utility ():
    try:
        flags = {
            '--in-temperature': 
                {'required': False, 'type': str}, 
            '--temperature-data':  # new alias for --in-temperature
                {'required': False, 'type': str}, 
            '--out-directory':  
                {
                    'required': False, 
                    'type': str, 
                    # 'default': Non
                }, 
            '--out-fdd': 
                {'required': False, 'type': str},
            '--fdd-data': #new alias for --out-fdd
                {'required': False, 'type': str},
            "--out-tdd": 
                {'required': False, 'type': str},
            '--tdd-data': #new alias for --out-tdd
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
        print('error')
        print (E)
        print(utility.__doc__)
        return

    print(arguments)
    # sys.exit(0)

    verbosity = {'log':2, 'warn':1, '':0}[arguments['--verbose']]
    arguments.args['--verbose']=verbosity
    subutilities.update_arguments(arguments)
    print(arguments)

    paths = subutilities.configure_paths(arguments)

    data = subutilities.load_datasets(paths, arguments)

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

    if arguments['--fill-holes']:
        subutilities.fill_holes(data['fdd'], data['tdd'], arguments, log = log)
    else:

        subutilities.main_utility(data, paths, arguments, log)

    subutilities.write_results(data, paths, arguments, log)

    if not arguments['--save-temp-monthly']:
        for file in glob.glob('temp-monthly-temperature-data.*'):
            os.remove(file)
        


utility()
