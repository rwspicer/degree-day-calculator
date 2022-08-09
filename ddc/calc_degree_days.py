"""
Calc Degree Days
----------------

Tools for calculating and storing spatial degree days values from temperature
"""
from cmath import exp
import os
from re import I
import shutil
import gc
import warnings
from multiprocessing import Process, Lock, active_children, cpu_count 
from multiprocessing import set_start_method
from copy import deepcopy
from tempfile import mkdtemp

from dateutil.relativedelta import relativedelta

from progress.bar import Bar


import numpy as np
import matplotlib.pyplot as plt
from scipy import interpolate

try:
    from multigrids import temporal_grid
except ImportError:
    from .multigrids import temporal_grid
TemporalGrid = temporal_grid.TemporalGrid

ROW, COL = 0, 1

warnings.filterwarnings("ignore")

# Some Mac OS nonsense for python>=3.8. Macos uses 'spawn' now instead of
# 'fork' but that causes issues with passing np.memmap objcets. Anyway
# this might be unstable on Mac OS now
set_start_method('fork') 


def calc_degree_days_for_cell (
        index, monthly_temps, tdd, fdd, roots, method_map, lock = Lock(),
        log={'verbose':0}, use_fallback = False
        ):
    """Caclulate degree days (thawing, and freezing) and store in to 
    a grid.
    
    Parameters
    ----------
    index: int
        Flattened grid cell index.
    day_array: list like
        day number for each temperature value. 
        len(days_array) == len(temp_array).
    temp_array: list like
        Temperature values. len(days_array) == len(temp_array).
    tdd_grid: np.array
        2d grid of # years by flattend grid size. TDD values are stored here.
    fdd_grid: np.array
        2d grid of # years by flattend grid size. FDD values are stored here.
    lock: multiprocessing.Lock, Optional.
        lock object, If not passed a new lock is created.
    log: dict
    roots_grid: np.array
        2d grid of # years by flattend grid size X2. where roots are stored
        
    """
    expected_roots = 2 * tdd.config['num_timesteps']
    # print('->>', expected_roots, use_fallback)
    row, col = index
    # print('\n->', expected_roots)
    days = monthly_temps.convert_timesteps_to_julian_days()
    temps = monthly_temps[:, row, col]
    spline = interpolate.UnivariateSpline(days, temps)
    # print('->>', expected_roots,len(spline.roots()), use_fallback)

    tdd_temp = []
    fdd_temp = []
    roots_temp = []

    if len(spline.roots()) != expected_roots:
        for sf in range(1,51):
            
            spline.set_smoothing_factor(sf)
            # print('->>', expected_roots,len(spline.roots()), use_fallback)
            len_roots = len(spline.roots())  ## make really explicit to avoid bugs
            if len_roots == expected_roots:
                break

    # print('->>', expected_roots,len(spline.roots()), use_fallback)
    fallback = False
    if len(spline.roots()) == expected_roots and not use_fallback: 
        # print('default')
        for rdx in range(len(spline.roots())-1):
            val = spline.integral(spline.roots()[rdx], spline.roots()[rdx+1])
            if val > 0:
                roots_temp.append(spline.roots()[rdx])
                tdd_temp.append(val)
            else:
                fdd_temp.append(val)
                roots_temp.append(-1 * spline.roots()[rdx])

        fdd_temp.append(+8000) # dummy value

        roots_temp.append(spline.roots()[-1]  * roots_temp[-1]/abs(roots_temp[-1]) * -1) 

        method_map[row, col] = 1

        if len(fdd_temp) != tdd.config['num_timesteps'] or \
           len(tdd_temp) != tdd.config['num_timesteps']:
            # print(
            #     'fallback b'
            # )
            fallback = True
            method_map[row, col] = 3
            tdd_temp = []
            fdd_temp = []
            roots_temp = []
    else:
        fallback = True
        # print('fallback a')
        method_map[row, col] = 2
        # print('fdd len', len(fdd_temp))
        # print('tdd len', len(tdd_temp))
        # print('roots len', len(roots_temp))
        
    if fallback:
        # default = False
        # print('fallback')
        start= list(monthly_temps.config['grid_name_map'].keys())[0]

        start_year = list(monthly_temps.config['grid_name_map'].keys())[0].year
        end_year = list(monthly_temps.config['grid_name_map'].keys())[-1].year + 1
        
        delta_year = relativedelta(years=1) 
        delta_6_months = relativedelta(months=6)

        for year in range(end_year-start_year):
            start_tdd = list(monthly_temps.config['grid_name_map'].keys())[0] + \
                delta_year * year
            end_tdd = start_tdd + delta_year

            start_fdd = list(monthly_temps.config['grid_name_map'].keys())[0] + \
                delta_year * year + delta_6_months
            end_fdd = (start_fdd + delta_year)
    
            start_tdd = (start_tdd - start).days
            end_tdd =   (end_tdd- start).days
            start_fdd = (start_fdd- start).days
            end_fdd = (end_fdd- start).days
    
            fdd_roots = sorted([start_fdd, end_fdd] + \
                [i for i in spline.roots() if start_fdd < i <= end_fdd])
            tdd_roots = sorted([start_tdd, end_tdd] + \
                [i for i in spline.roots() if start_tdd < i <= end_tdd])
    
            tdd_val = []
            for idx in range(1,len(tdd_roots)):
                s = tdd_roots[idx-1]
                e = tdd_roots[idx]
                val = spline.integral(s,e)
                tdd_val.append(val)
                if idx == 1:
                    roots_temp.append(e)

            tdd_val = sum([v for v in tdd_val if v > 0])
            
            fdd_val = []
            for idx in range(1,len(fdd_roots)):
                s = fdd_roots[idx-1]
                e = fdd_roots[idx]
                val = spline.integral(s,e)
                fdd_val.append(val)
                if idx == 1:
                    roots_temp.append(-1 * e)
            fdd_val = sum([v for v in fdd_val if v < 0])
            fdd_temp.append(fdd_val)
            tdd_temp.append(tdd_val)
            

    # print (tdd)
    
    # tdd = np.arrtdd))
    # fdd = np.array(len(fdd))
    lock.acquire()
    
    tdd[:, row, col] = np.array(tdd_temp)
    


    ## FDD array is not long enough (len(tdd) - 1) on its own, so we use the 
    # first winter value twice this works because the the spline curves are
    # created will always have a first root going from negative to positistve
    # This works for northern alaska and should not be assumed else where.\

    ## NOTE: the last fdd values is either set to a dummy val or only 
    ## partial fdd so use second to last value
    fdd_temp = fdd_temp[:-1] + [fdd_temp[-2]] 
    fdd[:,row, col] = np.array( fdd_temp )
                                        # I.E. if last year of data is 2015, the 
                                        # fdd for 2015 is set to fdd for 2014
    roots[:,row, col] = np.array(roots_temp)
    
    # except ValueError as e:
    #     pass # not sure why this is here but it looks good
    #     print ("NEW_ERROR at", index,":", str(e))

    lock.release()

def calc_grid_degree_days (
        data,
        start = 0, num_process = 1, 
        log={'Element Messages': [], 'verbose':0},
        logging_dir=None,
        use_fallback=False,
        recalc_mask = None,
    ):
    """Calculate degree days (Thawing, and Freezing) for an area. 
    
    Parameters
    ----------
    day_array: list like
        day number for each temperature value. 
        len(days_array) == temp_grid.shape[0].
    temp_grid: np.array
        2d numpy array temperature values of timestep by flattened grid size.
        len(days_array) == temp_grid.shape[0].
    tdd_grid: np.array
        Empty thawing degree day grid. Size should be # of years to model by 
        flattened grid size. Values are set based on integration of a curve
        caluclated from days_array, and a timeseries from temp_grid. If
        the timeseries is all no data values np.nan is set as all valus 
        for element. If the curve has too many roots, or too few 
        (# roots != 2*# years) -inf is set as all values.
    fdd_grid: np.array
        Empty freezing degree day grid. Size should be # of years to model by 
        flattened grid size. Values are set based on integration of a curve
        caluclated from days_array, and a timeseries from temp_grid. If
        the timeseries is all no data values np.nan is set as all valus 
        for element. If the curve has too many roots, or too few 
        (# roots != 2*# years) -inf is set as all values.
    shape: tuple
        shape of model domain
    start: int, defaults to 0 , or list
        Calculate values starting at flattened grid index equal to start.
        If it is a list, list values should be grid indices, and only 
        values for those indices will be caclulated.
    num_process: int, Defaults to 1.
        number of processes to use to do calcualtion. If set to None,
        cpu_count() value is used. If greater than 1 ttd_grid, and fdd_grid 
        should be memory mapped numpy arrays.
    log: dict
    roots_grid: np.array
        2d grid of # years by flattend grid size X2. where roots are stored
    logging_dir: optional, path
        path to save diagnostic file indcating where data was interpolated
    
    Returns
    -------
    cells
        indexes of interpolated locations
    """
    monthly_temps = data['monthly-temperature']
    tdd = data['tdd']
    fdd = data['fdd']
    roots = data['roots']
    w_lock = Lock()
    
    if num_process == 1:
        num_process += 1 # need to have a better fix?
    elif num_process is None:
       num_process = cpu_count()

    shape=monthly_temps.config['grid_shape']
    
    # temp = './
    mm = os.path.join(logging_dir,'ddc-temp-methodmap.data')
    mode = 'w+'
    if os.path.exists(mm):
        mode = 'r+'
    print('mode', mode) 
    method_map = np.memmap(
        mm, shape=shape,
        dtype = float, mode=mode,
    )
    if mode == 'w+':
        method_map[:] = np.nan
    

    print('Calculating valid indices!')

    # indices = range(start, temp_grid.shape[1])
    init = monthly_temps[monthly_temps.config['start_timestep']].flatten()
    indices = ~np.isnan(init)
    if not recalc_mask is None:
        mask = recalc_mask.flatten()
        indices = np.logical_and(indices, mask)

    indices = np.where(indices)[0]
    





    indices = indices[indices > start]

    n_cells = shape[0] * shape[1]

    with Bar('Calculating Degree-days',  max=n_cells, suffix='%(percent)d%% - %(index)d / %(max)d') as bar:
        for idx in indices: # flatted area grid index
            row, col = np.unravel_index(idx, shape)
            while len(active_children()) >= num_process:
                continue
            [gc.collect(i) for i in range(3)] # garbage collection
            # log['Element Messages'].append(
            #     'calculating degree days for element ' + str(idx) + \
            #     '. ~' + '%.2f' % ((idx/n_cells) * 100) + '% complete.'
            # )
            # if log['verbose'] >= 2:
                # print(log['Element Messages'][-1])

            # if (monthly_temps[:,row, col] == -9999).all() or \
            #         (np.isnan(monthly_temps[:,idx])).all():
            #     monthly_temps[:,row, col] = np.nan
            #     monthly_temps[:,row, col] = np.nan
            #     if not roots_grid is None:
            #         roots_grid[:,idx] = np.nan
            #     log['Element Messages'].append(
            #         'Skipping element for missing values at ' + str(idx)
            #     )
            #     print(log['Element Messages'][-1])
            #     continue

            index = row, col
            if num_process == 1:
                calc_degree_days_for_cell(
                    index, monthly_temps, tdd, fdd, roots,  
                    method_map, w_lock, log, use_fallback
                )
            else:
                Process(
                    target=calc_degree_days_for_cell,
                    name = "calc degree day at elem " + str(idx),
                    args = (
                        index, monthly_temps, tdd, fdd, roots,  
                        method_map, w_lock, log,  use_fallback,
                    )
                ).start()
            bar.index = idx-1
            bar.next()
    
    while len(active_children()) > 0 :
        if len(active_children()) == 1:
            from multiprocessing.managers import DictProxy
            if type(log) is DictProxy:
                # this means the log is probably locking things up 
                break
            
        continue

    if logging_dir:
        try: 
            os.makedirs(logging_dir)
        except:
            pass
        np.save(os.path.join(logging_dir, 'methods.data'), method_map)
        with open(os.path.join(logging_dir, 'methods.readme.txt'), 'w') as fd:
            fd.write(
                'Nan values -> no input-data\n'
                '1 -> default spline method used\n'
                '2 -> range spline method used\n'
            )


def log(logging_dir, data):
    """old style logging
    """
    
    start_ts = data['tdd'].config['start_timestep']
    # shape = data['tdd'].config['grid_shape']

    log_grid = deepcopy(data['tdd'][start_ts])

    np.save(os.path.join(logging_dir, 'pre-cleanup-example.data'), log_grid)

    log_grid[log_grid>=0] = 1 
    log_grid[log_grid==-np.inf] = 0
    np.save(os.path.join(logging_dir, 'interpolated-mask.data'), log_grid)

    shutil.copyfile(
        data['fdd'].filename, 
        os.path.join(logging_dir, "temp-fdd-pre-cleanup.data")
    )

    shutil.copyfile(
        data['tdd'].filename, 
        os.path.join(logging_dir, "temp-tdd-pre-cleanup.data")
    )        

def fill_missing_by_interpolation(
        data, locations, log, func=np.nanmean, reset_locations = False
    ):
    """
    Parameters
    ----------
    data: TemporalGrid
    locations: np.array
    log: dict like
        logging dict
    reset_locations: bool
        if true reset cells at locations == True to -np.inf before 
        running interpolation
    """
    ## fix missing cells
    
    m_rows, m_cols = np.where(locations == True)   
   
    log['Element Messages'].append(
        'Interpolating missing data pixels using: %s' % func.__name__ 
    )
    if log['verbose'] >= 1:
        print(log['Element Messages'][-1])

    for grid_type in ['fdd', 'tdd', 'roots']:
        loop_data = data[grid_type]

        if reset_locations:
            log['Element Messages'].append(
                "Resetting (to -np.inf) Missing Locations Before Processing..."
            )
            if log['verbose'] >= 1:
                print(log['Element Messages'][-1])
            loop_data[:, locations] = -np.inf

        len_cells = range(len(m_rows))
        with Bar('Processing: %s' % grid_type,  max=len_cells) as bar:
            for cell in len_cells:
                # f_index = m_rows[cell] * shape[1] + m_cols[cell]  # 'flat' index of
                # cell location
                row, col = m_rows[cell], m_cols[cell]
                kernel = np.array(loop_data[:,row-1:row+2,col-1:col-2])
                
                kernel[kernel == -np.inf] = np.nan #clean kernel

                # mean = np.nanmean(kernel.reshape(tdd_grid.shape[0],9),axis = 1)
                loop_data[:, row, col] = np.nanmean(kernel, axis = 1).mean(1)
                bar.next()
            
    # return cells



# def create_day_array (dates):
#     """Calculates number of days after start day for each date in dates array
    
#     Parameters
#     ----------
#     dates: datetime.datetime
#         sorted list of dates
        
#     Returns
#     -------
#     days: list
#         list of interger days since first date. The first value will be 0.
#         len(days) == len(dates)
#     """
#     init = dates[0]
#     days = []
#     for date in dates:
#         days.append((date - init).days)
        
#     return days

# def npmm_to_mg (npmm, rows, cols, ts, cfg={}):
#     """convert a numpy memory map file (npmm) to a Temporal grid (mg)

#     Parameters
#     ----------
#     npmm: np.memmap
#         data
#     rows: int
#     cols: int
#     ts: int
#         number rows, columns, and timsteps in data
#     cfg: dict
#         Multigrid kwargs

#     Returns
#     -------
#     Multigrid
#     """
#     grid = TemporalGrid(rows, cols, ts)
#     grid.config.update(cfg)
#     grid.grids[:] = npmm.reshape(ts, rows*cols)[:]
#     return grid


    
    
