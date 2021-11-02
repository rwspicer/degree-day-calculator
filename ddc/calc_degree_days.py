"""
Calc Degree Days
----------------

Tools for caclualting and storing spatial degree days values from temperature
"""
import numpy as np
from scipy import interpolate
from multiprocessing import Process, Lock, active_children, cpu_count
from copy import deepcopy
import os
from datetime import datetime
import matplotlib.pyplot as plt

ROW, COL = 0, 1

# from stack_rasters import load_and_stack

try:
    from multigrids import temporal_grid
except ImportError:
    from .multigrids import temporal_grid
 
TemporalGrid = temporal_grid.TemporalGrid


def calc_degree_days(
            day_array, temp_array, expected_roots = None,
            log={'Spline Errors': [], 'verbose':0}, idx="Unknown",
            keep_roots = False
        ):
    """Calc degree days (thawing, and freezing)
    
    Parameters
    ----------
    day_array: list like
        day number for each temperature value. 
        len(days_array) == len(temp_array).
    temp_array: list like
        Temperature values. len(days_array) == len(temp_array).
    expected_roots: int, None
        # of roots expected to find
    log: dict
    idx: str or int
        idx label used for writing the log
    keep_roots: bool
        if true roots are returned with fdd and tdd arrays
    
        
    Returns
    -------
    tdd, fdd: lists
        thawing and freezing degree day lists
    """
    # print (day_array)
#     if np.isnan(temp_array).all():
#         return np.zeros(115) - np.inf,np.zeros(115) - np.inf
    # print (temp_array)
#     print('processing element', idx)
    spline = interpolate.UnivariateSpline(day_array, temp_array)
    if not expected_roots is None and len(spline.roots()) != expected_roots:
        # print('reprocessing element', idx)
        # print (len(spline.roots()))
        i = 1
        while len(spline.roots()) != expected_roots:
            spline.set_smoothing_factor(i)
            i+= 1
            #print len(spline.roots())
            if i >50:
                log['Spline Errors'].append(
                    'expected root mismatch at element ' + str(idx)
                )
                if log['verbose'] >= 1:
                    print(log['Spline Errors'][-1])
                # print('expected root mismatch at element ' + str(idx))
                # print ('--->expected roots is not the same as spline.roots()', 'er', expected_roots, 'sr', len(spline.roots()))
                return list(np.zeros(expected_roots//2) - np.inf),list(np.zeros((expected_roots//2) - 1) - np.inf)

    tdd = []
    fdd = []
    roots = []
    for rdx in range(len(spline.roots())-1):
        val = spline.integral(spline.roots()[rdx], spline.roots()[rdx+1])
        # print(val)
        if val > 0:
            roots.append(spline.roots()[rdx])
            tdd.append(val)
        else:
            fdd.append(val)
            roots.append(-1 * spline.roots()[rdx])

    roots.append(spline.roots()[-1]  * roots[-1]/abs(roots[-1]) * -1)
            

    if keep_roots:
        return tdd, fdd, roots
    return tdd, fdd #, spline


def calc_and_store  (
        index, day_array, temp_array, tdd_grid, fdd_grid, lock = Lock(),
        log={'verbose':0}, roots_grid = None
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
    
    expected_roots = 2 * len(tdd_grid)
    if roots_grid is None:
        tdd, fdd  = calc_degree_days(
            day_array, temp_array, expected_roots, log, index, False
        )
    else:
        tdd, fdd, roots  = calc_degree_days(
            day_array, temp_array, expected_roots, log, index, True
        )


    lock.acquire()
    tdd_grid[:,index] = tdd
    ## FDD array is not long enough (len(tdd) - 1) on its own, so we use the 
    # first winter value twice this works because the the spline curves are
    # created will always have a first root going from negative to positistve
    # This works for northern alaska and should not be assumed else where.\
    ##

    ### see the notes
    ##
    fdd_grid[:,index] = fdd + [fdd[-1]] # I.E. if last year of data is 2015, the 
                                        # fdd for 2015 is set to fdd for 2014

    if not roots_grid is None:
        roots_grid[:,index] = roots
    lock.release()




def calc_grid_degree_days (
        day_array, temp_grid, tdd_grid, fdd_grid, shape, 
        start = 0, num_process = 1, 
        log={'Element Messages': [], 'verbose':0}, roots_grid = None,
        logging_dir=None
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
    

    returns
    -------
    cells
        indexes of interpolated locations
    """
    # p_lock = Lock()
    w_lock = Lock()
    
    if num_process is None:
       num_process = cpu_count()
    
    if type(start) is int:   
        indices = range(start, temp_grid.shape[1])
    else:
        indices = start
    
    if num_process == 1:
        num_process += 1 # need to have a better fix?
    for idx in indices: # flatted area grid index
        while len(active_children()) >= num_process:
            continue
        log['Element Messages'].append(
            'calculating degree days for element ' + str(idx) + \
            '. ~' + '%.2f' % ((idx/len(indices)) * 100) + '% complete.'
        )
        # print(idx)
        if log['verbose'] >= 2:
            print(log['Element Messages'][-1])


        if (temp_grid[:,idx] == -9999).all() or \
                (np.isnan(temp_grid[:,idx])).all():
            w_lock.acquire()
            tdd_grid[:,idx] = np.nan
            fdd_grid[:,idx] = np.nan
            if not roots_grid is None:
                roots_grid[:,idx] = np.nan
            w_lock.release()
            log['Element Messages'].append(
                'Skipping element for missing values at ' + str(idx)
            )
            if log['verbose'] >= 2:
                print(log['Element Messages'][-1])
            continue
        Process(target=calc_and_store,
            name = "calc degree day at elem " + str(idx),
            args=(
                idx,day_array,temp_grid[:,idx], tdd_grid, fdd_grid, w_lock, log,
                roots_grid
            )
        ).start()
    
    while len(active_children()) > 0 :
        if len(active_children()) == 1:
            from multiprocessing.managers import DictProxy
            if type(log) is DictProxy:
                # this means the log is probably locking things up 
                break
            
        continue
    
    ## fix missing cells
    m_rows, m_cols = np.where(tdd_grid[0].reshape(shape) == -np.inf)   
    #~ print  m_rows, m_cols
    cells = []

    try: 
        os.makedirs(logging_dir)
    except:
        pass

    if logging_dir:
        log_grid = deepcopy(tdd_grid[0].reshape(shape))
        np.save(os.path.join(logging_dir, 'pre_cleanup_example.data'), log_grid)
        log_grid[log_grid>=0] = 1 
        log_grid[log_grid==-np.inf] = 0
        np.save(os.path.join(logging_dir, 'interpolated.data'), log_grid)
    try: 
        os.makedirs(logging_dir)
    except:
        pass
    
    # plt.imsave(os.path.join(logging_dir, 'interpolated.png'), log_grid)
    



    for cell in range(len(m_rows)):
        f_index = m_rows[cell] * shape[1] + m_cols[cell]  # 'flat' index of
        # cell location
        
        
        g_tdd = np.array(
            tdd_grid.reshape((tdd_grid.shape[0],shape[0],shape[1]))\
                [:,m_rows[cell]-1:m_rows[cell]+2,m_cols[cell]-1:m_cols[cell]+2]
        ) # Find kernel of surrounding cells
        
        g_fdd = np.array(
            fdd_grid.reshape((fdd_grid.shape[0],shape[0],shape[1]))\
                [:,m_rows[cell]-1:m_rows[cell]+2,m_cols[cell]-1:m_cols[cell]+2]
        )# Find kernel of surrounding cells

        g_roots = np.array(
            roots_grid.reshape((roots_grid.shape[0],shape[0],shape[1]))\
                [:,m_rows[cell]-1:m_rows[cell]+2,m_cols[cell]-1:m_cols[cell]+2]
        )# Find kernel of surrounding cells
        
    
        g_tdd[g_tdd == -np.inf] = np.nan ## remove extra -infs
        g_fdd[g_fdd == -np.inf] = np.nan
        g_roots[g_roots == -np.inf] = np.nan

        # calc means
        tdd_mean = np.nanmean(g_tdd.reshape(tdd_grid.shape[0],9),axis = 1)
        fdd_mean = np.nanmean(g_fdd.reshape(fdd_grid.shape[0],9),axis = 1)
        roots_mean = np.nanmean(g_roots.reshape(roots_grid.shape[0],9),axis = 1)
        roots_mean = roots_mean.round().astype(int) # covert roots mean to 
        # nearest day


        ## assign days
        tdd_grid[:,f_index] = tdd_mean
        fdd_grid[:,f_index] = fdd_mean

        roots_grid[:,f_index] = roots_mean

        cells.append(f_index)
            
    return cells
        
    
        
        
def create_day_array (dates):
    """Calculates number of days after start day for each date in dates array
    
    Parameters
    ----------
    dates: datetime.datetime
        sorted list of dates
        
    Returns
    -------
    days: list
        list of interger days since first date. The first value will be 0.
        len(days) == len(dates)
    """
    init = dates[0]
    days = []
    for date in dates:
        days.append((date - init).days)
        
    return days

def npmm_to_mg (npmm, rows, cols, ts, cfg={}):
    """convert a numpy memory map file (npmm) to a Temporal grid (mg)
    """
    grid = TemporalGrid(rows, cols, ts)
    grid.config.update(cfg)
    grid.grids[:] = npmm.reshape(ts, rows*cols)[:]
    return grid


    
    
