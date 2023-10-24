import calc_degree_days as cdd 

from concurrent.futures import ProcessPoolExecutor

from multiprocessing import Process, Lock, active_children, cpu_count 

import numpy as np


def pool_wrapper (args):
    index, monthly_temps, tdd, fdd, roots, method_map, lock, log, ufb = args
    print( "task %s" % index)
    cdd.calc_degree_days_for_cell (
        index, monthly_temps, tdd, fdd, roots, method_map, 
        lock = lock, log=log, use_fallback = ufb
    )



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

    tasks = [
        (
            idx, monthly_temps, tdd, fdd, roots, 
            method_map, w_lock, log, use_fallback
        ) 
    for idx in indices]
        

    with ProcessPoolExecutor(num_process) as exe:
        # perform calculations
        exe.map(pool_wrapper, tasks)


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
