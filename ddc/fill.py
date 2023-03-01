"""
Fill
----

This contains subutilities and features for filling holes (missing pixels) 
in datasets.
"""
import numpy as np
from progress.bar import Bar
from spicebox import raster




def by_interpolation(
        data, locations, log, func=np.nanmean, reset_locations = False,
        loc_type = 'map', k_size = 1
    ):
    """
    Parameters
    ----------
    data: TemporalGrid
    locations: np.array
        locations to interpolate at
    log: dict like
        logging dict
    reset_locations: bool
        if true reset cells at locations == True to -np.inf before 
        running interpolation
    loc_type: str
        list, or map
    k_size: int, default 1
        size of kernel.
    """
    ## fix missing cells
    if loc_type == 'list': # list of tuple locations or set
        m_rows, m_cols  = np.array(list(locations)).T
    else: # loctype == map
        m_rows, m_cols = np.where(locations == True)   
   
    log['Element Messages'].append(
        'Interpolating missing data pixels using: %s' % func.__name__ 
    )
    if log['verbose'] >= 1:
        print(log['Element Messages'][-1])

    # for grid_type in ['fdd', 'tdd', 'roots']:
    #     loop_data = data[grid_type]

    len_cells = range(len(m_rows))
    loop_data = data
    if reset_locations:
        log['Element Messages'].append(
            "Resetting (to -np.inf) Missing Locations Before Processing..."
        )
        if log['verbose'] >= 1:
            print(log['Element Messages'][-1])

        for cell in len_cells:
            row, col = m_rows[cell], m_cols[cell]
            for year in loop_data.timestep_range():
                ix = year - loop_data.config['start_timestep']
                loop_data.grids[ix].reshape(loop_data.config['grid_shape'])[row, col] = -np.inf

    print('processing')
    with Bar('Processing: %s' % 'grid_type',  max=len(len_cells)) as bar:
        for cell in len_cells:
            # f_index = m_rows[cell] * shape[1] + m_cols[cell]  # 'flat' index of
            # cell location
            row, col = m_rows[cell], m_cols[cell]
            kernel = np.array(
                loop_data[
                    :,
                    row-k_size: row+k_size+1,
                    col-k_size: col+k_size+1
                ]
            )
            
            kernel[np.isinf(kernel)] = np.nan #clean kernel

            # mean = np.nanmean(kernel.reshape(tdd_grid.shape[0],9),axis = 1)
            loop_data[:, row, col] = np.nanmean(kernel, axis = 1).mean(1)
            bar.next()


METHODS = {
    'by-interpolation': by_interpolation,
}


def sub_utility(fdd, tdd, arguments, log = {}):
    """Calculates cells to interpolate and runs fdd and tdd datasets through 
    correction process. 

    Parameters
    ----------
    fdd:
    tdd:
    arguments: dict 
        contains
        --valid-area: path
            path to raster containing integer mask of either aoi, or locations 
            to fix. if mask is not integer data. Nan values are set to 0 and
            others to 1. In other cases all values > 0 are to 1 as well
        --area-type: String
            'exact' if valid area is a precalculated mask with values of 1 were
            interpolation should occurs and 0 otherwise.
            'aoi': valid_area is a mask of the valid aoi, with this argument,
            valid_area and missing data in the first timestep in fdd are used to 
            calculate cells to fix. 
        --hole-fill-method: function to use
            by-interpolation is the only current option uses mean of cells in
            kernel to fill missing value
        --reset-bad-cells: bool
            if true cells are set to np.nan before corrections are applied
        --kernel-size: int, 1
            size of kernel to use in correction process 

    """
    method = METHODS[arguments['--hole-fill-method']]

    
    locations = raster.load_raster(arguments['--valid-area'])[0]

    locations[np.isnan(locations)] = 0
    locations[ locations>1 ]  = 1
    locations = locations.astype(int)
    if arguments['--area-type'] == 'aoi':
        sample = fdd[fdd.config['start_timestep']]
        
        locations = np.logical_and(locations, np.isnan(sample))




    print("Filling Holes in FDD data")
    method(
        fdd, locations, log, 
        func=np.nanmean, 
        reset_locations = arguments['--reset-bad-cells'],
        loc_type = 'map', 
        k_size = arguments['--kernel-size']
    )
    print("Filling Holes in TDD data")
    method(
        tdd, locations, log, 
        func=np.nanmean, 
        reset_locations = arguments['--reset-bad-cells'],
        loc_type = 'map', 
        k_size = arguments['--kernel-size']
    )
