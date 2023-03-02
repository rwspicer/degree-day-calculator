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


