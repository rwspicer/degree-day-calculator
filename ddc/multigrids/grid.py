"""
Grid
----

grid.py

This file contains the Grid class

"""
from .multigrid import MultiGrid
from . import common
from . import figures
import numpy as np


class Grid (MultiGrid):
    """
        A class to represent a grid, that inherits from MultiGrid.

    Parameters
    ----------
    *args: list
        List of required arguments, containing exactly 2 items: # rows, 
        # columns
        Example call: g = Grid(rows, cols)
    **kwargs: dict
        Dictionary of key word arguments. Most of the valid arguments 
        are defined in the MultiGrid class, New and arguments with a different
        meaning are defined below:
        'grid_names': does not apply
        'data_model': how to internally represent data, memmap or array.
            Defaults  array.
    
    Attributes 
    ----------
    config: dict
        see MultiGrid attributes, and: 
        'real_shape': 2 tuple, (rows, cols)
        'memory_shape': 1 tuple, (rows * cols)
        'grid_name_map': does not exist for Grid calss
    grids: np.memmap or np.ndarray 
        grid data
    grid: np.memmap or np.ndarray 
        alias of grids
    shape: Tuple (int, int)
        Alias of grid_shape 
    """
    
    def __init__ (self, *args, **kwargs):
        """ Class initializer """
        args = [a for a in args]
        if type(args[0]) is int:
            args.append(1)
            if not 'data_model' in kwargs:
                kwargs['data_model'] = 'array'
        super(Grid , self).__init__(*args, **kwargs)
        
        self.grid = self.grids
        self.shape = self.config['grid_shape']
        try:
            del self.config['grid_name_map']
        except KeyError:
            pass
    
    def get_memory_shape (self, config):
        """Construct the shape needed for multigrid in memory from 
        configuration. 

        Parameters
        ----------
        config: dict
            Must have key, 'grid_shape' a tuple of 2 ints

        Returns
        -------
        Tuple
            ( flattened shape of grid )"""
        return (
            config['grid_shape'][0] * config['grid_shape'][1]
        )
    
    def get_real_shape (self, config):
        """Construct the shape that represents the shape of the Grid.

        Parameters
        ----------
        config: dict
            Must have key 'grid_shape' a tuple of 2 ints

        Returns
        -------
        Tuple
            ('rows', 'cols')
        """
        return (
            config['grid_shape'][0], config['grid_shape'][1]
        )

    def get_grid(self, flat = True):
        """ Get the grid, of flattened grid
        
        Parameters
        ----------

        flat: bool, defaults true
            returns the grid as a flattened array.

        Returns
        -------
        np.array
            1d if flat, 2d otherwise.
        """
        shape = self.config['real_shape'] if not flat else \
            self.config['memory_shape']
        return self[:,:].reshape(shape)

    def __getitem__(self, key): 
        """Get a item in the Grid

        Parameters
        ----------
        key: tuple, or int
            a value that is possible to be used as an index to a numpy array

        Return
        ------
        np.array like, or value of type data_type
        """
        if key is None:
            return self.grid.reshape(self.config['real_shape'])
        return self.grid.reshape(self.config['real_shape'])[key]

    def save_figure (
            self, filename, figure_func=figures.default, figure_args={}
        ):
        """
        """
        super(Grid , self).save_figure(None, filename, figure_func, figure_args)


    def show_figure (self, figure_func=figures.default, figure_args={}):
        """
        """
        super(Grid , self).show_figure(None, figure_func, figure_args)

    # def save_figure (self, filename, **kwargs):
    #     """Save a figure for a grid
        
    #     Parameters
    #     ----------
    #     filename: path
    #         path to save image at
    #     grid_id: int or str
    #         if an int, it should be the grid number.
    #         if a str, it should be a grid name.
    #     **kwargs: dict
    #         dict of key word arguments
    #         'limits': tuple, defaults (None, None)
    #             min, max limits for data
    #         'cmap': str, defaults 'viridis'
    #             matplotlib colormap
    #         'cbar_extend': str, defaults 'neither'
    #             'neither', 'min' or 'max' 
    #     """


        # dtype = common.load_or_use_default(kwargs, 'type', float)
        # data = self.grid.astype(dtype)
        # if dtype is float:
        #     data[np.logical_not(self.mask)] = np.nan

        # figure_name = self.dataset_name 

        # limits = common.load_or_use_default(kwargs, 'limits', (None,None))
        # cmap = common.load_or_use_default(kwargs, 'cmap', 'viridis')
        # cbar_extend = common.load_or_use_default(
        #     kwargs, 'cbar_extend', 'neither'
        # )
        

        # figures.save_figure(
        #     data.reshape(self.config['num_grids']) , 
        #     filename, 
        #     figure_name ,
        #     cmap = cmap,
        #     vmin = limits[0], 
        #     vmax = limits[1],
        #     cbar_extend = cbar_extend
        # )
