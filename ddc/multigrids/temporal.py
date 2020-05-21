from .multigrid import MultiGrid
import numpy as np
import yaml

from . import common, figures
import matplotlib.pyplot as plt

from . import clip

class TemporalMultiGrid (MultiGrid):
    """ A class to represent a set of multiple related grids of the same 
    dimensions, over a fixed period of time. Implemented using numpy arrays.

    Parameters
    ----------
    *args: list
        List of required arguments, containing exactly 3 items: # rows, 
        # columns, # grids, # time steps.
        Example call: mg = MultiGrid(rows, cols, n_grids, n_timesteps)
    **kwargs: dict
        Dictionary of key word arguments. Most of the valid arguments 
        are defined in the MultiGrid class, New and arguments with a different
        meaning are defined below:
    
    Attributes 
    ----------
    config: dict
         see MultiGrid attributes, and: 
        'grid_shape': 2 tuple, grid shape (rows, cols)
        'real_shape': 3 tuple, (num_grids, rows, cols)
        'memory_shape': 2 tuple, (num_grids, rows * cols)
        'num_timesteps': number of timesteps
        'timestep': the current timestep, for the grids in current_grids
        'start_timestep': the timestep to TemporalMultiGird start at. 
    grids: TemporalMultiGrid data, np.memmap or np.ndarray  
    current_grids: grids at the current timestep
    """
    
    def __init__ (self, *args, **kwargs):
        """ Class initializer """
        if type(args[0]) is str:
            with open(args[0], 'r') as f:
                self.num_timesteps = yaml.load(f, Loader=yaml.Loader)['num_timesteps']  
            super(TemporalMultiGrid , self).__init__(*args, **kwargs)
        else:
            self.num_timesteps = args[3]
            super(TemporalMultiGrid , self).__init__(*args, **kwargs)
            self.config['num_timesteps'] = self.num_timesteps
            self.config['timestep'] = 0
            self.config['start_timestep'] = \
                common.load_or_use_default(kwargs, 'start_timestep', 0)
        self.current_grids = self.grids[0]

    def get_memory_shape (self,config):
        """Construct the shape needed for multigrid in memory from 
        configuration. 

        Parameters
        ----------
        config: dict
            Must have keys 'num_grids' an int, 'grid_shape' a tuple of 2 ints

        Returns
        -------
        Tuple
            (num_grids, flattened shape of each grid ) 
        """
        return (
            self.num_timesteps, config['num_grids'], 
            config['grid_shape'][0] * config['grid_shape'][1]
        )

    def get_real_shape (self, config):
        """Construct the shape that represents the real shape of the 
        data for the MultiGird.

        Parameters
        ----------
        config: dict
            Must have keys 'num_grids' an int, 'grid_shape' a tuple of 2 ints

        Returns
        -------
        Tuple
            ('num_grids', 'rows', 'cols')
        """
        return (
            self.num_timesteps, config['num_grids'], 
            config['grid_shape'][0] , config['grid_shape'][1]
        )

    def __getitem__(self, key): 
        """ Get item function
        
        Parameters
        ----------
        key: str, int, or tuple
            str keys should be grid names
            int keys should be a time step
            tuple keys should be (str, int or slice), where the str is a 
                grid name, and the int or slice are indexes

        Returns
        -------
        np.array like
            if key is str: returns grid 'grid_name' at all time steps
            if key is int: returns all grids at timestep
            if key is tuple: returns grid 'grid_name' at requested time step(s)
        """
        ## this key is used to access the data at the end 
        ## of the function. it is modied based on key
        # print self.grid_name_map
        # print key,  self.config['start_timestep']
        access_key = [slice(None,None) for i in range(3)]
        if common.is_grid(key):
            # print 'a'
            # gets the grid at all timesteps
            access_key[1] = self.get_grid_number(key)
        elif common.is_grid_with_range(key):
            # print 'b'
            access_key[0] = slice(
                key[1].start - self.config['start_timestep'],
                key[1].stop - self.config['start_timestep']
            )
            access_key[1] = self.get_grid_number(key[0])
        elif common.is_grid_with_index(key):
            # print 'c'
            access_key[0] = key[1] - self.config['start_timestep']
            ac_1 = self.get_grid_number(key[0])
            if type(ac_1) is slice:
                ac_1 = ac_1.start
            access_key[1] = ac_1
        elif type(key) is int:
            # print 'd'
            access_key = key - self.config['start_timestep']
        # elif type(key) is slice: TODO NEED some changes to implement this
        #                               but it's not very important to do
        #     access_key = slice(
        #         key.start - self.config['start_timestep'],
        #         key.stop - self.config['start_timestep']
        #     )
        else:
            raise KeyError( 'Not a key for Temporal Multi Grid: '+ str(key))
        # print 'key', access_key, type(access_key)
        # print access_key
        return self.grids.reshape(self.config['real_shape'])[access_key]

    def __setitem__ (self ,key, value):
        """ Set item function

        Sets grids for a given key
        
        Parameters
        ----------
        key: str, int, or tuple
            str keys should be grid names
            int keys should be a time step
            tuple keys should be (str, int or slice), where the str is a 
                grid name, and the int or slice are indexes
        value: np.array like
            shape must match shape of data returned by getitem
            with the same key
        """
        
        access_key = [slice(None,None) for i in range(3)]
        if common.is_grid(key):
            # gets the grid at all timesteps
            # print 'a'
            access_key[1] = self.get_grid_number(key)
        elif common.is_grid_with_range(key):
            # print key
            # print 'b'
            access_key[0] = slice(
                key[1].start - self.config['start_timestep'],
                key[1].stop - self.config['start_timestep']
            )
            access_key[1] = self.get_grid_number(key[0])
        elif common.is_grid_with_index(key):
            # print 'c'
            access_key[0] = key[1] - self.config['start_timestep']
            access_key[1] = self.get_grid_number(key[0])
        elif type(key) is int:
            # print 'd'
            access_key = key - self.config['start_timestep']
        else:
            raise KeyError( 'Not a key for Temporal Multi Grid: '+ str(key))
        
        shape = self.grids[access_key].shape
        if type(value) in (int,float,str,bool):
            value = np.full(shape, value)
        self.grids[access_key] = value.reshape(shape)

    def get_grid(self, grid_id, time_step, flat = True):
        """Get a grid at a time step
        
        Parameters
        ----------
        grid_id: int or str
            if an int, it should be the grid number.
            if a str, it should be a grid name.
        time_step: int
            time step to get grid at
        flat: bool, defaults true
            returns the grid as a flattened array.

        Returns
        -------
        np.array
            1d if flat, 2d otherwise.
        """
        shape = \
            self.config['memory_shape'][-1] \
            if flat else self.config['grid_shape']
        return self.get_grid_over_time(
            grid_id, time_step, time_step+1
        ).reshape(shape)
        
    def get_grid_over_time(self, grid_id, start, stop, flat = True):
        """Get a grid at a time step
        
        Parameters
        ----------
        grid_id: int or str
            if an int, it should be the grid number.
            if a str, it should be a grid name.
        start: int
            start time step to get grid at
        stop: int
            end time step to get grid at
        flat: bool, defaults true
            returns the grid as a flattened array.

        Returns
        -------
        np.array
            2d if flat, 3d otherwise.
        """
        if not start is None:
            start += self.config['start_timestep']
        if not stop is None:
            stop += self.config['start_timestep']
        rows, cols = self.config['grid_shape'][0], self.config['grid_shape'][1]
        if start is None and stop is None:
            shape = (self.num_timesteps, rows, cols )
            if flat:
                shape = (self.num_timesteps, rows* cols )
            # print shape
            return self[grid_id].reshape(shape)
            
        shape = (stop-start, rows, cols)
        if flat:
            shape = (shape[0], rows * cols)
        # print shape
        return self[grid_id, slice(start,stop)].reshape(shape)

    def set_grid(self, grid_id, time_step, new_grid):
        """ Set a grid at a timestep
         Parameters 
        ----------
        grid_id: int or str
            if an int, it should be the grid number.
            if a str, it should be a grid name.
        time_step: int
            time step to get grid at
        new_grid: np.array like
            Grid to set. must be able to reshape to grid_shape.
        """
        time_step += self.config['start_timestep']
        self[grid_id, time_step] = new_grid

    def set_grid_over_time(self, grid_id, start, stop, new_grids):
        """Set a grid at a timestep
         Parameters 
        ----------
        grid_id: int or str
            if an int, it should be the grid number.
            if a str, it should be a grid name.
        time_step: int
            time step to get grid at
        new_grids: np.array like
            Grids to set. shape must match grids being accessed. 
        """
        if not start is None:
            start += self.config['start_timestep']
        if not stop is None:
            stop += self.config['start_timestep']
        self[grid_id, start:stop] = new_grids

    def increment_time_step (self):
        """Increment time_step, for current_girds.
        
        Returns 
        -------
        int 
            year for the new time step
        """
        self.config['timestep'] += 1
        
        if self.config['timestep'] >= self.num_timesteps:
            self.config['timestep'] -= 1
            msg = 'The timestep could not be incremented, because the ' +\
                'end of the period has been reached.'
            raise common.IncrementTimeStepError(msg)
        self.grids[self.config['timestep']][:] = self.grids[self.config['timestep']-1][:] 
        self.current_grids = self.grids[self.config['timestep']]
        
        return self.current_timestep()
    
    def current_timestep (self):
        """gets current timestep adjused for start_timestep
        
        Returns
        -------
        int
            year of last time step in model
        """
        return self.config['start_timestep'] + self.config['timestep']

    def save_figure(
            self, grid_id, ts, filename, figure_func=figures.default, figure_args={}, data=None
        ):
        """
        """
        if data is None:
            data = self[grid_id, ts].astype(float)
        # data[np.logical_not(self.AOI_mask)] = np.nan
        
        if not 'title' in figure_args:
            figure_args['title'] = self.dataset_name 
            if not grid_id is None:
                figure_args['title' ]+= ' ' + str( grid_id )
        fig = figure_func(data, figure_args)
        plt.savefig(filename)
        plt.close()

    def show_figure(self, grid_id, ts, figure_func=figures.default, figure_args={}, data=None):
        """
        """
        if data is None:
            data = self[grid_id, ts].astype(float)
        # data[np.logical_not(self.AOI_mask)] = np.nan
        if not 'title' in figure_args:
            figure_args['title'] = self.config['dataset_name']
            if not grid_id is None:
                figure_args['title'] += ' ' + str( grid_id )
        fig = figure_func(data, figure_args)
        plt.show()
        plt.close()

    def save_clip(
            self, grid_id, filename, clip_func=clip.default, clip_args={}
        ):
        """
        """
        if not grid_id is None:
            data = self[grid_id]
            data = data.reshape(data.shape[0], data.shape[2], data.shape[3])
        else:
            data = None
        try:
            clip_generated = clip_func(filename, data, clip_args)
        except clip.CilpError:
            return False
        return clip_generated

    def get_as_ml_features(self, mask = None, train_range = None):
        """TemporalMultiGrid version of get_as_ml_features,

        Parameters
        ----------
        mask: np.array
            2d array of boolean values where true indicates data to get from
            the 2d array mask is applied to 
        train_range: list like, or None(default)
            [int, int], min an max years to get the features from the temporal 
            multigrid

        """
        features = [ [] for g in range(self.config['num_grids']) ]
        if mask is None:
            mask = self.config['mask']

        # print (mask)
        
        if train_range is None:
            train_range = self.get_range()
        # print (train_range)

        for ts in train_range:
            # print(ts)
            for grid, gnum in self.config['grid_name_map'].items():
                features[gnum] += list(self[grid, ts][mask])

            
        return np.array(features)

    def create_subset(self, subset_grids):
        """creates a multigrid containting only the subset_girds

        parameters
        ----------
        subset_grids: list
        """
        subset = super().create_subset(subset_grids)
        subset.config['start_timestep'] = self.config['start_timestep']
        subset.config['timestep'] = self.config['start_timestep']
        

        return subset
        

    def create_subset(self, subset_grids):
        """creates a multigrid containting only the subset_girds

        parameters
        ----------
        subset_grids: list
        """
        rows = self.config['grid_shape'][0]
        cols = self.config['grid_shape'][1]
        num_ts = self.config['num_timesteps']
        n_grids = len(subset_grids)
        subset = TemporalMultiGrid(rows, cols, n_grids, num_ts,
            grid_names = subset_grids,
            data_type=self.config['data_type'],
            mask = self.config['mask'],
            
        )

        try:
            subset.config['description'] = \
                self.config['description'] + ' Subset.'
        except KeyError:
            subset.config['description'] = 'Unknown subset.'
        
        try:
            subset.config['dataset_name'] = \
                self.config['dataset_name'] + ' Subset.'
        except KeyError:
            subset.config['dataset_name'] = 'Unknown subset.'

        subset.config['start_timestep'] = self.config['start_timestep']
        subset.config['timestep'] = self.config['timestep']
        
        for idx, grid in enumerate(subset_grids):
            subset[grid][:] = self[grid][:]

        return subset


def dumb_test():
    """Dumb unit tests, move to testing framework
    """
    g_names = ['first', 'second']

    init_data = np.ones([3,2,5,10])
    init_data[0,1] += 1
    init_data[1,0] += 2
    init_data[1,1] += 3
    init_data[2,0] += 4
    init_data[2,1] += 5

    t1 = TemporalMultiGrid(5, 10, 2, 3, grid_names = g_names, initial_data = init_data)
    t2 = TemporalMultiGrid(5, 10, 2, 3, grid_names = g_names)

    print( 't1 != t2:', (t1.grids != t2.grids).all() )
    print( 't1 == init_data:', (t1.grids == init_data.reshape(t1.memory_shape)).all() )
    print( 't1 == zeros:', (t2.grids == np.zeros([3,2,5*10])).all() )

    
    return t1
