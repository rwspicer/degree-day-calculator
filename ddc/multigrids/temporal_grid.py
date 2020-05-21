from .multigrid import MultiGrid
import numpy as np
import yaml
# import figures
import os

from . import common
from . import clip

class TemporalGrid (MultiGrid):
    """ A class to represent a grid over a fixed period of time,
    Implemented using numpy arrays.

    Parameters
    ----------
    *args: list
        List of required arguments, containing exactly 3 items: # rows, 
        # columns, # time steps.
        Example call: mg = MultiGrid(rows, cols, n_timesteps)
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
        'grid_name_map': map of grid years to their indices.
    grids: TemporalMultiGrid data, np.memmap or np.ndarray  
    current_grids: grids at the current timestep
    """
    
    def __init__ (self, *args, **kwargs):
        """ Class initializer """

        if type(args[0]) is str:
            with open(args[0], 'r') as f:
                self.num_timesteps = yaml.load(f, Loader=yaml.Loader)['num_timesteps']  
            super(TemporalGrid , self).__init__(*args, **kwargs)
        else:
            self.num_timesteps = args[2]
            super(TemporalGrid , self).__init__(*args, **kwargs)
        
        self.config['num_timesteps'] = self.num_timesteps
        self.config['timestep'] = 0
        self.grid = self.grids[0]
        # self.config['start_timestep'] = 0

    def new(self, *args, **kwargs):
        """Does setup for a new TemporalGrid object
        
        Parameters
        ----------
        *args: list
            see MultiGird docs
        **kwargs: dict
            see MultiGird docs and:
                'start_timestep': int # to ues as the start timestep

        Returns
        -------
        Config: dict
            dict to be used as config attribute.
        Grids: np.array like
            array to be used as internal memory.
        """
        config = {}
        ib = common.load_or_use_default(kwargs, 'start_timestep', 0)
        config['start_timestep'] = ib
        kwargs['grid_names'] = [str(i) for i in range(ib, ib + args[2])]
        mg_config, grids = super(TemporalGrid, self).new(*args, **kwargs)
        mg_config.update(config)
        return mg_config, grids

    def __getitem__(self, key): 
        """ Get item function
        
        Parameters
        ----------
        key: str, int, or tuple

        Returns
        -------
        np.array like
        """
        if type(key) in (str,):
            key = self.get_grid_number(key)
        else:
            # print (key)
            key -= self.config['start_timestep']
            # print (key,self.config['start_timestep'])
        return self.grids.reshape(self.config['real_shape'])[key].reshape(self.config['grid_shape'])

    # def get_grids_at_keys(self,keys):
    #     """return the grids for the given keys

    #     Parameters
    #     ----------
    #     keys: list
    #         list of grids
        
    #     Returns
    #     -------
    #     np.array
    #     """
    #     select = np.zeros([len(keys), self.config['grid_shape'][0],self.config['grid_shape'][1] ] )
    #     c = 0
    #     for k in keys:
    #         select[c] = self[k]
    #         c += 1
    #     return select
        

    def get_memory_shape (self,config):
        """ Construct the shape needed for multigrid in memory from 
        configuration. 

        Parameters
        ----------
        config: dict
            Must have key 'grid_shape' a tuple of 2 ints

        Returns
        -------
        Tuple
            (num_timesteps, flattened shape of each grid ) 
        """ 
        return (
            self.num_timesteps, 
            config['grid_shape'][0] * config['grid_shape'][1]
        )

    def get_real_shape (self, config):
        """Construct the shape that represents the real shape of the 
        data for the MultiGird.

        Parameters
        ----------
        config: dict
            Must have key 'grid_shape' a tuple of 2 ints

        Returns
        -------
        Tuple
            (num_timesteps, 'rows', 'cols')
        """
        return (
            self.num_timesteps, 
            config['grid_shape'][0] , config['grid_shape'][1]
        )

    def increment_time_step (self):
        """increment time_step, 
        
        Returns 
        -------
        int 
            year for the new time step
        """
        # if archive_results:
        #     self.write_to_pickle(self.pickle_path)
        self.timestep += 1
        
        if self.timestep >= self.num_timesteps:
            self.timestep -= 1
            msg = 'The timestep could not be incremented, because the ' +\
                'end of the period has been reached.'
            raise common.IncrementTimeStepError(msg)
        self.grids[self.timestep][:] = self.grids[self.timestep-1][:] 
        self.grid = self.grids[self.timestep]
        
        return self.current_timestep()

    def save_clip(self, filename, clip_func=clip.default, clip_args={}):
        """
        """
    
        data = self.grids.reshape(self.config['real_shape'])
      
        try:
            clip_generated = clip_func(filename, data, clip_args)
        except clip.CilpError:
            return False
        return clip_generated
    
    def current_timestep (self):
        """gets current timestep adjused for start_timestep
        
        Returns
        -------
        int
            year of last time step in model
        """
        return self.config['start_timestep'] + self.config['timestep']

    def create_subset(self, subset_grids):
        """creates a multigrid containting only the subset_girds

        parameters
        ----------
        subset_grids: list
        """
        subset = super().create_subset(subset_grids)
        
        subset.config['start_timestep'] = subset_grids[0]
        subset.config['timestep'] = subset_grids[0]
        

        return subset
        