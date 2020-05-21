
"""
"""
from .__metadata__ import __version__

from .multigrid import MultiGrid
from .temporal_grid import TemporalGrid
from .temporal import TemporalMultiGrid
from .grid import Grid

import pprint

class MultigridConfig(dict):
    """Multigrid config class. This class works like a dictionary, but 
    sets some defult keys. Also enforces some key values
    


    Parameters
    ----------
    """

    def __init__(self, *args, **kwargs):
        """Function Docs 
        Parameters
        ----------
        Returns
        -------
        """
        rows = None
        cols = None
        n_grids = None
        

        self.enforced_values = {
            'data_model': ['memmap', 'array'],
            
        }

        self.update ({
            'dataset': {
                'name': 'Unknown',
                'version': None,
                'description': '',
                'units': 'Unknown',
            },
            'multigrids': {
                'version': __version__, 
                'type': MultiGrid,
            },
            'data_model': self.enforced_values['data_model'][0], 
            'data_type': 'float32',
            
            # 'memory_shape': None, 
            # 'real_shape': None, 

            'mode': 'r+',
            'filename': None,
            'cfg_path': '', 

            'grid_name_map' : {},
            
            'grid_shape': (args[0], args[1]), 
            'num_grids': args[2] if len(args) > 2 else None , 

            'start_timestep': None, 
            'num_timesteps': args[3] if len(args) > 3 else None , 
            'timestep': None, 
            
            'mask': None, 
            
            'raster_metadata': None, 
            
        })

        self.update(kwargs)

    def __repr__ (self):
        # pp = pprint.PrettyPrinter(indent=2)
        return str(pprint.pformat( dict(self.items()) ))

    def __setitem__ (self, key, value):
        """
        """
        if key in self.enforced_values:
            if not value in self.enforced_values[key]:
                raise KeyError('Enforced Value Failure')

        if key.split('_')[0] in ['dataset', 'multigrids']:
            super().__getitem__(key.split('_')[0])[
                key.split('_')[1]
            ] = value

        super().__setitem__(key, value)

    def __getitem__ (self, key):

        if key.split('_')[0] in ['dataset', 'multigrids']:
            return super().__getitem__(key.split('_')[0])[key.split('_')[1]]
        # elif key.split('_')[0] in 'multigrids':
        #     return super().__getitem__(key.split('_')[0])[
        #         key.split('_')[1]
        #     ]

        return super().__getitem__(key)
