

import numpy as np

class GridSizeMismatchError(Exception):
    """GridSizeMismatchError"""
    pass

class IncrementTimeStepError (Exception):
    """Raised if grid timestep not found"""

load_or_use_default = lambda c, k, d: c[k] if k in c else d

def is_grid(key):
    return type(key) is str

def is_grid_with_range(key):
    return type(key) is tuple and len(key) == 2 and \
        type(key[0]) is str and type(key[1]) is slice

def is_grid_with_index(key):
    return type(key) is tuple and len(key) == 2 and \
        type(key[0]) is str and type(key[1]) is int

def is_grid_list(key):
    if type(key) is tuple:
        return np.array([type(k) is str for k in key]).all()
    return False