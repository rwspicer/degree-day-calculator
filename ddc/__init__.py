__version__ = '2.2.0'



# Some Mac OS nonsense for python>=3.8. Macos uses 'spawn' now instead of
# 'fork' but that causes issues with passing np.memmap objcets. Anyway
# this might be unstable on Mac OS now
from multiprocessing import set_start_method
set_start_method('fork') 
