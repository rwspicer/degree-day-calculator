"""
Sort Functions
--------------

Alternate sort functions

"""
import os

MONTHS = ['{:02d}'.format(m) for m in range(1,13) ]          

def sort_snap_files (files):
    """Sorts snap geotiff files by year then month.

    Parameters
    ----------
    files: list
        unordered list of files
    
    Returns
    -------
    ordered list of files
    """

    files = sorted(files)

    year = int(os.path.split(files[0])[1][-8:-4])

    sorted_files = []
    while len(sorted_files) < len(files):
        for month in MONTHS:
            for file in files:
                if file.find(month + '_' + str(year)) != -1:
                    sorted_files.append(file)
                    break
        year += 1
    return sorted_files
