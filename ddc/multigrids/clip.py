"""
figures
-------

Functions for generating figures
"""
import matplotlib.pyplot as plt 
import tempfile
import glob
import os
import shutil

try:
    import moviepy.editor as mpe
except ImportError:
    mpe = None

from . import figures

class CilpError(Exception):
    """Raised for errors in clip generation
    """

def moviepy_installed():
    """checks MoviePy installation 
    
    Retruns 
    -------
    bool: true if MoviePy is installed
    """
    if mpe is None:
        return False
    return True


def default(filename, data, new_clip_args):
    """

    Parameters
    ----------
    data: np.array frames
    """
    clip_args = {
        'frames list': None, # pre created frames
        'progress bar': False,
        'verbose': False,
        'end_ts': None,
    }
    clip_args.update(new_clip_args)

    if not moviepy_installed():
        raise CilpError('MoviePy not installed')

    tempdir = None
    files = []
    if clip_args['frames list'] is None:
        tempdir = tempfile.mkdtemp()
        
        fig_range = range(data.shape[0])
        if clip_args['end_ts']:
           fig_range = range(clip_args['end_ts'])
        for fno in fig_range:
            frame = data[fno]
            fig = figures.default(frame, clip_args['figure_args'])
            fig_path = os.path.join(tempdir, 'tempframe'+str(fno)+'.png')
            plt.savefig(fig_path)
            plt.close()
            files.append(fig_path)
    else:
        files = clip_args['frames list']
        
        
    clip = mpe.ImageSequenceClip(files, fps=5)
    clip.write_videofile(
        filename, 
        # progress_bar = clip_args['progress bar'], 
        verbose = clip_args['verbose']
    )

    if tempdir:
        shutil.rmtree(tempdir)
    return True


        

