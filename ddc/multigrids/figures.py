"""
figures
-------

Functions for generating figures
"""
import matplotlib.pyplot as plt
import numpy as np
import copy

def default(data, new_fig_args):
    """create a categorical plot

    Parameters
    ----------
    data: np.array
        2d array
    new_fig_args: dict
        must contain keys:
            "title": str
    Returns
    -------
    matplotlib.image.AxesImage
    """
    fig_args = {
        "interpolation": 'nearest',
        "cmap": 'viridis',
        "orientation": 'vertical',
        "threshold": 0,
        "cbar_extend": 'neither',
        "vmin": None,
        "vmax": None,
        "shrink": .75,
        "mask": None,
        "show_cb": True,
        }
    fig_args.update(new_fig_args)

    if not fig_args["mask"] is None:
        data[np.logical_not(fig_args["mask"])] = np.nan

    imgplot = plt.imshow(
        data,
        interpolation = fig_args["interpolation"], 
        cmap = fig_args["cmap"], 
        vmin = fig_args["vmin"],
        vmax = fig_args["vmax"]
    )
    if fig_args['show_cb']:
        cb = plt.colorbar(
            # ticks = range(len(fig_args["categories"])), 
            orientation = fig_args["orientation"],
            extend =  fig_args["cbar_extend"],
            shrink = fig_args["shrink"]
        )
    plt.title(fig_args['title'], wrap = True)
    return imgplot

def categorical(data, new_fig_args):
    """create a categorical plot

    Parameters
    ----------
    data: np.array
        2d array
    new_fig_args: dict
        must contain keys:
            "categories": list
            "title": str
        may contain keys:
            "interpolation": str
            "cmap": str,
            "orientation": str
    Returns
    -------
    matplotlib.image.AxesImage
    """
    fig_args = {
        "interpolation": 'nearest',
        "cmap": 'viridis',
        "orientation": 'vertical',
        "shrink": .75,
        "ax_labelsize": 10,
    }
    fig_args.update(new_fig_args)
    imgplot = plt.imshow(
        data, 
        interpolation = fig_args["interpolation"], 
        cmap = plt.cm.get_cmap(fig_args["cmap"], len(fig_args["categories"])), 
        vmin = 0,
        vmax = len(fig_args["categories"])
    )
    plt.title(fig_args["title"], wrap = True)
    cb = plt.colorbar(
        ticks = range(len(fig_args["categories"])), 
        orientation = fig_args["orientation"],
        shrink = fig_args["shrink"],
        # labelsize = 10,
    )
    cb.set_ticklabels(fig_args["categories"])
    plt.clim(-0.5, len(fig_args["categories"]) - .5)
    cb.ax.tick_params(labelsize=fig_args['ax_labelsize'])
    return imgplot

def threshold(data, new_fig_args):
    """
    """
    data = copy.deepcopy(data)
    fig_args = {
        "interpolation": 'nearest',
        "cmap": 'bone',
        "orientation": 'vertical',
        "threshold": 0,
        "cbar_extend": 'neither',
        "shrink": .75,
    }
    fig_args.update(new_fig_args)

    data[data>fig_args["threshold"]] = 1
    imgplot = plt.imshow(
        data, 
        interpolation = fig_args["interpolation"], 
        cmap = fig_args["cmap"], 
    )
    plt.title(fig_args["title"], wrap = True)
    cb = plt.colorbar(
        # ticks = range(len(fig_args["categories"])), 
        orientation = fig_args["orientation"],
        extend =  fig_args["cbar_extend"],
        shrink = fig_args["shrink"]
    )
    # cb.set_ticklabels(fig_args["categories"])
    # plt.clim(-0.5, 2.5)
    return imgplot

def categorical_threshold(data, new_fig_args):
    """
    """
    data = copy.deepcopy(data)
    fig_args = {
        "interpolation": 'nearest',
        "cmap": 'bone',
        "orientation": 'vertical',
        "threshold": 0,
        "cbar_extend": 'neither',
        "shrink": .75,
    }
    fig_args.update(new_fig_args)

    if  len(fig_args["categories"]) != 2:
        raise AttributeError("2, and only 2,  categories must be provided")

    # mask = np.isnan(data)
    data[data>fig_args["threshold"]] = 1
    data[np.logical_not(data>fig_args["threshold"])] = 0
    # data[mask] = np.nan
    imgplot = plt.imshow(
        data, 
        interpolation = fig_args["interpolation"], 
        cmap = plt.cm.get_cmap(fig_args["cmap"], len(fig_args["categories"])), 
        vmin = 0,
        vmax = len(fig_args["categories"])
    )
    plt.title(fig_args["title"], wrap = True)
    cb = plt.colorbar(
        ticks = range(len(fig_args["categories"])), 
        orientation = fig_args["orientation"],
        extend =  fig_args["cbar_extend"],
        shrink = fig_args["shrink"]
    )
    cb.set_ticklabels(fig_args["categories"])
    plt.clim(-0.5, 1.5)
    return imgplot

def save_figure(data, path, title,
        cmap = 'viridis', vmin = 0.0, vmax = 1.0,
        cbar_extend = 'neither'):
    """save the grid as image, with title and color bar
    
    Parameters
    ----------
    data: np.array
        grid to save
    path: path
        path with filename to save file at
    title:
        title to put on image
    cmap: str
        colormap
    vmin: float
        min limit
    vmax: float
        max limit
    cbar_extend: str
       'neither', 'min' or 'max' 
    """
    imgplot = plt.imshow(
        data, 
        interpolation = 'nearest', 
        cmap = cmap, 
        vmin = vmin, 
        vmax = vmax
    )
    plt.title(title, wrap = True)
    plt.colorbar(extend = cbar_extend, shrink = 0.92)
    #~ imgplot.save(path)
    #~ plt.imsave(path, imgplot)
    plt.savefig(path)
    plt.close()
