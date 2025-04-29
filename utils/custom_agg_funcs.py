"""Module to contain any custom aggregation functions."""

import numpy as np


def calc_circular_mean_wind_dir(x):
    """Calculate circular mean of wind direction along time dimension.
    
    This function calculates the circular mean of wind direction data, treating
    the data as angles in degrees. It preserves spatial dimensions while reducing
    along the time dimension. CP note: You may ask, why not use scipy.stats.circmean?
    I experimented with it but it was too difficult to weave into the Dask and xarray context.

    Args:
        x: xarray.DataArray of wind direction data in degrees

    Returns:
        xarray.DataArray of circular mean wind direction
    """
    # convert to radians for circular mean calculation
    x_rad = np.deg2rad(x)
    
    # calculate sin and cos components
    sin_component = np.sin(x_rad).mean(dim="Time")
    cos_component = np.cos(x_rad).mean(dim="Time")
    
    # calculate the circular mean and convert back to degrees
    result = np.rad2deg(np.arctan2(sin_component, cos_component))
    
    # ensure result is in [0, 360) range
    return (result + 360) % 360