import glob
import os

import xarray as xr
import numpy as np
import pandas as pd
from pyproj import Transformer

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.colors import TwoSlopeNorm
from matplotlib.dates import MonthLocator, DateFormatter
import matplotlib.dates as mdates


def point_locations_to_test(lat_lon_di):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3338", always_xy=True)
    locations = {name: transformer.transform(lon, lat) for name, (lat, lon) in lat_lon_di.items()}
    return locations


def list_files_for_variable(var_id):
    data_dir = f"/beegfs/CMIP6/cparr4/daily_downscaled_era5_for_rasdaman/{var_id}"
    file_pattern = os.path.join(data_dir, f"{var_id}_*_era5_4km_3338.nc")
    file_list = sorted(glob.glob(file_pattern))
    return file_list


def load_all_data_for_variable(file_list):
    ds = xr.open_mfdataset(file_list,
                           combine='by_coords',
                           chunks={'time': 366},
                           engine="h5netcdf",
                           parallel=True)
    return ds


def compute_annual_mean(ds, pt_extract=None):
    if pt_extract:
        annual_mean = ds.groupby('time.year').mean('time', keep_attrs=True)
    else:
        annual_mean = ds.groupby('time.year').mean()   
    return annual_mean


def compute_annual_sum(ds, pt_extract=None):
    if pt_extract:
        annual_sum = ds.groupby('time.year').sum('time', keep_attrs=True)
    else:
        annual_sum = ds.groupby('time.year').sum()   
    return annual_sum


def compute_annual_min(ds, pt_extract=None):
    if pt_extract:
        annual_min = ds.groupby('time.year').min('time', keep_attrs=True)
    else:
        annual_min = ds.groupby('time.year').min()   
    return annual_min


def compute_annual_max(ds, pt_extract=None):
    if pt_extract:
        annual_max = ds.groupby('time.year').max('time', keep_attrs=True)
    else:
        annual_max = ds.groupby('time.year').max()   
    return annual_max


def extract_data_for_points(locations, ds, var_id):
    
    x_coords = np.array([x for x, y in locations.values()])
    y_coords = np.array([y for x, y in locations.values()])
    names = list(locations.keys())
    
    # Create DataArrays with point dimension preserved
    points_ds = ds.sel(
        x=xr.DataArray(x_coords, dims='point', coords={'point': names}),
        y=xr.DataArray(y_coords, dims='point', coords={'point': names}),
        method='nearest',
        tolerance=5000
    )[var_id]

    if "sum" in var_id:
        annual_totals = compute_annual_sum(points_ds, pt_extract=True)
        tag = "Total Annual"
    elif "rain" in var_id:
        annual_totals = compute_annual_sum(points_ds, pt_extract=True)
        tag = "Total Annual"
    elif "min" in var_id:
        annual_totals = compute_annual_mean(points_ds, pt_extract=True)
        tag = "Mean Annual Daily Minimum"
    elif "max" in var_id:
        annual_totals = compute_annual_mean(points_ds, pt_extract=True)
        tag = "Mean Annual Daily Maximum"
    else:
        annual_totals = compute_annual_mean(points_ds, pt_extract=True)
        tag = "Mean Annual"
    
    result_df = annual_totals.to_dataframe().reset_index()
    result_df = result_df.pivot(index='year', columns='point', values=var_id)
    result_df["year"] = result_df.index.values
    return result_df, tag


def plot_point_extraction_time_series_small_multiples(locations, df, attrs, tag, save=False):

    var_name = attrs["standard_name"].replace("_", " ").title()
    desc = attrs["long_name"]
    unit = attrs["units"]
    
    if unit == "degree_C":
        unit = "°C"
    
    # Create figure with 4 rows and 3 columns
    fig, axes = plt.subplots(nrows=4, ncols=3, figsize=(18, 20), sharex=True, sharey=True)
    fig.suptitle(f"ERA5 4km - {desc}", fontsize=17, y=1.01)
    
    axes = axes.flatten()
    
    # Plot each location in its own subplot
    for idx, (loc, ax) in enumerate(zip(locations, axes)):
        # Skip if we have more axes than locations
        if idx >= len(locations):
            ax.set_visible(False)
            continue
        
        ax.plot(df['year'], df[loc], 
                color='#1f77b4', alpha=0.8, linewidth=1.5)
        
        # Add linear trend line
        x = df['year'].values
        y = df[loc].values
        mask = ~np.isnan(y)
        if sum(mask) > 1:  # Need at least 2 points for trend
            coeffs = np.polyfit(x[mask], y[mask], 1)
            ax.plot(x, np.polyval(coeffs, x), '--', color='#ff7f0e', alpha=0.7)
            
            # Add trend annotation
            trend_text = f"Trend: {10 * coeffs[0]:.2f} {unit} / decade"
            ax.text(0.02, 0.95, trend_text, transform=ax.transAxes,
                   fontsize=10, verticalalignment='top',
                   bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))
        
        ax.set_title(loc, fontsize=12, pad=10)
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add mean value annotation
        mean_temp = f"Mean: {df[loc].mean():.1f} {unit}"
        ax.text(0.02, 0.05, mean_temp, transform=ax.transAxes,
                fontsize=10, verticalalignment='bottom',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))
    
    # Add common labels
    for ax in axes[-3:]:  # Only show xlabel on bottom row
        ax.set_xlabel('Year', fontsize=10)
    
    for ax in axes[::3]:  # Only show ylabel on first column
        ax.set_ylabel(f"{tag} {var_name} {unit}", fontsize=10)
    
    plt.tight_layout()
    if save == True:
        prefix = desc.replace(" ", "_").lower()
        plt.savefig(f"figures/{prefix}_annual_agg_communities.png",
                    dpi=144,
                    bbox_inches='tight')
    plt.show()

def run_point_extraction_figs(lat_lon_locations, var_id, save=False):
    locations = point_locations_to_test(lat_lon_locations)
    fps = list_files_for_variable(var_id)
    ds = load_all_data_for_variable(fps)

    ds_attrs = ds[var_id].attrs
    
    data_extraction, tag = extract_data_for_points(locations, ds, var_id)
    ds.close()
    plot_point_extraction_time_series_small_multiples(locations, data_extraction, ds_attrs, tag, save)


def run_point_extraction_figs_multi(lat_lon_locations: dict, var_ids: list[str]):
    locations = point_locations_to_test(lat_lon_locations)
    data_by_variable = {}
    attrs_by_variable = {}

    for var_id in var_ids:
        print(f"Processing: {var_id}")
        fps = list_files_for_variable(var_id)
        ds, ds_attrs = load_all_data_for_variable(fps)
        data_extraction = extract_data_for_points(locations, ds, var_id)
        ds.close()

        data_by_variable[var_id] = data_extraction
        attrs_by_variable[var_id] = ds_attrs

    plot_point_extraction_time_series_small_multiples_multi(
        locations,
        data_by_variable,
        attrs_by_variable
    )


def plot_point_extraction_time_series_small_multiples_multi(
    locations: dict,
    data_by_variable: dict[str, 'pd.DataFrame'],
    attrs_by_variable: dict[str, dict],
    save: bool = False
):
    import matplotlib.pyplot as plt

    var_ids = list(data_by_variable.keys())
    sample_attrs = attrs_by_variable[var_ids[0]]

    unit = sample_attrs["units"]
    if unit == "degree_C":
        unit = "°C"

    fig, axes = plt.subplots(nrows=4, ncols=3, figsize=(20, 20), sharex=True, sharey=True)
    fig.suptitle(f"ERA5 Trends by Location\n{', '.join(var_ids)}", fontsize=16, y=1.02)
    axes = axes.flatten()

    for idx, (loc, ax) in enumerate(zip(locations, axes)):
        if idx >= len(locations):
            ax.set_visible(False)
            continue

        for var_id in var_ids:
            df = data_by_variable[var_id]
            if loc not in df.columns:
                continue
            ax.plot(df['year'], df[loc], label=var_id.replace("_", " ").title(), linewidth=1.5)

            # Trend line
            x = df['year'].values
            y = df[loc].values
            mask = ~np.isnan(y)
            if sum(mask) > 1:
                coeffs = np.polyfit(x[mask], y[mask], 1)
                ax.plot(x, np.polyval(coeffs, x), '--', alpha=0.5)

        ax.set_title(loc, fontsize=12, pad=10)
        ax.grid(True, alpha=0.3)

    # Label axes
    for ax in axes[-3:]:
        ax.set_xlabel('Year', fontsize=10)
    for ax in axes[::3]:
        ax.set_ylabel(f"Annual ({unit})", fontsize=10)

    # Legend (one shared)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper left', ncol=len(var_ids), fontsize=13)

    plt.tight_layout()
    if save:
        fname = "_".join(var_ids) + "_multipoint_annuals.png"
        plt.savefig(f"figures/annual_time_series/{fname}", dpi=200, bbox_inches='tight')
    plt.show()


def plot_daily_normals_two_periods(
    lat_lon_locations: dict,
    var_id: str,
    period_1: tuple[int, int] = (1960, 1989),
    period_2: tuple[int, int] = (1990, 2019),
    save: bool = False
):
    """Plot daily normals (mean by day-of-year) for two climatology periods at given locations."""

    locations = point_locations_to_test(lat_lon_locations)
    fps = list_files_for_variable(var_id)
    ds = load_all_data_for_variable(fps)

    var = ds[var_id]
    ds_attrs = var.attrs

    def extract_daily_climatology(ds_subset: xr.DataArray, years: tuple[int, int]) -> xr.DataArray:
        """Compute daily climatology over a specified year range."""
        period = ds_subset.sel(time=slice(f"{years[0]}-01-01", f"{years[1]}-12-31"))
        return period.groupby("time.dayofyear").mean("time", keep_attrs=True)

    x_coords = np.array([x for x, y in locations.values()])
    y_coords = np.array([y for x, y in locations.values()])
    names = list(locations.keys())

    points_da = var.sel(
        x=xr.DataArray(x_coords, dims='point', coords={'point': names}),
        y=xr.DataArray(y_coords, dims='point', coords={'point': names}),
        method='nearest',
        tolerance=5000
    )

    clim1 = extract_daily_climatology(points_da, period_1)
    clim2 = extract_daily_climatology(points_da, period_2)

    fig, axes = plt.subplots(nrows=4, ncols=3, figsize=(18, 20), sharex=True, sharey=True)
    axes = axes.flatten()

    unit = ds_attrs["units"]
    if unit == "degree_C":
        unit = "°C"

    var_name = ds_attrs.get("standard_name", var_id).replace("_", " ").title()
    desc = ds_attrs.get("long_name", var_id)


    fig.suptitle(f"Daily Normals: {desc} ({unit})\n{period_1[0]}–{period_1[1]} vs {period_2[0]}–{period_2[1]}", 
                 fontsize=17, y=1.02)

    for idx, (loc, ax) in enumerate(zip(locations, axes)):
        if idx >= len(locations):
            ax.set_visible(False)
            continue

        ax.plot(clim1["dayofyear"], clim1.sel(point=loc), label=f"{period_1[0]}–{period_1[1]}", color="#1f77b4")
        ax.plot(clim2["dayofyear"], clim2.sel(point=loc), label=f"{period_2[0]}–{period_2[1]}", color="#d62728")

        ax.set_title(loc, fontsize=12)
        ax.grid(True, alpha=0.3)

    # Labels
    for ax in axes[-3:]:
        ax.set_xlabel("Day of Year", fontsize=10)
    for ax in axes[::3]:
        ax.set_ylabel(f"{var_name} ({unit})", fontsize=10)

    # Legend
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper left', fontsize=13)

    plt.tight_layout()
    if save:
        fname = f"{var_id}_daily_normals_{period_1[0]}_{period_2[1]}.png"
        plt.savefig(f"figures/{fname}", dpi=200, bbox_inches='tight')
    plt.show()

    ds.close()
