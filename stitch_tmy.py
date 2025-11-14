#!/usr/bin/env python3
"""Stitch 12 monthly TMY files and reproject to create final TMY dataset.

This script:
1. Loads 12 monthly intermediate files
2. Concatenates them along the time dimension
3. Creates a synthetic continuous time axis (8760 hours, no leap day)
4. Reprojects to EPSG:3338
5. Writes the final TMY NetCDF file

Example usage:
    python stitch_tmy.py --start_year 2010 --end_year 2020
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Dict
import time
import gc

import numpy as np
import pandas as pd
import xarray as xr

from config import data_config, config, tmy_config
from process_single_variable import regrid_to_3338
from utils.dask_utils import get_dask_client, configure_dask_memory
from utils.logging import get_logger, setup_variable_logging

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Stitch 12 monthly TMY files into final TMY dataset"
    )
    parser.add_argument(
        "--start_year",
        type=int,
        default=2010,
        help="Start year used for TMY calculation (default: 2010)"
    )
    parser.add_argument(
        "--end_year",
        type=int,
        default=2020,
        help="End year used for TMY calculation (default: 2020)"
    )
    return parser.parse_args()


def find_monthly_files(start_year: int, end_year: int) -> List[Path]:
    """Find all 12 monthly intermediate files.

    Args:
        start_year: Start year
        end_year: End year

    Returns:
        List of 12 file paths in month order

    Raises:
        FileNotFoundError: If any monthly file is missing
    """
    monthly_files = []

    for month in range(1, 13):
        filename = f"month_{month:02d}_{start_year}_{end_year}.nc"
        filepath = tmy_config.get_intermediate_dir() / filename

        if not filepath.exists():
            raise FileNotFoundError(
                f"Missing monthly file: {filepath}. "
                f"Ensure all 12 monthly jobs completed successfully."
            )

        monthly_files.append(filepath)

    logger.info(f"Found all 12 monthly files in {tmy_config.get_intermediate_dir()}")
    return monthly_files


def load_and_concat_months(monthly_files: List[Path]) -> xr.Dataset:
    """Load and concatenate 12 monthly files.

    Args:
        monthly_files: List of 12 monthly file paths

    Returns:
        Concatenated dataset with all months
    """
    logger.info("Loading 12 monthly files...")

    monthly_datasets = []
    total_hours = 0

    for i, filepath in enumerate(monthly_files, 1):
        logger.info(f"Loading month {i}/12: {filepath.name}")
        ds = xr.open_dataset(filepath)
        monthly_datasets.append(ds)
        total_hours += len(ds.Time)
        logger.info(f"  Month {i} has {len(ds.Time)} timesteps")

    logger.info(f"Total hours across all months: {total_hours}")

    if total_hours != 8760:
        logger.warning(
            f"Expected 8760 hours (365 days), got {total_hours}. "
        )

    logger.info("Concatenating monthly datasets...")
    tmy_ds = xr.concat(monthly_datasets, dim="Time")

    # always sweeping up
    for ds in monthly_datasets:
        ds.close()

    logger.info(f"Concatenated dataset has {len(tmy_ds.Time)} timesteps")
    # could probably stick the source year reference in a different file
    tmy_ds = tmy_ds.drop_vars("source_year")
    return tmy_ds


def create_synthetic_time_axis(ds: xr.Dataset, center_year: int) -> xr.Dataset:
    """Create synthetic continuous time axis for TMY.

    Args:
        ds: Input dataset with concatenated months
        center_year: Year to use for time axis

    Returns:
        Dataset with synthetic time axis
    """
    logger.info(f"Creating synthetic time axis centered at {center_year}")

    # continuous hourly time axis 8760 hours = (365 days * 24 hours, no leap day)
    n_hours = len(ds.Time)
    new_time = pd.date_range(
        f"{center_year}-01-01",
        periods=n_hours,
        freq="h"
    )
    # assign new time coordinates
    ds = ds.assign_coords(Time=new_time)

    logger.info(f"New time axis: {ds.Time[0].values} to {ds.Time[-1].values}")
    return ds


def write_tmy_output(
    ds: xr.Dataset,
    variable: str,
    output_file: Path,
    start_year: int,
    end_year: int,
) -> None:
    """Write final TMY output to NetCDF file.

    Args:
        ds: xarray Dataset to write
        variable: The primary variable name
        output_file: Output file path
        start_year: Start year used for TMY
        end_year: End year used for TMY
    """
    # Ensure output directory exists
    output_file.parent.mkdir(exist_ok=True, parents=True)

    # CF Compliant Attributes
    current_time = time.strftime("%Y-%m-%d %H:%M:%S UTC")
    center_year = (start_year + end_year) // 2

    ds.attrs["Conventions"] = "CF-1.8"
    ds.attrs["title"] = (
        "Typical Meteorological Year (TMY) from WRF-downscaled ERA5, 4km Alaska Albers"
    )
    ds.attrs["institution"] = (
        "Alaska Climate Adaptation Science Center, University of Alaska Fairbanks"
    )
    ds.attrs["source"] = "Hourly WRF-downscaled ERA5"
    ds.attrs["history"] = f"{current_time}: Created using {Path(__file__).name}"
    ds.attrs["comment"] = (
        f"Each grid cell's TMY is composed of 12 p50 (median) months selected independently. "
        f"Each month represents the 50th percentile month based on mean monthly temperature "
        f"across {start_year}-{end_year}."
    )
    ds.attrs["tmy_method"] = "Spatial heterogeneous p50 monthly temperature"
    ds.attrs["tmy_source_years"] = f"{start_year}-{end_year}"
    ds.attrs["tmy_center_year"] = center_year
    ds.attrs["references"] = "Placeholder"

    # Rename Time to time for CF compliance FIRST (before accessing ds.time)
    ds = ds.rename({"Time": "time"})

    # Now we can access ds.time
    ds.attrs["tmy_timesteps"] = len(ds.time)

    # Add variable attributes
    ds[variable].attrs["long_name"] = "Air temperature at 2 meters"
    ds[variable].attrs["standard_name"] = "air_temperature"
    ds[variable].attrs["units"] = "K"
    ds[variable].attrs["description"] = "Hourly temperature from TMY composition"
    ds.coords["time"].attrs["standard_name"] = "time"
    ds.coords["time"].attrs["axis"] = "T"
    ds.coords["time"].attrs["long_name"] = "time"

    # Set spatial dimensions
    ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

    encoding = {var: {"zlib": True, "complevel": 5} for var in ds.data_vars}

    logger.info(f"Writing final TMY output to {output_file}")
    ds.to_netcdf(output_file, engine="h5netcdf", encoding=encoding)
    logger.info(f"Successfully wrote TMY output to {output_file}")


def stitch_tmy(
    start_year: int,
    end_year: int,
) -> Path:
    """Stitch monthly TMY files into final output.

    Args:
        start_year: Start year used for TMY
        end_year: End year used for TMY

    Returns:
        Path to final TMY file
    """
    variable = "T2"
    center_year = (start_year + end_year) // 2

    output_dir = tmy_config.get_tmy_output_dir()
    output_file = output_dir / f"t2_tmy_{start_year}_{end_year}_era5_4km_3338.nc"

    # init the dask
    client = cluster = None

    try:
        # configure the dask
        configure_dask_memory()
        logger.info(f"Creating Dask client with {config.dask.task_type} configuration")
        client, cluster = get_dask_client(config.dask.cores, config.dask.task_type)

        # data in
        monthly_files = find_monthly_files(start_year, end_year)
        tmy_ds = load_and_concat_months(monthly_files)

        # the arrow of time
        tmy_ds = create_synthetic_time_axis(tmy_ds, center_year)

        # Get grid info for reprojection
        # Use the concatenated TMY dataset which has the correct spatial dimensions
        logger.info("Getting grid information for reprojection")
        logger.info("Using spatial dimensions from monthly files")

        # Extract grid info directly from the TMY dataset
        # We already have XLAT, XLONG from the monthly files
        x_coords = tmy_ds.coords["west_east"].values
        y_coords = tmy_ds.coords["south_north"].values

        # Get WRF projection info from geo file
        geo_ds = xr.open_dataset(data_config.geo_file)
        wrf_proj = "+proj=stere +units=m +a=6370000.0 +b=6370000.0 +lat_0=90.0 +lon_0=-152 +lat_ts=64 +nadgrids=@null"
        from pyproj import Proj, Transformer, CRS
        wgs_proj = Proj(proj="latlong", datum="WGS84")
        wgs_to_wrf_transformer = Transformer.from_proj(wgs_proj, wrf_proj)

        # Get domain center
        e, n = wgs_to_wrf_transformer.transform(
            geo_ds.attrs["CEN_LON"], geo_ds.attrs["CEN_LAT"]
        )

        # Grid spacing
        dx = dy = 4000

        # Calculate x, y coordinates for the subset
        nx = len(x_coords)
        ny = len(y_coords)

        # Calculate the starting positions based on the indices
        # The indices tell us the offset from the full domain
        x0 = -(geo_ds.sizes["west_east"] - 1) / 2.0 * dx + e + x_coords[0] * dx
        y0 = -(geo_ds.sizes["south_north"] - 1) / 2.0 * dy + n + y_coords[0] * dy

        # Create coordinate arrays
        x = np.arange(nx) * dx + x0
        y = np.arange(ny) * dy + y0

        wrf_crs = CRS.from_proj4(wrf_proj)
        grid_info = {"x": x, "y": y, "wrf_crs": wrf_crs}

        geo_ds.close()

        # Reproject to EPSG:3338
        logger.info("Reprojecting TMY data to EPSG:3338")
        tmy_3338 = regrid_to_3338(tmy_ds, grid_info)

        # Compute eagerly
        logger.info("Computing reprojection (this may take a while)...")
        tmy_3338 = tmy_3338.compute()

        # Clean up
        del tmy_ds
        client.run(lambda: gc.collect())

        # Write output
        write_tmy_output(
            tmy_3338,
            variable,
            output_file,
            start_year,
            end_year,
        )

        del tmy_3338
        client.run(lambda: gc.collect())

        logger.info(f"Successfully created final TMY file: {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error stitching TMY: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    finally:
        if client is not None:
            try:
                client.close()
            except:
                pass
        if cluster is not None:
            try:
                cluster.close()
            except:
                pass


def main() -> None:
    """Main processing function."""
    args = parse_args()

    # Set up logging
    setup_variable_logging(
        variable="tmy_stitch",
        base_dir=Path.cwd(),
        console_only=True
    )

    try:
        result = stitch_tmy(
            start_year=args.start_year,
            end_year=args.end_year,
        )

        logger.info(f"Successfully stitched TMY for years {args.start_year}-{args.end_year}")
        logger.info(f"Output written to: {result}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Failed to stitch TMY: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
