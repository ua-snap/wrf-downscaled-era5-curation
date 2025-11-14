#!/usr/bin/env python3
"""Process a single calendar month for TMY (Typical Meteorological Year).

This script processes one calendar month of TMY data by:
1. Loading hourly T2 data for this month across multiple years
2. Computing monthly means for this month only
3. Identifying the p50 (median) year for each grid cell based on those means
4. Extracting hourly data from the p50 year for each grid cell
5. Saving an intermediate monthly file (not reprojected)

This is designed to run in parallel with other months as part of
a SLURM job array. Months make a natural "chunk" by which to distribute the computation.

Example usage:
    # Process January (month 1) for years 2010-2020
    python process_tmy_month.py --month 1 --start_year 2010 --end_year 2020
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional
from random import randrange
import gc

import numpy as np
import xarray as xr

from config import data_config, config, tmy_config
from process_single_variable import get_year_filepaths
from utils.dask_utils import get_dask_client, configure_dask_memory
from utils.logging import get_logger, setup_variable_logging

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Process a single calendar month for TMY"
    )
    parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Calendar month to process (1-12)"
    )
    parser.add_argument(
        "--start_year",
        type=int,
        default=2010,
        help="Start year for TMY calculation (default: 2010)"
    )
    parser.add_argument(
        "--end_year",
        type=int,
        default=2012,
        help="End year for TMY calculation (default: 2012)"
    )

    args = parser.parse_args()
    return args


def get_month_filepaths(calendar_month: int, start_year: int, end_year: int) -> List[Path]:
    """Get all filepaths for a specific calendar month across multiple years.

    Args:
        calendar_month: Month to get (1-12)
        start_year: Starting year
        end_year: Ending year (inclusive)

    Returns:
        List of file paths for this month across all years
    """
    month_fps = []

    for year in range(start_year, end_year + 1):
        year_fps = get_year_filepaths(year)

        for fp in year_fps:
            # Parse filename to get month
            # input file names are strictly: era5_wrf_dscale_4km_YYYY-MM-DD.nc
            filename = fp.name
            if f"-{calendar_month:02d}-" in filename:
                month_fps.append(fp)

    logger.info(
        f"Found {len(month_fps)} daily files for month {calendar_month} "
        f"across years {start_year}-{end_year}"
    )
    return sorted(month_fps)


def read_month_data(filepaths: List[Path]) -> xr.Dataset:
    """Read data for a single variable for a single month across years.

    Args:
        filepaths: List of input file paths for this month

    Returns:
        xarray Dataset with specific variable data
    """
    variable = "T2" # later, make this configurable

    # Only keep essential variables
    keep_vars = [variable, "Time", "south_north", "west_east", "XLONG", "XLAT"]

    # Get list of variables to drop
    with xr.open_dataset(filepaths[0]) as sample_ds:
        all_vars = list(sample_ds.variables)
    drop_vars = [v for v in all_vars if v not in keep_vars]

    # exposing chunking config, but nothing exotic here
    # this is a knob we often gotta fiddle with though
    chunks = {
        "Time": "auto",
        "south_north": -1,
        "west_east": -1,
    }

    logger.info(f"Opening {len(filepaths)} daily files for this month")
    # this combo of engine, chunks, and parallel=on is the way
    ds = xr.open_mfdataset(
        filepaths,
        drop_variables=drop_vars,
        chunks=chunks,
        parallel=True,
        engine="h5netcdf",
        combine="by_coords",
    )
    # logs for time bookkeeping as we process, gonna be a lotta logging like this
    logger.info(f"Loaded data with shape: {ds[variable].shape}")
    logger.info(f"Input data dims are: {ds[variable].dims}")
    first  = ds["Time"].isel(Time=0)
    middle = ds["Time"].isel(Time=len(ds["Time"]) // 2)
    random = ds["Time"].isel(Time=randrange(len(ds["Time"])))
    last   = ds["Time"].isel(Time=-1)
    logger.info("First source data timestamp:  %s", first.values)
    logger.info("Middle source data timestamp: %s", middle.values)
    logger.info("Random source data timestamp: %s", random.values)
    logger.info("Last source data timestamp:   %s", last.values)
    # this will be a hourly time series of the same calendar months, e.g. Januaries from 2010-2012
    return ds


def compute_month_means(ds: xr.Dataset, calendar_month: int, variable: str = "T2") -> xr.Dataset:
    """Compute monthly mean values for the calendar month.
    This monthly mean is a basis by which the percentile ranks are constructed.

    Args:
        ds: Input dataset with hourly data
        calendar_month: Calendar month (1-12)
        variable: Variable name (default: T2)

    Returns:
        Dataset with monthly means for this month only
    """
    logger.info(f"Computing monthly means for month {calendar_month}...")

    # need variable name to access data array so leaving it configurable for now with `T2` default
    # xarray inside baseball in the logs here via flox
    monthly_means = ds[variable].resample(Time="ME").mean()
    # and this will actually interpolate the months between the occurences of the `calendar_month`
    # evidenced in the logs below
    logger.info(f"Monthly Means is of type {type(monthly_means)}")
    logger.info(f"Data dims are: {monthly_means.dims}")
    logger.info(f"Data shape is: {monthly_means.shape}")
    logger.debug("All timestamps:   %s", monthly_means["Time"].values)

    # Add year and month coordinates
    monthly_means = monthly_means.assign_coords({
        "year": monthly_means["Time"].dt.year,
        "month": monthly_means["Time"].dt.month,
    })
    logger.info(f"Data coordinates are: {monthly_means.coords}")

    monthly_means = monthly_means.where(monthly_means["month"] == calendar_month, drop=True)

    logger.info(f"Computed {len(monthly_means.Time)} monthly means for month {calendar_month}")
    first  = monthly_means["Time"].isel(Time=0)
    middle = monthly_means["Time"].isel(Time=len(monthly_means["Time"]) // 2)
    random = monthly_means["Time"].isel(Time=randrange(len(monthly_means["Time"])))
    last   = monthly_means["Time"].isel(Time=-1)
    logger.info("First monthly mean timestamp:  %s", first.values)
    logger.info("Middle monthly mean timestamp: %s", middle.values)
    logger.info("Random monthly mean timestamp: %s", random.values)
    logger.info("Last monthly mean timestamp:   %s", last.values)

    #return monthly_means
    return monthly_means.to_dataset(name=variable)


def identify_p50_for_month(monthly_means: xr.Dataset, variable: str = "T2") -> xr.DataArray:
    """Identify p50 year for each grid cell for each month.
    Basically construct a map of the median year, by month.

    Args:
        monthly_means: Dataset with monthly mean for specified month only
        variable: Variable name (T2 our default)

    Returns:
        DataArray with shape (south_north, west_east) containing the
        year of the p50 month for each grid cell
    """
    logger.info("Identifying p50 year for each grid cell...")

    month_data = monthly_means[variable]

    # For each grid cell, find the median year
    median_values = month_data.quantile(0.5, dim="Time")

    # Create output array
    p50_years = np.zeros(
        (len(monthly_means.south_north), len(monthly_means.west_east)),
        dtype=np.int16
    )

    # Find which year corresponds to the median value for each grid cell
    # iterating over array indicies not great...could perhaps be vectorized
    # but performance isn't terrible
    for i in range(len(monthly_means.south_north)):
        for j in range(len(monthly_means.west_east)):
            cell_data = month_data.isel(south_north=i, west_east=j)
            median_val = median_values.isel(south_north=i, west_east=j).values

            # Find the time index closest to median
            diff = np.abs(cell_data.values - median_val)
            median_idx = np.argmin(diff)

            # Get the year for that time
            p50_year = int(cell_data["year"].isel(Time=median_idx).values)
            p50_years[i, j] = p50_year

    # Convert to DataArray
    p50_da = xr.DataArray(
        p50_years,
        coords={
            "south_north": monthly_means.south_north,
            "west_east": monthly_means.west_east,
        },
        dims=["south_north", "west_east"],
        name="p50_year"
    )

    unique_years = np.unique(p50_years)
    logger.info(f"p50 years for this month: {sorted(unique_years)}")
    logger.info(f"p50 year map is of type {type(p50_da)}")
    logger.info(f"p50 year map dims are: {p50_da.dims}")
    logger.info(f"p50 year map shape is: {p50_da.shape}")
    return p50_da


def vecextract_month_hourly_data(
    ds: xr.Dataset,
    p50_year_map: xr.DataArray,
    calendar_month: int,
    variable: str = "T2",
) -> xr.Dataset:
    """Extract hourly data for this month based on p50 years.

    For each grid cell, this selects the hourly series from the p50 year
    and assembles a single (Time, south_north, west_east) TMY month.
    """
    logger.info(f"Extracting hourly data for month {calendar_month}...")

    # Log the incoming dataset
    logger.info(f"Source ds[{variable}] loaded with shape: {ds[variable].shape}")
    logger.info(f"Source ds[{variable}] dims are: {ds[variable].dims}")
    first  = ds["Time"].isel(Time=0)
    middle = ds["Time"].isel(Time=len(ds["Time"]) // 2)
    random = ds["Time"].isel(Time=randrange(len(ds["Time"])))
    last   = ds["Time"].isel(Time=-1)
    logger.info("First source data timestamp:  %s", first.values)
    logger.info("Middle source data timestamp: %s", middle.values)
    logger.info("Random source data timestamp: %s", random.values)
    logger.info("Last source data timestamp:   %s", last.values)

    # Get unique p50 years we actually need
    requested_years = np.unique(p50_year_map.values).astype(int)
    requested_years = np.sort(requested_years)
    logger.info(f"Requested p50 years for this month: {requested_years.tolist()}")

    month_ds_list: list[xr.Dataset] = []
    available_years: list[int] = []

    # Load hourly data for this calendar month for each requested year
    for year in requested_years:
        time_slice = f"{year}-{calendar_month:02d}"
        logger.info(f"Selecting Time slice {time_slice}")

        try:
            year_month_data = ds.sel(Time=time_slice)

            if year_month_data.sizes.get("Time", 0) == 0:
                logger.warning(f"No timesteps found for {time_slice}")
                continue

            # Exclude leap day if February per TMY spec (8760 hours total)
            if calendar_month == 2:
                is_leap_day = year_month_data["Time"].dt.day == 29
                year_month_data = year_month_data.where(~is_leap_day, drop=True)

            # Log this year’s subset
            logger.info(
                f"year_month_data[{year}][{variable}] shape: "
                f"{year_month_data[variable].shape}"
            )
            logger.info(
                f"year_month_data[{year}][{variable}] dims: "
                f"{year_month_data[variable].dims}"
            )
            first  = year_month_data["Time"].isel(Time=0)
            middle = year_month_data["Time"].isel(
                Time=len(year_month_data["Time"]) // 2
            )
            random = year_month_data["Time"].isel(
                Time=randrange(len(year_month_data["Time"]))
            )
            last   = year_month_data["Time"].isel(Time=-1)
            logger.info("First %s timestamp:  %s", year, first.values)
            logger.info("Middle %s timestamp: %s", year, middle.values)
            logger.info("Random %s timestamp: %s", year, random.values)
            logger.info("Last %s timestamp:   %s", year, last.values)

            month_ds_list.append(year_month_data)
            available_years.append(year)

        except KeyError:
            logger.warning(f"No data found for {time_slice}")
            continue

    if not month_ds_list:
        raise ValueError(f"No data found for calendar month {calendar_month}")

    # Now we have one dataset per available year for this month
    available_years = np.array(available_years, dtype=int)
    logger.info(f"Available years for this month: {available_years.tolist()}")

    # Turn into DataArrays for the target variable
    da_list: list[xr.DataArray] = [ds_i[variable] for ds_i in month_ds_list]
    for year, da_i in zip(available_years, da_list, strict=False):
        logger.info(
            f"da_list year {year} has shape {da_i.shape} and dims {da_i.dims}"
        )

    # Sanity check: each year should have the same number of hours
    counts = np.array([da_i.sizes["Time"] for da_i in da_list], dtype=int)
    logger.info(f"Hours per year for month {calendar_month}: {counts.tolist()}")

    first_count = int(counts[0])
    if not np.all(counts == first_count):
        raise ValueError(
            f"Inconsistent hour count per year for month {calendar_month}: {counts.tolist()}"
        )

    hours_in_month = first_count
    logger.info(
        f"Each selected year has {hours_in_month} hourly timesteps for month {calendar_month}"
    )

    # even though each year really has different calendar dates
    # for TMY we want a canonical monthly clock: hour 0, 1, 2, …, 743.
    # so just reuse the timestamps from the first year (say 2010-01-01 … 2010-01-31) for all years, just as labels.
    template_time = da_list[0]["Time"]
    logger.info(
        f"template_time length: {template_time.sizes['Time']}, "
        f"first: {template_time.isel(Time=0).values}, "
        f"last: {template_time.isel(Time=-1).values}"
    )

    da_list_canon: list[xr.DataArray] = [
        da_i.assign_coords(Time=template_time) for da_i in da_list
    ]
    for year, da_i in zip(available_years, da_list_canon, strict=False):
        logger.info(
            f"da_list_canon year {year} shape: {da_i.shape}, dims: {da_i.dims}"
        )

    # stack into a 4D array: (source_year, Time, south_north, west_east)
    # basically a `n-number-of-years`-stack of cubes:
    da_year = xr.concat(da_list_canon, dim="source_year")
    da_year = da_year.assign_coords(source_year=("source_year", available_years))
    logger.info(f"da_year shape: {da_year.shape}")
    logger.info(f"da_year dims: {da_year.dims}")
    first  = da_year["Time"].isel(Time=0)
    middle = da_year["Time"].isel(Time=len(da_year["Time"]) // 2)
    random = da_year["Time"].isel(Time=randrange(len(da_year["Time"])))
    last   = da_year["Time"].isel(Time=-1)
    logger.info("First da_year timestamp:  %s", first.values)
    logger.info("Middle da_year timestamp: %s", middle.values)
    logger.info("Random da_year timestamp: %s", random.values)
    logger.info("Last da_year timestamp:   %s", last.values)
    # we want a single canonical Time axis
    da_year = da_year.assign_coords(Time=template_time)

    # we usethe p50 map as a “picker” for each square
    # p50_year_map has dims (south_north, west_east) and values that match 'source_year'
    selected = da_year.sel(source_year=p50_year_map)
    selected = selected.transpose("Time", "south_north", "west_east")
    # so at each pixel (i, j), it says “use year 2010” or “use year 2011” or “use year 2012”, etc.
    # xarray broadcasts that over Time, so for every hour and every grid cell, it picks the slice from the right year’s cube.

    logger.info(f"selected shape: {selected.shape}")
    logger.info(f"selected dims: {selected.dims}")
    first  = selected["Time"].isel(Time=0)
    middle = selected["Time"].isel(Time=len(selected["Time"]) // 2)
    random = selected["Time"].isel(Time=randrange(len(selected["Time"])))
    last   = selected["Time"].isel(Time=-1)
    logger.info("First selected timestamp:  %s", first.values)
    logger.info("Middle selected timestamp: %s", middle.values)
    logger.info("Random selected timestamp: %s", random.values)
    logger.info("Last selected timestamp:   %s", last.values)

    # Build final monthly dataset
    month_ds = xr.Dataset(
        {
            variable: selected
        },
        coords={
            "Time": selected["Time"],
            "south_north": ds.south_north,
            "west_east": ds.west_east,
            "XLAT": ds.XLAT,
            "XLONG": ds.XLONG,
        },
    )
    logger.info(f"month_ds[{variable}] shape: {month_ds[variable].shape}")
    logger.info(f"month_ds[{variable}] dims: {month_ds[variable].dims}")

    month_ds.attrs["calendar_month"] = calendar_month
    month_ds.attrs["p50_years"] = ",".join(map(str, map(int, available_years)))
    month_ds.attrs["hours_in_month"] = hours_in_month

    logger.info(
        f"Extracted {hours_in_month} hourly timesteps for month {calendar_month} "
        f"with shape {month_ds[variable].shape}"
    )
    return month_ds


def process_single_month(
    calendar_month: int,
    start_year: int,
    end_year: int,
    overwrite: bool = False
) -> Path:
    """Process TMY for a single calendar month.

    Args:
        calendar_month: Month to process (1-12)
        start_year: Starting year
        end_year: Ending year
        overwrite: Whether to overwrite existing files

    Returns:
        Path to intermediate monthly file
    """
    variable = "T2"

    tmy_config.get_intermediate_dir().mkdir(parents=True, exist_ok=True)
    output_file = tmy_config.get_intermediate_dir() / f"month_{calendar_month:02d}_{start_year}_{end_year}.nc"

    # initialize dask client variables
    client = cluster = None

    try:
        # configure Dask with "io_bound" default
        configure_dask_memory()
        logger.info(f"Creating Dask client with {config.dask.task_type} configuration")
        client, cluster = get_dask_client(config.dask.cores, config.dask.task_type)

        # data in
        filepaths = get_month_filepaths(calendar_month, start_year, end_year)
        logger.info(f"Reading data for month {calendar_month}")
        ds = read_month_data(filepaths)

        # Compute monthly means for basis of percentiles
        # Non-temperature variables may require a different basis upon which to rank them
        monthly_means = compute_month_means(ds, calendar_month, variable)
        monthly_means = monthly_means.compute()

        # determine p50 year for each grid cell for the given month
        p50_year_map = identify_p50_for_month(monthly_means, variable)
        # Clean up
        del monthly_means
        gc.collect()

        # Extract hourly data
        month_ds = vecextract_month_hourly_data(ds, p50_year_map, calendar_month, variable)
        logger.info("Computing hourly data...")
        month_ds = month_ds.compute()

        # Clean up
        del ds, p50_year_map
        client.run(lambda: gc.collect())

        # Add metadata
        month_ds.attrs["start_year"] = start_year
        month_ds.attrs["end_year"] = end_year

        # Write output
        logger.info(f"Writing intermediate file to {output_file}")
        encoding = {var: {"zlib": True, "complevel": 5} for var in month_ds.data_vars}
        month_ds.to_netcdf(output_file, engine="h5netcdf", encoding=encoding)

        logger.info(f"Successfully wrote {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Error processing month {calendar_month}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    # CP note: in my experience, you can't do enough cleanup with Dask + SLURM (sigh)
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

    # set up logging by borrowing from daily aggregation code
    setup_variable_logging(
        variable=f"tmy_month_{args.month:02d}",
        base_dir=Path.cwd(),
        console_only=True
    )
    logger.info(f"Worker Python script args parsed as {args}")
    try:
        result = process_single_month(
            calendar_month=args.month,
            start_year=args.start_year,
            end_year=args.end_year,
        )

        logger.info(f"Successfully processed month {args.month}")
        logger.info(f"Output written to: {result}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Failed to process month {args.month}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
