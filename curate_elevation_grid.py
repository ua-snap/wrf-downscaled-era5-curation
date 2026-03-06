import time
from pathlib import Path

import xarray as xr

from config import data_config, config
from process_single_variable import get_grid_info, regrid_to_3338

elevation_grid_path = data_config.input_dir / "invar/elevation_4km.nc"
ds = xr.open_dataset(elevation_grid_path)

# doing this identically to the climate data projection to ensure the same grid is used for both
grid_info = get_grid_info(elevation_grid_path, data_config.geo_file)

elevation_regridded = regrid_to_3338(ds.elevation, grid_info)

output_file = data_config.output_dir / "elevation_grid_4km.nc"

output_file.parent.mkdir(exist_ok=True, parents=True)

# CF Compliant Attributes
current_time = time.strftime("%Y-%m-%d %H:%M:%S UTC")
elevation_regridded.attrs["Conventions"] = "CF-1.8"
elevation_regridded.attrs["title"] = (
    f"Elevation Grid (m) WRF-downscaled ERA5, 4km Alaska Albers"
)
elevation_regridded.attrs["institution"] = (
    "Alaska Climate Adaptation Science Center, University of Alaska Fairbanks"
)
elevation_regridded.attrs["history"] = (
    f"{current_time}: Created using {Path(__file__).name}"
)

# Set spatial dimensions
elevation_regridded = elevation_regridded.rio.set_spatial_dims(
    x_dim="x", y_dim="y", inplace=True
)

elevation_regridded.to_netcdf(output_file, engine="h5netcdf")
print(f"Saved regridded elevation grid to: {output_file}")
