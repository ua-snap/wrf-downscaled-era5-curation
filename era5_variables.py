"""ERA5 variable definitions and management.

This module provides a lookup table for ERA5 variables and utility functions
for finding variables and validating variable requests. The variables are stored
in a flat dictionary structure for simplified access and processing.
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple

# Flattened variable lookup table - all variables in a single dictionary
# CP note: tried lumping these in categories, but it was unweildy
era5_datavar_lut: Dict[str, Dict[str, Any]] = {
    "slp_mean": {
        "var_id": "slp",
        "short_name": "slp",
        "standard_name": "air_pressure_at_mean_sea_level",
        "units": "hPa",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean sea level pressure",
    },
    "ctt_mean": {
        "var_id": "ctt",
        "short_name": "ctt",
        "standard_name": "cloud_top_temperature",
        "units": "degree_C",
        "agg_func": lambda x: x.mean(dim="Time") - 273.15,
        "description": "Daily mean cloud top temperature",
    },
    "ctt_min": {
        "var_id": "ctt",
        "short_name": "ctt",
        "standard_name": "cloud_top_temperature",
        "units": "degree_C",
        "agg_func": lambda x: x.min(dim="Time") - 273.15,
        "description": "Daily minimum cloud top temperature",
    },
    "ctt_max": {
        "var_id": "ctt",
        "short_name": "ctt",
        "standard_name": "cloud_top_temperature",
        "units": "degree_C",
        "agg_func": lambda x: x.max(dim="Time") - 273.15,
        "description": "Daily maximum cloud top temperature",
    },
    "dbz_max": {
        "var_id": "dbz",
        "short_name": "dbz",
        "standard_name": "equivalent_reflectivity_factor",
        "units": "dBZ",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum radar reflectivity",
    },
    "rh2_mean": {
        "var_id": "rh2",
        "short_name": "rh2m",
        "standard_name": "relative_humidity",
        "units": "%",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 2m relative humidity",
    },
    "rh2_min": {
        "var_id": "rh2",
        "short_name": "rh2m",
        "standard_name": "relative_humidity",
        "units": "%",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum 2m relative humidity",
    },
    "rh2_max": {
        "var_id": "rh2",
        "short_name": "rh2m",
        "standard_name": "relative_humidity",
        "units": "%",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 2m relative humidity",
    },
    "t2_mean": {
        "var_id": "T2",
        "short_name": "t2m",
        "standard_name": "air_temperature",
        "units": "degree_C",
        "agg_func": lambda x: x.mean(dim="Time") - 273.15,
        "description": "Daily mean temperature at 2 meters",
    },
    "t2_min": {
        "var_id": "T2",
        "short_name": "t2m",
        "standard_name": "air_temperature",
        "units": "degree_C",
        "agg_func": lambda x: x.min(dim="Time") - 273.15,
        "description": "Daily minimum temperature at 2 meters",
    },
    "t2_max": {
        "var_id": "T2",
        "short_name": "t2m",
        "standard_name": "air_temperature",
        "units": "degree_C",
        "agg_func": lambda x: x.max(dim="Time") - 273.15,
        "description": "Daily maximum temperature at 2 meters",
    },
    "q2_mean": {
        "var_id": "Q2",
        "short_name": "q2m",
        "standard_name": "specific_humidity",
        "units": "kg kg-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean water vapor mixing ratio at 2 meters",
    },
    "psfc_mean": {
        "var_id": "PSFC",
        "short_name": "psfc",
        "standard_name": "surface_air_pressure",
        "units": "Pa",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean surface pressure",
    },
    "rh_mean": {
        "var_id": "rh",
        "short_name": "rh",
        "standard_name": "relative_humidity",
        "units": "%",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean relative humidity",
    },
    "rh_min": {
        "var_id": "rh",
        "short_name": "rh",
        "standard_name": "relative_humidity",
        "units": "%",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum relative humidity",
    },
    "rh_max": {
        "var_id": "rh",
        "short_name": "rh",
        "standard_name": "relative_humidity",
        "units": "%",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum relative humidity",
    },
    "temp_mean": {
        "var_id": "temp",
        "short_name": "t",
        "standard_name": "air_temperature",
        "units": "K",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean temperature",
    },
    "temp_min": {
        "var_id": "temp",
        "short_name": "t",
        "standard_name": "air_temperature",
        "units": "K",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum temperature",
    },
    "temp_max": {
        "var_id": "temp",
        "short_name": "t",
        "standard_name": "air_temperature",
        "units": "K",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum temperature",
    },
    "qvapor_mean": {
        "var_id": "QVAPOR",
        "short_name": "q",
        "standard_name": "specific_humidity",
        "units": "kg kg-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean water vapor mixing ratio",
    },
    "cldfra_mean": {
        "var_id": "CLDFRA",
        "short_name": "cldfra",
        "standard_name": "cloud_area_fraction",
        "units": "1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean cloud fraction",
    },
    "cldfra_max": {
        "var_id": "CLDFRA",
        "short_name": "cldfra",
        "standard_name": "cloud_area_fraction",
        "units": "1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum cloud fraction",
    },
    "wspd10_mean": {
        "var_id": "wspd10",
        "short_name": "wspd10m",
        "standard_name": "wind_speed",
        "units": "m s-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m wind speed",
    },
    "wspd10_max": {
        "var_id": "wspd10",
        "short_name": "wspd10m",
        "standard_name": "wind_speed",
        "units": "m s-1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 10m wind speed",
    },
    "wdir10_mean": {
        "var_id": "wdir10",
        "short_name": "wdir10m",
        "standard_name": "wind_from_direction",
        "units": "degree",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m wind direction",
    },
    "u10_mean": {
        "var_id": "u10",
        "short_name": "u10m",
        "standard_name": "eastward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m eastward wind component",
    },
    "u10_max": {
        "var_id": "u10",
        "short_name": "u10m",
        "standard_name": "eastward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 10m eastward wind component",
    },
    "v10_mean": {
        "var_id": "v10",
        "short_name": "v10m",
        "standard_name": "northward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m northward wind component",
    },
    "v10_max": {
        "var_id": "v10",
        "short_name": "v10m",
        "standard_name": "northward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 10m northward wind component",
    },
    "u_mean": {
        "var_id": "u",
        "short_name": "u",
        "standard_name": "eastward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean eastward wind component",
    },
    "u_max": {
        "var_id": "u",
        "short_name": "u",
        "standard_name": "eastward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum eastward wind component",
    },
    "v_mean": {
        "var_id": "v",
        "short_name": "v",
        "standard_name": "northward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean northward wind component",
    },
    "v_max": {
        "var_id": "v",
        "short_name": "v",
        "standard_name": "northward_wind",
        "units": "m s-1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum northward wind component",
    },
    "w_mean": {
        "var_id": "w",
        "short_name": "w",
        "standard_name": "upward_air_velocity",
        "units": "m s-1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean vertical wind component",
    },
    "w_max": {
        "var_id": "w",
        "short_name": "w",
        "standard_name": "upward_air_velocity",
        "units": "m s-1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum vertical wind component",
    },
    "snow_sum": {
        "var_id": "SNOW",
        "short_name": "swe",
        "standard_name": "surface_snow_amount",
        "units": "kg m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated snow water equivalent",
    },
    "snowh_mean": {
        "var_id": "SNOWH",
        "short_name": "snowd",
        "standard_name": "surface_snow_thickness",
        "units": "m",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean physical snow depth",
    },
    "snowc_max": {
        "var_id": "SNOWC",
        "short_name": "snowc",
        "standard_name": "surface_snow_area_fraction",
        "units": "1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum snow cover fraction",
    },
    "rainnc_sum": {
        "var_id": "rainnc",
        "short_name": "prnc",
        "standard_name": "precipitation_amount",
        "units": "mm",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated total grid-scale precipitation",
    },
    "rainc_sum": {
        "var_id": "rainc",
        "short_name": "prc",
        "standard_name": "convective_rainfall_amount",
        "units": "mm",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated total cumulus precipitation",
    },
    "acsnow_sum": {
        "var_id": "acsnow",
        "short_name": "prsn",
        "standard_name": "snowfall_amount",
        "units": "kg m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated snow",
    },
    "hfx_sum": {
        "var_id": "HFX",
        "short_name": "hfss",
        "standard_name": "surface_upward_sensible_heat_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upward sensible heat flux at the surface",
    },
    "lh_sum": {
        "var_id": "LH",
        "short_name": "hfls",
        "standard_name": "surface_upward_latent_heat_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated latent heat flux at the surface",
    },
    "swdnb_sum": {
        "var_id": "SWDNB",
        "short_name": "rsds",
        "standard_name": "surface_downwelling_shortwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling shortwave flux at surface",
    },
    "swdnbc_sum": {
        "var_id": "SWDNBC",
        "short_name": "rsdscs",
        "standard_name": "surface_downwelling_clear_sky_shortwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling clear sky shortwave flux at surface",
    },
    "swupb_sum": {
        "var_id": "SWUPB",
        "short_name": "rsus",
        "standard_name": "surface_upwelling_shortwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling shortwave flux at surface",
    },
    "swupbc_sum": {
        "var_id": "SWUPBC",
        "short_name": "rsuscs",
        "standard_name": "surface_upwelling_clear_sky_shortwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling clear sky shortwave flux at surface",
    },
    "lwdnb_sum": {
        "var_id": "LWDNB",
        "short_name": "rlds",
        "standard_name": "surface_downwelling_longwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling longwave flux at surface",
    },
    "lwdnbc_sum": {
        "var_id": "LWDNBC",
        "short_name": "rldscs",
        "standard_name": "surface_downwelling_clear_sky_longwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling clear sky longwave flux at surface",
    },
    "lwupb_sum": {
        "var_id": "LWUPB",
        "short_name": "rlus",
        "standard_name": "surface_upwelling_longwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling longwave flux at surface",
    },
    "lwupbc_sum": {
        "var_id": "LWUPBC",
        "short_name": "rluscs",
        "standard_name": "surface_upwelling_clear_sky_longwave_flux",
        "units": "W m-2",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling clear sky longwave flux at surface",
    },
    "twb_mean": {
        "var_id": "twb",
        "short_name": "twb",
        "standard_name": "wet_bulb_temperature",
        "units": "K",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean wet bulb temperature",
    },
    "twb_min": {
        "var_id": "twb",
        "short_name": "twb",
        "standard_name": "wet_bulb_temperature",
        "units": "K",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum wet bulb temperature",
    },
    "twb_max": {
        "var_id": "twb",
        "short_name": "twb",
        "standard_name": "wet_bulb_temperature",
        "units": "K",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum wet bulb temperature",
    },
    "albedo_mean": {
        "var_id": "ALBEDO",
        "short_name": "al",
        "standard_name": "surface_albedo",
        "units": "1",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean albedo",
    },
    "smois_mean": {
        "var_id": "SMOIS",
        "short_name": "mrsos",
        "standard_name": "volume_fraction_of_water_in_soil_layer",
        "units": "m3 m-3",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean soil moisture",
    },
    "sh2o_mean": {
        "var_id": "SH2O",
        "short_name": "mrso",
        "standard_name": "volume_fraction_of_liquid_water_in_soil_layer",
        "units": "m3 m-3",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean soil liquid water content",
    },
    "tsk_mean": {
        "var_id": "TSK",
        "short_name": "ts",
        "standard_name": "surface_temperature",
        "units": "K",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean surface skin temperature",
    },
    "tsk_min": {
        "var_id": "TSK",
        "short_name": "ts",
        "standard_name": "surface_temperature",
        "units": "K",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum surface skin temperature",
    },
    "tsk_max": {
        "var_id": "TSK",
        "short_name": "ts",
        "standard_name": "surface_temperature",
        "units": "K",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum surface skin temperature",
    },
    "tslb_mean": {
        "var_id": "TSLB",
        "short_name": "tsl",
        "standard_name": "soil_temperature",
        "units": "K",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean soil temperature",
    },
    "tslb_min": {
        "var_id": "TSLB",
        "short_name": "tsl",
        "standard_name": "soil_temperature",
        "units": "K",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum soil temperature",
    },
    "tslb_max": {
        "var_id": "TSLB",
        "short_name": "tsl",
        "standard_name": "soil_temperature",
        "units": "K",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum soil temperature",
    },
    "seaice_max": {
        "var_id": "SEAICE",
        "short_name": "siconc",
        "standard_name": "sea_ice_area_fraction",
        "units": "1",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum sea ice area fraction",
    },
    "sst_mean": {
        "var_id": "SST",
        "short_name": "tos",
        "standard_name": "sea_surface_temperature",
        "units": "K",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean sea surface temperature",
    },
    "sst_min": {
        "var_id": "SST",
        "short_name": "tos",
        "standard_name": "sea_surface_temperature",
        "units": "K",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum sea surface temperature",
    },
    "sst_max": {
        "var_id": "SST",
        "short_name": "tos",
        "standard_name": "sea_surface_temperature",
        "units": "K",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum sea surface temperature",
    },
    "height_mean": {
        "var_id": "height",
        "short_name": "zg",
        "standard_name": "geopotential_height",
        "units": "m",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean geopotential height",
    },
}


def get_variable_info(variable_name: str) -> Optional[Dict[str, Any]]:
    """Get information about a variable.
    
    Args:
        variable_name: The name of the variable to find
        
    Returns:
        Variable information dict or None if not found
    """
    return era5_datavar_lut.get(variable_name)


def list_all_variables() -> List[str]:
    """List all available variables.
    
    Returns:
        List of all variable names
    """
    return list(era5_datavar_lut.keys())


def validate_variable_request(variable_name: str) -> Tuple[bool, str]:
    """Validate a variable request, suggesting alternatives if not found.
    
    Args:
        variable_name: The name of the variable to validate
        
    Returns:
        Tuple of (is_valid, message)
    """
    if not variable_name:
        return False, "No variable name provided"
        
    if variable_name in era5_datavar_lut:
        return True, f"Variable '{variable_name}' found"
    
    # If not found, suggest alternatives
    all_vars = list_all_variables()
    # Find similar variable names
    similar = [v for v in all_vars if variable_name.lower() in v.lower() or v.lower() in variable_name.lower()]
    
    if similar:
        return False, f"Variable '{variable_name}' not found. Did you mean one of: {', '.join(similar)}?"
    else:
        return False, f"Variable '{variable_name}' not found in available variables."


def get_source_variables(variable_names: List[str]) -> Set[str]:
    """Get the set of unique source variables needed for the requested variables.
    
    Args:
        variable_names: List of variable names to process
        
    Returns:
        Set of source variable IDs
    """
    source_vars = set()
    for var_name in variable_names:
        var_info = get_variable_info(var_name)
        if var_info:
            source_vars.add(var_info["var_id"])
    return source_vars
