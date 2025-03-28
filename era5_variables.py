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
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean sea level pressure (hPa)",
    },
    "ctt_mean": {
        "var_id": "ctt",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean cloud top temperature (degC)",
    },
    "ctt_min": {
        "var_id": "ctt",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum cloud top temperature (degC)",
    },
    "ctt_max": {
        "var_id": "ctt",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum cloud top temperature (degC)",
    },
    "dbz_max": {
        "var_id": "dbz",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum radar reflectivity (dBZ)",
    },
    "rh2_mean": {
        "var_id": "rh2",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 2m relative humidity (%)",
    },
    "rh2_min": {
        "var_id": "rh2",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum 2m relative humidity (%)",
    },
    "rh2_max": {
        "var_id": "rh2",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 2m relative humidity (%)",
    },
    "t2_mean": {
        "var_id": "T2",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean temperature at 2 meters (K)",
    },
    "t2_min": {
        "var_id": "T2",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum temperature at 2 meters (K)",
    },
    "t2_max": {
        "var_id": "T2",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum temperature at 2 meters (K)",
    },
    "q2_mean": {
        "var_id": "Q2",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean water vapor mixing ratio at 2 meters (kg kg-1)",
    },
    "psfc_mean": {
        "var_id": "PSFC",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean surface pressure (Pa)",
    },
    "rh_mean": {
        "var_id": "rh",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean relative humidity (%)",
    },
    "rh_min": {
        "var_id": "rh",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum relative humidity (%)",
    },
    "rh_max": {
        "var_id": "rh",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum relative humidity (%)",
    },
    "temp_mean": {
        "var_id": "temp",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean temperature (K)",
    },
    "temp_min": {
        "var_id": "temp",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum temperature (K)",
    },
    "temp_max": {
        "var_id": "temp",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum temperature (K)",
    },
    "qvapor_mean": {
        "var_id": "QVAPOR",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean water vapor mixing ratio (kg kg-1)",
    },
    "cldfra_mean": {
        "var_id": "CLDFRA",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean cloud fraction",
    },
    "cldfra_max": {
        "var_id": "CLDFRA",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum cloud fraction",
    },
    "wspd10_mean": {
        "var_id": "wspd10",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m wind speed (m s-1)",
    },
    "wspd10_max": {
        "var_id": "wspd10",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 10m wind speed (m s-1)",
    },
    "wdir10_mean": {
        "var_id": "wdir10",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m wind direction (degrees)",
    },
    "u10_mean": {
        "var_id": "u10",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m earth-rotated u-wind component (m s-1)",
    },
    "u10_max": {
        "var_id": "u10",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 10m earth-rotated u-wind component (m s-1)",
    },
    "v10_mean": {
        "var_id": "v10",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean 10m earth-rotated v-wind component (m s-1)",
    },
    "v10_max": {
        "var_id": "v10",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum 10m earth-rotated v-wind component (m s-1)",
    },
    "u_mean": {
        "var_id": "u",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean earth-rotated u-wind component (m s-1)",
    },
    "u_max": {
        "var_id": "u",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum earth-rotated u-wind component (m s-1)",
    },
    "v_mean": {
        "var_id": "v",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean earth-rotated v-wind component (m s-1)",
    },
    "v_max": {
        "var_id": "v",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum earth-rotated v-wind component (m s-1)",
    },
    "w_mean": {
        "var_id": "w",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean destaggered w-wind component (m s-1)",
    },
    "w_max": {
        "var_id": "w",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum destaggered w-wind component (m s-1)",
    },
    "snow_sum": {
        "var_id": "SNOW",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated snow water equivalent (kg m-2)",
    },
    "snowh_mean": {
        "var_id": "SNOWH",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean physical snow depth (m)",
    },
    "snowc_max": {
        "var_id": "SNOWC",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum snow coverage flag (1 for snow cover)",
    },
    "rainnc_sum": {
        "var_id": "rainnc",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated total grid-scale precipitation (mm)",
    },
    "rainc_sum": {
        "var_id": "rainc",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated total cumulus precipitation (mm)",
    },
    "acsnow_sum": {
        "var_id": "acsnow",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated snow (kg m-2)",
    },
    "hfx_sum": {
        "var_id": "HFX",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upward heat flux at the surface (W m-2)",
    },
    "lh_sum": {
        "var_id": "LH",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated latent heat flux at the surface (W m-2)",
    },
    "swdnb_sum": {
        "var_id": "SWDNB",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling shortwave flux at bottom (W m-2)",
    },
    "swdnbc_sum": {
        "var_id": "SWDNBC",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling clear sky shortwave flux at bottom (W m-2)",
    },
    "swupb_sum": {
        "var_id": "SWUPB",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling shortwave flux at bottom (W m-2)",
    },
    "swupbc_sum": {
        "var_id": "SWUPBC",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling clear sky shortwave flux at bottom (W m-2)",
    },
    "lwdnb_sum": {
        "var_id": "LWDNB",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling longwave flux at bottom (W m-2)",
    },
    "lwdnbc_sum": {
        "var_id": "LWDNBC",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated downwelling clear sky longwave flux at bottom (W m-2)",
    },
    "lwupb_sum": {
        "var_id": "LWUPB",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling longwave flux at bottom (W m-2)",
    },
    "lwupbc_sum": {
        "var_id": "LWUPBC",
        "agg_func": lambda x: x.sum(dim="Time"),
        "description": "Daily accumulated upwelling clear sky longwave flux at bottom (W m-2)",
    },
    "twb_mean": {
        "var_id": "twb",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean wet bulb temperature (K)",
    },
    "twb_min": {
        "var_id": "twb",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum wet bulb temperature (K)",
    },
    "twb_max": {
        "var_id": "twb",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum wet bulb temperature (K)",
    },
    "albedo_mean": {
        "var_id": "ALBEDO",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean albedo (-)",
    },
    "smois_mean": {
        "var_id": "SMOIS",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean soil moisture (m3 m-3)",
    },
    "sh2o_mean": {
        "var_id": "SH2O",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean soil liquid water (m3 m-3)",
    },
    "tsk_mean": {
        "var_id": "TSK",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean surface skin temperature (K)",
    },
    "tsk_min": {
        "var_id": "TSK",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum surface skin temperature (K)",
    },
    "tsk_max": {
        "var_id": "TSK",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum surface skin temperature (K)",
    },
    "tslb_mean": {
        "var_id": "TSLB",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean soil temperature (K)",
    },
    "tslb_min": {
        "var_id": "TSLB",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum soil temperature (K)",
    },
    "tslb_max": {
        "var_id": "TSLB",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum soil temperature (K)",
    },
    "seaice_max": {
        "var_id": "SEAICE",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum sea ice flag",
    },
    "sst_mean": {
        "var_id": "SST",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean sea surface temperature (K)",
    },
    "sst_min": {
        "var_id": "SST",
        "agg_func": lambda x: x.min(dim="Time"),
        "description": "Daily minimum sea surface temperature (K)",
    },
    "sst_max": {
        "var_id": "SST",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum sea surface temperature (K)",
    },
    "height_mean": {
        "var_id": "height",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean model height (m)",
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
