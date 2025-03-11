era5_datavar_lut = {
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
    "snowh_max": {
        "var_id": "SNOWH",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum physical snow depth (m)",
    },
    "snowc_max": {
        "var_id": "SNOWC",
        "agg_func": lambda x: x.max(dim="Time"),
        "description": "Daily maximum snow coverage flag (1 for snow cover)",
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
    "height_mean": {
        "var_id": "height",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean model height (m)",
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
    "qvapor_mean": {
        "var_id": "QVAPOR",
        "agg_func": lambda x: x.mean(dim="Time"),
        "description": "Daily mean water vapor mixing ratio (kg kg-1)",
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
}
