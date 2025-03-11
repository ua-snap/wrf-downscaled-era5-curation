"""Configuration settings for ERA5 WRF downscaling data processing pipeline.

This module handles configuration through environment variables with sensible defaults.
Users can override these settings by setting environment variables or by importing
and modifying the config values directly.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from os import getenv
from typing import Any

from era5_variables import era5_datavar_lut


@dataclass
class Config:
    """Configuration settings for the processing pipeline."""

    # Input/Output paths
    INPUT_DIR: Path = Path(getenv("ERA5_INPUT_DIR", "/beegfs/CMIP6/wrf_era5/04km"))
    OUTPUT_DIR: Path = Path(
        getenv(
            "ERA5_OUTPUT_DIR", "/beegfs/CMIP6/cparr4/daily_downscaled_era5_for_rasdaman"
        )
    )

    # Time range settings
    START_YEAR: int = int(getenv("ERA5_START_YEAR", "1960"))
    END_YEAR: int = int(getenv("ERA5_END_YEAR", "2020"))

    DATA_VARS: list[str] = field(default_factory=lambda: _get_data_vars())

    # File patterns and naming
    INPUT_FILE_PATTERN: str = getenv(
        "ERA5_INPUT_PATTERN", "era5_wrf_dscale_4km_{date}.nc"
    )
    OUTPUT_FILE_TEMPLATE: str = getenv(
        "ERA5_OUTPUT_TEMPLATE", "era5_wrf_dscale_4km_{datavar}_{year}_3338.nc"
    )

    # Source for WRF geo_em file
    GEO_EM_FILE: Path = Path(
        getenv("GEO_EM_FILE", "/beegfs/CMIP6/wrf_era5/geo_em.d02.nc")
    )

    # Overwrite existing files or not
    OVERWRITE: bool = bool(getenv("ERA5_OVERWRITE", "False"))

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate_years()
        self._validate_paths()
        self._create_directories()

    def _validate_years(self) -> None:
        """Validate year range configuration."""
        current_year = datetime.now().year

        if not 1950 <= self.START_YEAR <= current_year:
            raise ValueError(f"START_YEAR must be between 1950 and {current_year}")

        if not self.START_YEAR <= self.END_YEAR <= current_year:
            raise ValueError(f"END_YEAR must be between START_YEAR and {current_year}")

    def _validate_paths(self) -> None:
        """Validate path configurations."""
        if not isinstance(self.INPUT_DIR, Path):
            self.INPUT_DIR = Path(self.INPUT_DIR)
        if not isinstance(self.OUTPUT_DIR, Path):
            self.OUTPUT_DIR = Path(self.OUTPUT_DIR)

    def _create_directories(self) -> None:
        """Create output directories if they don't exist."""
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        return {key: getattr(self, key) for key in self.__annotations__}


def _get_data_vars() -> list[str]:
    """Get data variables from environment or default to all available variables."""
    env_vars = getenv("ERA5_DATA_VARS", "")
    if env_vars:
        return [x.strip() for x in env_vars.split(",") if x.strip()]
    return list(era5_datavar_lut.keys())


# Create a global instance of the configuration
config = Config()

# Export the config instance as the primary interface
__all__ = ["config"]
