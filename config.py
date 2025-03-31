"""Configuration settings for ERA5 WRF downscaling data processing pipeline.

This module handles configuration through environment variables with sensible defaults.
Users can override these settings by setting environment variables or by importing
and modifying the config values directly.
"""

from dataclasses import dataclass, field
from datetime import datetime
import logging
from os import getenv
from pathlib import Path
from typing import Dict, List, Any


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
    START_YEAR: int = int(getenv("ERA5_START_YEAR", "1980"))
    END_YEAR: int = int(getenv("ERA5_END_YEAR", "1990"))

    DATA_VARS: List[str] = field(default_factory=lambda: _get_data_vars())

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

    # Performance optimization mode
    OPTIMIZATION_MODE: str = getenv("ERA5_OPTIMIZATION_MODE", "io_optimized")

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate_years()
        self._validate_paths()
        self._validate_optimization_mode()
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

    def _validate_optimization_mode(self) -> None:
        """Validate optimization mode configuration."""
        valid_modes = ["balanced", "io_optimized", "compute_optimized", "fully_optimized"]
        if self.OPTIMIZATION_MODE not in valid_modes:
            logging.warning(f"Invalid optimization mode: {self.OPTIMIZATION_MODE}")
            logging.warning(f"Using default: io_optimized")
            self.OPTIMIZATION_MODE = "io_optimized"

    def _create_directories(self) -> None:
        """Create output directories if they don't exist."""
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def update_from_args(self, args: Any) -> None:
        """Update configuration from command-line arguments.
        
        Args:
            args: Parsed command-line arguments
        """
        # Only update if value is provided and not None
        if hasattr(args, 'era5_dir') and args.era5_dir is not None:
            self.INPUT_DIR = args.era5_dir
        if hasattr(args, 'output_dir') and args.output_dir is not None:
            self.OUTPUT_DIR = args.output_dir
        if hasattr(args, 'start_year') and args.start_year is not None:
            self.START_YEAR = args.start_year
        if hasattr(args, 'end_year') and args.end_year is not None:
            self.END_YEAR = args.end_year
        if hasattr(args, 'variable') and args.variable is not None:
            self.DATA_VARS = [args.variable]
        elif hasattr(args, 'variables') and args.variables is not None:
            self.DATA_VARS = [v.strip() for v in args.variables.split(",") if v.strip()]
        if hasattr(args, 'optimization_mode') and args.optimization_mode is not None:
            self.OPTIMIZATION_MODE = args.optimization_mode
        # Log the updated configuration
        logging.debug(f"Updated configuration: {self}")


    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {key: getattr(self, key) for key in self.__annotations__}


def _get_data_vars() -> List[str]:
    """Get data variables from environment or default to a subset of available variables."""
    env_vars = getenv("ERA5_DATA_VARS", "")
    if env_vars:
        return [x.strip() for x in env_vars.split(",") if x.strip()]
    else:
        # just a default set of variables
        return [
            "t2_mean", "t2_min", "t2_max",
        ]


# Create a global instance of the configuration
config = Config()

# Export the config instance as the primary interface
__all__ = ["config"]

