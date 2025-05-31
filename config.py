"""Configuration settings for ERA5 WRF downscaling data processing pipeline.

This module handles configuration through environment variables with sensible defaults.
"""

from dataclasses import dataclass, field
from datetime import datetime
import logging
from os import getenv
from pathlib import Path
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

# Using a frozen dataclass to ensure that the config is immutable
@dataclass(frozen=True)
class DataLocationConfig:
    """Configuration for all data paths and patterns."""
    input_dir: Path
    output_dir: Path
    geo_file: Path
    file_pattern: str

    # Default Chinook paths - kept as documentation and development defaults
    DEFAULT_PATHS = {
        "input_dir": "/beegfs/CMIP6/wrf_era5/04km",
        "output_dir": "/beegfs/CMIP6/$USER/daily_downscaled_era5_for_rasdaman",
        "geo_file": "/beegfs/CMIP6/wrf_era5/geo_em.d02.nc"
    }
    # why a classmethod?
    # because we want to be able to create an instance of the class without having to pass in all the arguments
    @classmethod
    def from_env(cls, require_env_vars: bool = True) -> 'DataLocationConfig':
        """Create configuration from environment variables.
        
        Args:
            require_env_vars: If True, raise error when env vars missing.
                            If False, use default Chinook paths.
        """
        # Get paths from environment
        input_dir = getenv("ERA5_INPUT_DIR")
        output_dir = getenv("ERA5_OUTPUT_DIR")
        geo_file = getenv("ERA5_GEO_FILE")

        if require_env_vars:
            missing = []
            if not input_dir:
                missing.append("ERA5_INPUT_DIR")
            if not output_dir:
                missing.append("ERA5_OUTPUT_DIR")
            if not geo_file:
                missing.append("ERA5_GEO_FILE")
            
            if missing:
                raise ValueError(
                    "Missing required environment variables:\n"
                    f"{', '.join(missing)}\n\n"
                    "These must be set before running the pipeline.\n"
                    f"Default paths on Chinook would be:\n"
                    f"ERA5_INPUT_DIR={cls.DEFAULT_PATHS['input_dir']}\n"
                    f"ERA5_OUTPUT_DIR={cls.DEFAULT_PATHS['output_dir']}\n"
                    f"ERA5_GEO_FILE={cls.DEFAULT_PATHS['geo_file']}"
                )
        else:
            logger.warning(
                "Using default Chinook paths - this is not recommended for production!\n"
                "Set ERA5_INPUT_DIR, ERA5_OUTPUT_DIR, and ERA5_GEO_FILE environment "
                "variables to override defaults."
            )
            input_dir = input_dir or cls.DEFAULT_PATHS["input_dir"]
            output_dir = output_dir or cls.DEFAULT_PATHS["output_dir"]
            geo_file = geo_file or cls.DEFAULT_PATHS["geo_file"]

        return cls(
            input_dir=Path(input_dir),
            output_dir=Path(output_dir),
            geo_file=Path(geo_file),
            file_pattern=getenv(
                "ERA5_FILE_PATTERN", 
                "era5_wrf_dscale_4km_{date}.nc"
            )
        )

    def validate(self) -> None:
        """Validate all paths exist and are accessible."""
        if not self.input_dir.exists():
            raise ValueError(f"Input directory does not exist: {self.input_dir}")
        if not self.geo_file.exists():
            raise ValueError(f"Geo file does not exist: {self.geo_file}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_year_dir(self, year: int) -> Path:
        """Get input directory for a specific year."""
        return self.input_dir / str(year)

    def get_output_file(self, variable: str, year: int) -> Path:
        """Get output file path for a variable and year."""
        return self.output_dir / variable / f"{variable}_{year}_daily_era5_4km_3338.nc"

    def get_variable_dir(self, variable: str) -> Path:
        """Get output directory for a specific variable."""
        return self.output_dir / variable

    def validate_output_file(self, variable: str, year: int) -> bool:
        """Check if output file exists and is valid."""
        file = self.get_output_file(variable, year)
        return file.exists() and file.stat().st_size > 0

@dataclass
class Config:
    """Configuration settings for the processing pipeline."""
    # Time range settings
    START_YEAR: int = int(getenv("ERA5_START_YEAR", "1960"))
    END_YEAR: int = int(getenv("ERA5_END_YEAR", "2020"))
    DATA_VARS: List[str] = field(default_factory=lambda: _get_data_vars())

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate_years()

    def _validate_years(self) -> None:
        """Validate year range configuration."""
        current_year = datetime.now().year
        if not 1950 <= self.START_YEAR <= current_year:
            raise ValueError(f"START_YEAR must be between 1950 and {current_year}")
        if not self.START_YEAR <= self.END_YEAR <= current_year:
            raise ValueError(f"END_YEAR must be between START_YEAR and {current_year}")

def _get_data_vars() -> List[str]:
    """Get data variables from environment or default to a subset of available variables."""
    env_vars = getenv("ERA5_DATA_VARS", "")
    if env_vars:
        return [x.strip() for x in env_vars.split(",") if x.strip()]
    else:
        return ["t2_mean", "t2_min", "t2_max"]

# Create global instances
# In production, always require environment variables
data_config = DataLocationConfig.from_env(require_env_vars=True)
config = Config()

# Export both config instances as the primary interface
__all__ = ["config", "data_config"]

