"""Configuration settings for ERA5 WRF downscaling data processing pipeline.

This module handles configuration through environment variables with sensible defaults.
"""

from dataclasses import dataclass, field
from datetime import datetime
import logging
from os import getenv
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

def _parse_cores_env_var() -> Optional[int]:
    """Parse ERA5_DASK_CORES environment variable with validation.
    
    Returns:
        int: Number of cores if ERA5_DASK_CORES is set and valid
        None: If ERA5_DASK_CORES is not set (triggers auto-detection)
        
    Raises:
        ValueError: If ERA5_DASK_CORES is set but invalid
    """
    cores_str = getenv("ERA5_DASK_CORES")
    if cores_str is None:
        return None
    
    try:
        cores = int(cores_str)
        if cores <= 0:
            raise ValueError(f"ERA5_DASK_CORES must be positive, got: {cores}")
        return cores
    except ValueError as e:
        raise ValueError(f"Invalid ERA5_DASK_CORES value '{cores_str}': {e}")

def _get_data_vars() -> List[str]:
    """Get data variables from environment or default to a subset of available variables."""
    env_vars = getenv("ERA5_DATA_VARS", "")
    if env_vars:
        return [x.strip() for x in env_vars.split(",") if x.strip()]
    else:
        return ["t2_mean", "t2_min", "t2_max"]

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

@dataclass(frozen=True)
class DaskConfig:
    """Configuration for Dask compute settings.
    
    This class handles Dask-specific configuration through environment variables:
        ERA5_DASK_CORES: Number of cores to use (default: auto-detect)
        ERA5_DASK_TASK_TYPE: Task type (default: io_bound)
        
    Memory is automatically detected from SLURM allocation (90% of SLURM_MEM_PER_NODE)
    or defaults to 64GB for non-SLURM environments.
    """
    # ERA5_DASK_CORES environment variable takes precedence over auto-detection.
    # Configuration pathway:
    # 1. If ERA5_DASK_CORES is set: use that value (after validation)
    # 2. If ERA5_DASK_CORES is not set: use None (triggers auto-detection in dask_utils)
    # 3. dask_utils.get_dask_client() handles auto-detection:
    #    - First tries SLURM_CPUS_PER_TASK if in SLURM environment
    #    - Falls back to os.cpu_count() if no SLURM allocation
    cores: Optional[int] = field(
        default_factory=lambda: _parse_cores_env_var()
    )
    task_type: str = field(
        default_factory=lambda: getenv("ERA5_DASK_TASK_TYPE", "io_bound")
    )

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        from utils.dask_utils import VALID_TASK_TYPES
        
        # Validate cores
        if self.cores is not None and self.cores <= 0:
            raise ValueError(f"Cores must be positive, got: {self.cores}")
        
        # Validate task type
        if self.task_type not in VALID_TASK_TYPES:
            raise ValueError(
                f"Invalid task type: {self.task_type}. "
                f"Must be one of: {VALID_TASK_TYPES}"
            )

def _validate_batch_size(batch_size: int) -> None:
    """Validate batch size with performance guidance.
    
    Valid range: 2-365 files (minimum 2 files, maximum 1 year of daily files)
    
    Batch size controls how many files are processed together in memory.
    Performance profiling shows that medium batch sizes (90-180) perform best,
    with 90 files being optimal for most ERA5 workloads. Larger batch sizes 
    (300-365) can create overhead, while smaller batch sizes may be needed 
    for future 3D variables with additional dimensions.
    
    Args:
        batch_size: Number of files to process in each batch
        
    Raises:
        ValueError: If batch size is outside valid range (2-365)
    """
    if not isinstance(batch_size, int):
        raise ValueError(f"Batch size must be an integer, got: {type(batch_size).__name__}")
    
    if batch_size < 2:
        raise ValueError(
            f"Batch size must be at least 2 files, got: {batch_size}. "
            "Minimum batch size ensures efficient processing."
        )
    
    if batch_size > 365:
        raise ValueError(
            f"Batch size cannot exceed 365 files (1 year of daily data), got: {batch_size}. "
            "Large batch sizes can cause memory issues and Dask hangs."
        )

@dataclass
class Config:
    """Configuration settings for the processing pipeline.
    
    This class handles pipeline-wide configuration through environment variables:
        ERA5_START_YEAR: Start year for processing (default: 1960)
        ERA5_END_YEAR: End year for processing (default: 2020)
        ERA5_DATA_VARS: Comma-separated list of variables (default: t2_mean,t2_min,t2_max)
        ERA5_BATCH_SIZE: Number of files to process per batch (default: 90)
    """
    # Time range settings
    START_YEAR: int = int(getenv("ERA5_START_YEAR", "1960"))
    END_YEAR: int = int(getenv("ERA5_END_YEAR", "2020"))
    DATA_VARS: List[str] = field(default_factory=lambda: _get_data_vars())
    
    # Processing settings
    BATCH_SIZE: int = int(getenv("ERA5_BATCH_SIZE", "90"))
    
    # Dask configuration
    dask: DaskConfig = field(default_factory=lambda: DaskConfig())

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate_years()
        _validate_batch_size(self.BATCH_SIZE)

    def _validate_years(self) -> None:
        """Validate year range configuration."""
        current_year = datetime.now().year
        if not 1950 <= self.START_YEAR <= current_year:
            raise ValueError(f"START_YEAR must be between 1950 and {current_year}")
        if not self.START_YEAR <= self.END_YEAR <= current_year:
            raise ValueError(f"END_YEAR must be between START_YEAR and {current_year}")

# Create global instances
# In production, always require environment variables
data_config = DataLocationConfig.from_env(require_env_vars=True)
config = Config()

# Export both config instances as the primary interface
__all__ = ["config", "data_config"]

