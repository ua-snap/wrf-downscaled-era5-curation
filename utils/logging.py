"""Logging utilities.

Will create a log structure like this example:
logs/
├── era5_process
│ ├── rainnc_sum
│ ├── ...next_variable
│ └── ...next_variable
└── submit_era5_jobs

with one *.out log file per variable, per year.
The job submission log is in the submit_era5_jobs directory.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union

def get_logger(name: str) -> logging.Logger:
    """Get a named logger.
    
    Args:
        name: Name for the logger, typically __name__ of the calling module
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def create_log_directory(base_dir: Union[str, Path], variable: Optional[str] = None) -> Path:
    """Create a log directory with the standard structure.
    
    Creates the directory structure: base_dir/logs/[variable]
    If variable is None, only creates base_dir/logs/
    
    Args:
        base_dir: Base directory (usually working directory)
        variable: Variable name for subdirectory (optional)
        
    Returns:
        Path to the created log directory
    """
    base_path = Path(base_dir)
    if variable:
        log_dir = base_path / "logs" / variable
    else:
        log_dir = base_path / "logs"
        
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def get_log_file_path(base_dir: Union[str, Path], variable: str, year: Optional[int] = None) -> Path:
    """Generate standard log file path.
    
    Creates path with structure: base_dir/logs/variable/variable_year.log
    If year is None, creates: base_dir/logs/variable/variable.log
    
    Args:
        base_dir: Base directory (usually working directory)
        variable: Variable name
        year: Year to process (optional)
        
    Returns:
        Path to the log file
    """
    log_dir = create_log_directory(base_dir, variable)
    if year is not None:
        log_file = log_dir / f"{variable}_{year}.log"
    else:
        log_file = log_dir / f"{variable}.log"
    return log_file


def setup_variable_logging(
    variable: str, 
    year: Optional[int] = None, 
    base_dir: Optional[Union[str, Path]] = None,
    verbose: bool = False,
    console: bool = True,
    console_only: bool = False
) -> None:
    """Set up logging specifically for variable processing.
    
    Creates and configures logs using the standard directory structure:
    base_dir/logs/variable/variable_year.log
    
    Handles fallback to console-only logging if file logging fails.
    
    Args:
        variable: Variable name being processed
        year: Year being processed (optional)
        base_dir: Base directory for logs (defaults to current working directory)
        verbose: Whether to use verbose (DEBUG) logging
        console: Whether to output logs to console
        console_only: If True, only log to console (no file logging)
    """
    # Set log level based on verbose flag
    log_level = logging.DEBUG if verbose else logging.INFO

    # Use current working directory if no base_dir provided
    if base_dir is None:
        base_dir = Path.cwd()
    
    if console_only:
        # Console-only logging, no file creation
        configure_logging(level=log_level, log_file=None, console=console)
        # Get a logger to output a confirmation message
        logger = get_logger(__name__)
        logger.info(f"Console-only logging configured for {variable}" + (f" year {year}" if year else ""))
    else:
        try:
            # Create variable-specific log file
            log_file = get_log_file_path(base_dir, variable, year)
            configure_logging(level=log_level, log_file=log_file, console=console)
            # Get a logger to output a confirmation message
            logger = get_logger(__name__)
            logger.info(f"Logging configured for {variable}" + (f" year {year}" if year else ""))
        except Exception as e:
            # Use print since logger may not be configured yet
            print(f"WARNING: Could not configure file logging: {e}")
            print("WARNING: Falling back to console-only logging")
            configure_logging(level=log_level, console=True)


def configure_logging(
    level: int = logging.DEBUG,
    log_file: Optional[Union[str, Path]] = None,
    max_size_mb: int = 1,
    backup_count: int = 3,
    console: bool = True,
    format_str: Optional[str] = None
) -> None:
    """Configure logging with flexible options.
    
    Args:
        level: Logging level (default: DEBUG)
        log_file: Path to log file (optional)
        max_size_mb: Maximum log file size in MB before rotation
        backup_count: Number of backup log files to keep
        console: Whether to log to console
        format_str: Optional custom log format
    """
    handlers = []
    
    # Set up file handler if log_file is provided
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create a rotating file handler
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        handlers.append(file_handler)
    
    # Add console handler if console is True
    if console:
        handlers.append(logging.StreamHandler())
    
    # Use default format if none provided
    if format_str is None:
        format_str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=handlers
    ) 