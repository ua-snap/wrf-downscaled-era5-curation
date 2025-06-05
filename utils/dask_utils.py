"""Dask utility functions for distributed computing.

This module provides utility functions for setting up and configuring Dask
clusters and clients optimized for different types of workloads (IO-bound vs
compute-bound). It includes functions for calculating optimal worker configurations,
memory allocation, and client creation.
"""

import os
import re
import math
from typing import Tuple, Optional

from dask.distributed import LocalCluster, Client
import dask
from utils.logging import get_logger

# Get a named logger for this module
logger = get_logger(__name__)

# Task type constants
TASK_TYPE_IO = "io_bound"
TASK_TYPE_COMPUTE = "compute_bound"
TASK_TYPE_BALANCED = "balanced"

VALID_TASK_TYPES = [TASK_TYPE_IO, TASK_TYPE_COMPUTE, TASK_TYPE_BALANCED]

def validate_memory_string(memory_limit: str) -> bool:
    """Validate memory string format.
    
    Args:
        memory_limit: Memory limit string to validate
        
    Returns:
        True if format is valid, False otherwise
    """
    if not isinstance(memory_limit, str):
        return False
    pattern = r'^\d+(\.\d+)?(B|KB|MB|GB)$'
    return bool(re.match(pattern, memory_limit.upper()))

def calculate_worker_config(cores: int, task_type: str = "balanced") -> Tuple[int, int]:
    """Calculate optimal worker count and threads per worker based on core count and task type.
    
    Provides different configurations based on the type of processing task:
    - "io_bound": More workers with fewer threads, optimal for file operations
    - "compute_bound": Fewer workers with more threads, better for CPU-intensive tasks
    - "balanced": Balanced approach using sqrt formula (default)
    
    Args:
        cores: Number of available CPU cores
        task_type: Type of task being performed ("io_bound", "compute_bound", or "balanced")
        
    Returns:
        Tuple of (worker_count, threads_per_worker)
    """
    if task_type == "io_bound":
        # More workers with fewer threads for I/O operations
        worker_count = min(cores, 12)  # 12 workers × 2 threads each @ 24 cores
        threads_per_worker = max(2, cores // worker_count)
    elif task_type == "compute_bound":
        # Fewer workers with more threads for computation
        worker_count = max(4, cores // 4)  # At least 4 workers
        threads_per_worker = max(2, cores // worker_count)
    else:  # balanced
        # Square root approach for balanced workloads (default behavior)
        worker_count = max(2, min(8, round(math.sqrt(cores))))
        threads_per_worker = max(1, cores // worker_count)
    
    # Ensure total threads don't exceed core count
    total_threads = worker_count * threads_per_worker
    if total_threads > cores:
        # Adjust threads per worker down if needed
        threads_per_worker = max(1, cores // worker_count)
    
    return worker_count, threads_per_worker


def calculate_worker_memory(total_memory_gb: float, n_workers: int, task_type: str = "balanced") -> str:
    """Calculate appropriate memory per worker based on available memory and task type.
    
    Args:
        total_memory_gb: Total memory available in GB
        n_workers: Number of workers being created
        task_type: Type of task being performed
        
    Returns:
        Memory limit per worker as a string (e.g., "4GB")
    """
    # Minimum reasonable memory per worker
    min_memory_per_worker_gb = 2  # 2GB minimum
    
    # Calculate memory based on task type
    if task_type == "io_bound":
        # I/O tasks typically need less memory
        worker_fraction = 0.5  # Use 50% of available memory
        memory_factor = 0.7    # Each worker gets less memory
    elif task_type == "compute_bound":
        # Compute tasks may need more memory
        worker_fraction = 0.8  # Use 80% of available memory
        memory_factor = 1.3    # Each worker gets more memory
    else:  # balanced
        worker_fraction = 0.65  # Use 65% of available memory
        memory_factor = 1.0    # Standard memory allocation
    
    # Calculate memory per worker with task-specific adjustments
    safe_total_gb = total_memory_gb * worker_fraction
    base_worker_memory_gb = safe_total_gb / n_workers
    adjusted_worker_memory_gb = base_worker_memory_gb * memory_factor
    
    # Ensure we meet minimum memory requirements
    worker_memory_gb = max(min_memory_per_worker_gb, adjusted_worker_memory_gb)
    
    # Return formatted memory string
    return f"{int(worker_memory_gb)}GB"


def get_dask_client(cores: Optional[int] = None, 
                   task_type: str = "io_bound") -> Tuple[Client, LocalCluster]:
    """Set up a Dask LocalCluster and Client optimized for specific task types.
    
    Automatically determines optimal worker count and threads per worker based on
    available resources and the type of task (`io_bound`, `compute_bound`, `balanced`).
    The choice of task type only affects the number of workers and threads, not
    the memory allocation.
    
    Memory is automatically detected from SLURM allocation (using 90% of
    SLURM_MEM_PER_NODE) or defaults to 64GB for non-SLURM environments. Dask
    workers are always allocated 90% of this available memory.
    
    Args:
        cores: Number of cores to use (None for auto-detect)
        task_type: Type of task being performed (default: io_bound)
    
    Returns:
        Tuple of (Client, LocalCluster)
    """
    # Auto-detect memory from SLURM if available
    memory_limit = "64GB"  # Default fallback
    if "SLURM_MEM_PER_NODE" in os.environ:
        slurm_mem_mb = int(os.environ["SLURM_MEM_PER_NODE"])
        slurm_mem_gb = slurm_mem_mb / 1024
        # Use 100% of SLURM allocation as the total available memory
        memory_limit = f"{int(slurm_mem_gb)}GB"
        logger.info(f"Detected memory from SLURM: {memory_limit}")
    else:
        logger.info(f"Using default memory limit: {memory_limit}")
    
    # For SLURM jobs, use the allocated resources
    if "SLURM_JOB_ID" in os.environ:
        # If SLURM_CPUS_PER_TASK is set, use that for the number of cores
        if "SLURM_CPUS_PER_TASK" in os.environ:
            slurm_cores = int(os.environ["SLURM_CPUS_PER_TASK"])
            cores = slurm_cores if cores is None else min(cores, slurm_cores)
            logger.info(f"Using {cores} cores from SLURM allocation")
    
    # If cores is still None, use CPU count
    if cores is None:
        cores = os.cpu_count()
    
    # Calculate worker configuration based on task type
    n_workers, threads_per_worker = calculate_worker_config(cores, task_type)
    
    # Calculate total memory available (from memory_limit string)
    if isinstance(memory_limit, str):
        # Parse memory string like "16GB"
        memory_value = float(re.match(r'(\d+(\.\d+)?)', memory_limit).group(1))
        if "MB" in memory_limit:
            memory_gb = memory_value / 1024
        elif "GB" in memory_limit:
            memory_gb = memory_value
        else:
            # Default to bytes, convert to GB
            memory_gb = float(memory_limit) / (1024 * 1024 * 1024)
    else:
        memory_gb = float(memory_limit) / (1024 * 1024 * 1024)
    
    # Define a single, universal memory fraction for the Dask worker pool
    DASK_MEMORY_FRACTION = 0.90
    worker_pool_memory_gb = memory_gb * DASK_MEMORY_FRACTION
    
    # Calculate memory per worker based on the fixed fraction
    memory_per_worker = f"{int(worker_pool_memory_gb / n_workers)}GB"
    
    # Set up a local cluster with appropriate resources
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_per_worker
    )
    
    logger.info(f"Created Dask LocalCluster [{task_type} mode] with {n_workers} workers, "
                 f"{threads_per_worker} threads per worker, and {memory_per_worker} memory per worker")
    
    # Create a client
    client = Client(cluster)
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    
    return client, cluster


def configure_dask_memory() -> None:
    """Configure Dask memory management settings.
    
    Sets up standard memory thresholds for spilling, target usage, pausing, and termination
    to prevent out-of-memory errors during processing.
    """
    dask.config.set({
        "distributed.worker.memory.spill": 0.85,  # Spill to disk at 85% memory
        "distributed.worker.memory.target": 0.75,  # Target 75% memory usage
        "distributed.worker.memory.pause": 0.95,   # Pause execution at 95% memory
        "distributed.worker.memory.terminate": 0.98,  # Terminate at 98% memory
        "distributed.worker.memory.sizeof.sizeof-recurse-limit": 100 #recursion limit to prevent RecursionError
    }) 
    logger.info("Configured Dask memory management settings") 