"""Centralized memory monitoring with 5-second intervals."""

import threading
import time
import psutil
from utils.logging import get_logger

# Get a named logger for this module
logger = get_logger(__name__)

def start():
    """Start memory monitoring."""
    global _monitor_thread, _active
    if _active:
        return
    
    _active = True
    _monitor_thread = threading.Thread(target=_monitor, daemon=True)
    _monitor_thread.start()
    logger.info("Memory monitoring started (5s intervals)")

def stop():
    """Stop memory monitoring."""
    global _active
    _active = False
    logger.info("Memory monitoring stopped")

def _monitor():
    """Monitoring loop."""
    proc = psutil.Process()
    while _active:
        try:
            usage = proc.memory_info().rss / (1024**3)  # GB
            logger.info(f"Memory: {usage:.2f} GB")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Monitoring error: {str(e)}")
            break

_active = False
_monitor_thread = None 