import logging

def configure_logging() -> None:
    """Configure logging with DEBUG level and standard format."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    ) 