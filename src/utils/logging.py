"""
Logging configuration and utilities.
"""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional

LOG_DIR = Path("logs")
LOG_FORMAT = "%(__asctime__)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    name: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging with optional file handler.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Optional log file path
        name: Logger name (default: root logger)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        LOG_DIR.mkdir(exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            LOG_DIR / log_file,
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
