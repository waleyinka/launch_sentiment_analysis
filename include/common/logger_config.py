"""
Logging configuration module for the MindFuel quote delivery service.

This module provides a helper to obtain a logger instance configured with
console output and rotating log files for production style observability.
"""

import logging
import logging.handlers
import os
from typing import Optional

from launch_sentiment.include.common.config import LOG_DIRECTORY, LOG_FILE_NAME, LOG_LEVEL


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a configured logger instance.

    The logger writes to both console and a rotating log file. This function
    is safe to call from multiple modules because it will only attach
    handlers once per logger.

    Args:
        name: Name for the logger. If None, uses the root logger.

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handler if logger already configured
    if logger.handlers:
        return logger

    # Set logger level
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Prevent log propagation to root (essential for Airflow)
    logger.propagate = False

    # Create logs directory if it doesn't exist
    os.makedirs(LOG_DIRECTORY, exist_ok=True)
    log_path = os.path.join(LOG_DIRECTORY, LOG_FILE_NAME)

    # Formatter for log messages
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler (rotates daily)
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger