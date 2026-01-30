"""
Logging utilities for LaunchSentiment.

This module provides a consistent logger configuration across the project.
It is designed to work both locally and inside Airflow tasks.

The goal is predictable, structured logs that make debugging pipelines easier.
"""

from __future__ import annotations

import logging
import sys
import os
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Create or return a configured logger.

    The logger writes to stdout to integrate cleanly with Docker logs and Airflow
    task logs.

    Args:
        name: Logger name, typically __name__.
        level: Optional log level override, such as "INFO" or "DEBUG".

    Returns:
        A configured Logger instance.
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handler if logger already configured
    if logger.handlers:
        return logger

    # Set logger level
    log_level = (level or "INFO").upper()
    logger.setLevel(log_level)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.propagate = False
    return logger