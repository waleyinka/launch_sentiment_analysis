"""
Database helper utilities.

This module provides a small wrapper for obtaining Postgres connections.
The pipeline modules use it for consistent connection creation.
"""

from __future__ import annotations

import psycopg2 # type: ignore

from launch_sentiment.include.common.config import DbConfig


def get_connection(cfg: DbConfig):
    """Create a psycopg2 connection.

    Args:
        cfg: Database configuration.

    Returns:
        psycopg2 connection object.
    """
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
