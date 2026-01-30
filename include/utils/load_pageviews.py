# pyright: reportMissingImports=false

"""
Load transformed pageviews into Postgres.

This module loads the transformed CSV output into a Postgres table with an
hour level idempotence strategy.

Idempotence approach:
a unique constraint on (company_name, hour_timestamp)
delete existing rows for that hour before inserting
"""

from __future__ import annotations

import csv
import os

import psycopg2 # type: ignore
from psycopg2.extras import execute_batch # type: ignore

from launch_sentiment.include.common.config import load_db_config
from launch_sentiment.include.common.db import get_connection
from launch_sentiment.include.common.logger_config import get_logger


logger = get_logger(__name__)


def load_data(
    csv_file_path: str,
    target_hour_timestamp: str,
    table_name: str = "pageviews_hourly",
) -> int:
    """
    Load transformed pageviews data into Postgres.

    This function is idempotent at the hour level:
    - creat table if it does not exist
    - deletes existing rows for the hour
    - inserts new rows

    Args:
        csv_file_path: Path to transformed CSV file.
        target_hour_timestamp: Hour represented by the data (ISO string).
        table_name: Target database table.

    Returns:
        Number of rows inserted.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If the hour timestamp is missing.
        psycopg2.Error: If database operations fail.
    """
        
    # Validate input
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

    if not target_hour_timestamp:
        raise ValueError("target_hour_timestamp is required for load")

    logger.info(
        "Starting load for hour %s", target_hour_timestamp,
    )

    # Read CSV rows
    rows: list[tuple[str, int, str]] = []
    with open(csv_file_path, newline="", encoding="utf-8") as csvfile:
        
        reader = csv.DictReader(csvfile)

        for row in reader:
            rows.append(
                (
                    row["company_name"],
                    int(row["pageviews"]),
                    row["hour_timestamp"],
                )
            )

    if not rows:
        logger.warning("No rows found in CSV. Nothing to load.")
        return 0


    # Database transaction
    db_config = load_db_config()
    conn = get_connection(db_config)

    try:
        with conn:
            with conn.cursor() as cur:

                # Ensure table exists (DDL)
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        company_name TEXT NOT NULL,
                        pageviews INTEGER NOT NULL,
                        hour_timestamp TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        CONSTRAINT {table_name}_uk
                            UNIQUE (company_name, hour_timestamp)
                    );
                """
                cur.execute(create_table_sql)
                
                
                # Delete existing data for this hour
                delete_sql = f"""
                    DELETE FROM {table_name}
                    WHERE hour_timestamp = %s::timestamptz
                """
                cur.execute(delete_sql, (target_hour_timestamp,))
                
                deleted = cur.rowcount

                logger.info(
                    "Deleted %s existing rows for hour %s",
                    deleted,
                    target_hour_timestamp,
                )

                # Insert new rows
                insert_sql = f"""
                    INSERT INTO {table_name}
                    (company_name, pageviews, hour_timestamp)
                    VALUES (%s, %s, %s::timestamptz)
                """
                execute_batch(cur, insert_sql, rows)

                logger.info(
                    "Inserted %s rows into %s",
                    len(rows),
                    table_name,
                )
        
            return len(rows)

    except Exception:
        logger.error(
            "Load failed for hour %s", target_hour_timestamp, exc_info=True,
        )
        raise

    finally:
        conn.close()

    logger.info(
        "Load completed successfully for hour %s (% rows)",
        target_hour_timestamp,
        len(rows)
    )
    
# EOF