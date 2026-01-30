"""
Runtime helpers for LaunchSentiment.

This module owns:
URL construction for a given Airflow logical_date
run-specific local file paths
thin task wrappers used by the DAG

Design intent:
Keep the DAG file clean and declarative.
Keep orchestration glue and runtime derivations in one place.
"""

from __future__ import annotations

from pathlib import Path

import pendulum

from launch_sentiment.include.common.config import load_app_config
from launch_sentiment.include.utils.fetch_pageviews import fetch_data
from launch_sentiment.include.utils.extract_pageviews import extract_data
from launch_sentiment.include.utils.transform_pageviews import transform_data
from launch_sentiment.include.utils.load_pageviews import load_data


def build_wikimedia_url(logical_date_str: str) -> str:
    """Build the Wikimedia download URL for a given Airflow logical date.

    Args:
        logical_date_str: Airflow logical date as an ISO string.

    Returns:
        Full URL to the hourly pageviews gzip file.
    """
    cfg = load_app_config()
    dt = pendulum.parse(logical_date_str)

    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    yyyymmdd = f"{dt.year:04d}{dt.month:02d}{dt.day:02d}"
    hh = f"{dt.hour:02d}"
    filename = f"pageviews-{yyyymmdd}-{hh}0000.gz"

    return f"{cfg.pageviews_base_url}/{year}/{year}-{month}/{filename}"


def build_paths(logical_date_str: str) -> dict[str, str]:
    """Build run-specific file paths for raw, extracted, and transformed artifacts.

    Args:
        logical_date_str: Airflow logical date as an ISO string.

    Returns:
        Dictionary containing:
        gzip_path: raw download path
        txt_path: extracted text path
        csv_path: transformed csv path
        hour_iso: ISO timestamp representing the hour
        filename: the Wikimedia dump filename
    """
    cfg = load_app_config()
    dt = pendulum.parse(logical_date_str)

    yyyymmdd = f"{dt.year:04d}{dt.month:02d}{dt.day:02d}"
    hh = f"{dt.hour:02d}"
    filename = f"pageviews-{yyyymmdd}-{hh}0000.gz"

    base = Path(cfg.local_data_dir)
    gzip_path = base / "raw" / filename
    txt_path = base / "extracted" / filename.replace(".gz", ".txt")
    csv_path = base / "transformed" / f"pageviews_hourly_{dt.format('YYYYMMDD_HH')}.csv"

    return {
        "gzip_path": str(gzip_path),
        "txt_path": str(txt_path),
        "csv_path": str(csv_path),
        "hour_iso": dt.to_iso8601_string(),
        "filename": filename,
    }


def task_fetch(logical_date_str: str) -> str:
    """Download the hourly gzip dump and return its local path.

    Args:
        logical_date_str: Airflow logical date as an ISO string.

    Returns:
        Local path to the downloaded gzip file.
    """
    url = build_wikimedia_url(logical_date_str)
    paths = build_paths(logical_date_str)
    return fetch_data(url=url, output_path=paths["gzip_path"])


def task_extract(logical_date_str: str) -> str:
    """Extract gzip to plain text and return the extracted file path.

    Args:
        logical_date_str: Airflow logical date as an ISO string.

    Returns:
        Local path to the extracted text file.
    """
    paths = build_paths(logical_date_str)
    return extract_data(
        input_gzip_path=paths["gzip_path"],
        output_text_path=paths["txt_path"],
    )


def task_transform(logical_date_str: str) -> str:
    """Transform extracted text into a compact CSV and return the CSV path.

    Args:
        logical_date_str: Airflow logical date as an ISO string.

    Returns:
        Local path to the transformed CSV file.
    """
    cfg = load_app_config()
    paths = build_paths(logical_date_str)

    transform_data(
        input_file_path=paths["txt_path"],
        target_hour_timestamp=paths["hour_iso"],
        target_companies=set(cfg.target_companies),
        output_file_path=paths["csv_path"],
    )
    return paths["csv_path"]


def task_load(logical_date_str: str) -> int:
    """Load transformed CSV into Postgres and return number of inserted rows.

    Args:
        logical_date_str: Airflow logical date as an ISO string.

    Returns:
        Number of rows inserted.
    """
    paths = build_paths(logical_date_str)
    return load_data(
        csv_file_path=paths["csv_path"],
        target_hour_timestamp=paths["hour_iso"],
        table_name="pageviews_hourly",
    )