"""
Configuration layer for LaunchSentiment.

This module centralizes environment driven configuration so the pipeline is
portable across local, Docker, and CI environments.

It uses fail fast validation for required settings to avoid partial pipeline
runs that fail late.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    """Read environment variables with basic validation.

    Args:
        name: Environment variable name.
        default: Default value if not present.
        required: If True, raises ValueError when missing.

    Returns:
        Environment variable value.

    Raises:
        ValueError: If required=True and variable is missing.
    """
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value or ""


@dataclass(frozen=True)
class AppConfig:
    """Application configuration values.

    Attributes:
        pageviews_base_url: Root base URL for Wikimedia pageviews dumps.
        local_data_dir: Local directory for raw and intermediate files.
        target_companies: Comma separated page titles to extract.
    """
    pageviews_base_url: str
    local_data_dir: str
    target_companies: list[str]


@dataclass(frozen=True)
class DbConfig:
    """Database configuration values.

    Attributes:
        host: Postgres host.
        port: Postgres port.
        dbname: Postgres database name.
        user: Postgres username.
        password: Postgres password.
    """
    host: str
    port: int
    dbname: str
    user: str
    password: str


def load_app_config() -> AppConfig:
    """Load application configuration from environment variables.

    Returns:
        AppConfig instance.
    """
    companies_raw = _get_env("TARGET_COMPANIES", "Amazon,Apple_Inc.,Facebook,Google,Microsoft")
    companies = [c.strip() for c in companies_raw.split(",") if c.strip()]

    return AppConfig(
        pageviews_base_url=_get_env("PAGEVIEWS_BASE_URL", "https://dumps.wikimedia.org/other/pageviews"),
        local_data_dir=_get_env("LOCAL_DATA_DIR", "/opt/airflow/data"),
        target_companies=companies,
    )


def load_db_config() -> DbConfig:
    """Load DB configuration from environment variables.

    Returns:
        DbConfig instance.

    Raises:
        ValueError: If required DB settings are missing.
    """
    return DbConfig(
        host=_get_env("POSTGRES_HOST", required=True),
        port=int(_get_env("POSTGRES_PORT", "5432")),
        dbname=_get_env("POSTGRES_DB", required=True),
        user=_get_env("POSTGRES_USER", required=True),
        password=_get_env("POSTGRES_PASSWORD", required=True),
    )
    

from dataclasses import dataclass


@dataclass(frozen=True)
class SmtpConfig:
    """SMTP configuration values.

    Attributes:
        host: SMTP server host.
        port: SMTP server port.
        user: SMTP username.
        password: SMTP password.
        from_email: Sender email address.
    """
    host: str
    port: int
    user: str
    password: str
    from_email: str
