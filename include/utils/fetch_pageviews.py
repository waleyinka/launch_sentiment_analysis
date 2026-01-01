import os
from pathlib import Path

import requests

from launch_sentiment.include.common.logger_config import get_logger

logger = get_logger(__name__)


def fetch_data(url: str, output_path: str, timeout_seconds: int = 60) -> str:
    """
    This function download a Wikimedia pageviews gzip file to disk. It is 
    idempotent i.e if the output file already exists, it skips downloading.

    Args:
        url: Direct URL to the Wikimedia .gz file.
        output_path: Local path where the file should be saved.
        timeout_seconds: Request timeout in seconds.

    Returns:
        The output_path where the gzip file exists.

    Raises:
        FileNotFoundError: If output directory cannot be created.
        requests.RequestException: If the download fails.
    """
    out_path = Path(output_path)
    out_dir = out_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        logger.info("Gzip already exists, skipping download: %s", out_path)
        return str(out_path)

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    logger.info("Starting download: %s → %s", url, out_path)

    try:
        with requests.get(url, stream=True, timeout=timeout_seconds) as resp:
            resp.raise_for_status()

            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        os.replace(tmp_path, out_path)
        logger.info("Download completed successfully: %s", out_path)
        return str(out_path)

    except Exception as exc:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

        logger.error("Download failed: %s", exc, exc_info=True)
        raise
