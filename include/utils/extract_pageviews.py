import gzip
import os
from pathlib import Path

from launch_sentiment.include.common.logger_config import get_logger


logger = get_logger(__name__)


def extract_data(
    input_gzip_path: str,
    output_text_path: str
) -> str:
    """
    Extract a Wikimedia pageviews gzip file into a plain text file.

    This function is idempotent and safe for retries.
    - If the output file already exists, extraction is skipped.
    - Extraction is written to a temporary file and atomically renamed.

    Args:
        input_gzip_path: Path to the .gz pageviews file
        output_text_path: Path where extracted text file should be written

    Returns:
        Path to the extracted text file
    
    Raises:
        FileNotFoundError: If the input gzip does not exist.
    """

    # Validate input
    if not os.path.exists(input_gzip_path):
        raise FileNotFoundError(f"Gzip file not found: {input_gzip_path}")
    
    out_path = Path(output_text_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 2. Idempotency check
    if out_path.exists():
        logger.info(
            "Extracted file already exists. Skipping extraction: %s",
            out_path
        )
        return str(out_path)
    
    tmp_path = out_path.with_suffix(".tmp")
    
    logger.info(
        "Starting extraction: %s → %s",
        input_gzip_path,
        output_text_path
    )
    

    # Ensure output directory exists
    # os.makedirs(os.path.dirname(output_text_path), exist_ok=True)
    

    # Stream extraction
    try:
        with gzip.open(input_gzip_path, "rt", encoding="utf-8") as gz_file, \
             open(tmp_path, "w", encoding="utf-8") as out:

            for line in gz_file:
                out.write(line)
                
        os.replace(tmp_path, out_path)

    except Exception as exc:
        
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logger.error("Extraction failed", exc_info=True)
        raise

    # Success
    logger.info("Extraction completed successfully: %s", out_path)

    return str(out_path)