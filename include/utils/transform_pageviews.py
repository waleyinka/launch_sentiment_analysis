"""
This module transforms raw Wikimedia pageviews into a structured hourly dataset (CSV).

The input is a plain text file containing space-delimited records:
project page_title view_count response_size

This module:
- streams a large input file line by line
- filters to English Wikipedia (project code "en")
- aggregates pageviews for a configured list of company page titles
- writes a CSV output suitable for database loading
"""

from __future__ import annotations

import os
import csv
from pathlib import Path

from launch_sentiment.include.common.logger_config import get_logger


logger = get_logger(__name__)


def transform_data(
    input_file_path: str,
    target_hour_timestamp: str,
    target_companies:  set[str],
    output_file_path: str
) -> dict[str, int]:
    """
    Transform Wikimedia pageviews data for a single hour.

    Args:
        input_file_path (str): Path to extracted Wikimedia pageviews text file.
        target_hour_timestamp (str): Hour represented by the data (e.g. 2025-12-10T17:00).
        target_companies (set[str]): Set of page titles to aggregate (e.g. {"Amazon", "Apple"}).
        output_file_path (str): Path where the transformed CSV will be written.

    Returns:
        dict[str, int]: Mapping of company name to total pageviews for the hour.

    Raises:
        FileNotFoundError: If the input file does not exist.
        ValueError: If required arguments are missing or invalid.
    """
    
    # Validate input (fail fast)
    if not os.path.exists(input_file_path):
        raise FileNotFoundError("Input pageviews file not found")
    
    if not target_companies:
        raise ValueError("No target companies provided")

    if not target_hour_timestamp:
        raise ValueError("Hour timestamp is required for transformation")
    
    logger.info(
        "Starting transformation for %s", target_hour_timestamp
    )
    
    # Initialize aggregate container - One accumulator per company
    totals = {company: 0 for company in target_companies}

    # Stream through the input file line by line
    with open(input_file_path, "r", encoding="utf-8") as stream:
        
        for line in stream:
            parts = line.split()
            if len(parts) < 4:
                continue

            domain_code = parts[0]
            page_title = parts[1]
            # view_count = int(parts[2])
            # response_size = parts[3]
                
            if domain_code != "en":
                continue
                
            if page_title not in totals:
                continue
                
            try:
                view_count = int(parts[2])
            except ValueError:
                logger.warning("Invalid view count, skipping line")
                continue
                
            if view_count < 0:
                logger.warning("Negative view count, skipping line")
                continue
    
            totals[page_title] += view_count

            
    output_path = Path(output_file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    
    with open(tmp_path, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Header
        writer.writerow(
            ["company_name", "pageviews", "hour_timestamp"]
        )
        
        # Data rows
        for company in sorted(totals.keys()):
            writer.writerow([company, totals[company], target_hour_timestamp])
    
    os.replace(tmp_path, output_path)
    
    logger.info(
        "Transformation completed successfully. Wrote %s rows to %s",
        len(totals),
        output_path,
    )
    
    return totals

# EOF



'''
companies = {
    "Amazon",
    "Apple_Inc.",
    "Facebook",
}

totals = {
    "Amazon": 0,
    "Apple": 0,
    "Facebook": 0,
}

lines = [
    "en Amazon 100 12345", 
    "en Amazon 50 12345", 
    "en Apple 50 12345",
    "en Facebook 70 12345",
]

for line in lines:
    parts = line.split()
    
    company_name = parts[1]
    pageviews = int(parts[2])
    
    if company_name in companies:
        totals[company_name] += pageviews
    
print(totals)
'''