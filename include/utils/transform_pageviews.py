"""
Transform a raw Wikimedia pageviews text file for one hour into a structured
dataset containing aggregated pageview counts for selected companies.

The function:
- Streams a large input file line by line
- Filters for target company page titles
- Aggregates pageviews per company
- Writes a CSV output suitable for database loading
"""

import os
import csv

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
        "Starting transformation for %s using file %s",
        target_hour_timestamp,
        input_file_path,
    )
    
    # Initialize aggregate container - One accumulator per company
    totals = {company: 0 for company in target_companies}

    # Stream through the input file line by line
    with open(input_file_path, "r") as stream:
        
        for line in stream:
            try:
                parts = line.split()

                domain_code = parts[0]
                page_title = parts[1]
                view_count = int(parts[2])
                response_size = parts[3]
            
            except (IndexError, ValueError):
                logger.warning("Malformed line encountered, skipping")
                continue
            
            # Filter for target companies (exact match)
            if page_title in totals:
                totals[page_title] += view_count
    
    # Write structured output
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    
    with open(output_file_path, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Header
        writer.writerow(
            ["company_name", "pageviews", "hour_timestamp"]
        )
        
        # Data rows
        for company in sorted(totals.keys()):
            writer.writerow([company, totals[company], target_hour_timestamp])
    
    logger.info(
        "Transformation completed successfully. Wrote %s rows to %s",
        len(totals),
        output_file_path,
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