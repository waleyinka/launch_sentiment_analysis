import csv
from pathlib import Path

from src.transform.transform_pageviews import transform_data

import inspect
print(inspect.signature(transform_data))

def test_transform_data_basic(tmp_path: Path):
    """
    Test that transform_data correctly aggregates pageviews
    for target companies and writes the expected CSV output.
    """

    # --------------------------------------------------
    # 1. Arrange (test inputs)
    # --------------------------------------------------

    input_file = tmp_path / "pageviews.txt"
    output_file = tmp_path / "output.csv"

    # Fake extracted Wikimedia data (very small on purpose)
    input_file.write_text(
        "en Amazon 100 12345\n"
        "en Amazon 50 12345\n"
        "en Apple 20 12345\n"
        "en RandomPage 999 12345\n"
        "en Facebook 70 12345\n"
    )

    target_companies = {"Amazon", "Apple", "Facebook"}
    hour_timestamp = "2025-12-10T17:00"

    # --------------------------------------------------
    # 2. Act (run transformation)
    # --------------------------------------------------

    result = transform_data(
        input_file_path=str(input_file),
        target_hour_timestamp=hour_timestamp,
        target_companies=target_companies,
        output_file_path=str(output_file),
    )

    # --------------------------------------------------
    # 3. Assert (validate results)
    # --------------------------------------------------

    # Validate returned dictionary
    assert result == {
        "Amazon": 150,
        "Apple": 20,
        "Facebook": 70,
    }

    # Validate output file exists
    assert output_file.exists()

    # Validate CSV contents
    with open(output_file, newline="") as csvfile:
        rows = list(csv.reader(csvfile))

    assert rows[0] == ["company_name", "pageviews", "hour_timestamp"]

    assert rows[1:] == [
        ["Amazon", "150", hour_timestamp],
        ["Apple", "20", hour_timestamp],
        ["Facebook", "70", hour_timestamp],
    ]
