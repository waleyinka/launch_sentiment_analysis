"""
Wikimedia pageviews URL and path helpers.

This module encapsulates the rules for constructing the hourly dump filename
and its full URL.

It keeps URL formatting logic in one place so the DAG and tasks remain clean.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PageviewsTarget:
    """Represents a single hourly Wikimedia pageviews dump target.

    Attributes:
        year: Four digit year.
        month: Two digit month.
        day: Two digit day.
        hour: Two digit hour in 24h format.
    """
    year: int
    month: int
    day: int
    hour: int

    @property
    def yyyymmdd(self) -> str:
        """Return date component formatted as YYYYMMDD."""
        return f"{self.year:04d}{self.month:02d}{self.day:02d}"

    @property
    def hh(self) -> str:
        """Return hour component formatted as HH."""
        return f"{self.hour:02d}"

    @property
    def filename(self) -> str:
        """Return the expected dump filename.

        Wikimedia hourly dumps use a pattern like:
        pageviews-YYYYMMDD-HH0000.gz

        Returns:
            Filename string.
        """
        return f"pageviews-{self.yyyymmdd}-{self.hh}0000.gz"

    def build_url(self, base_url: str) -> str:
        """Build the full download URL for this target.

        Args:
            base_url: Root URL like https://dumps.wikimedia.org/other/pageviews

        Returns:
            Full URL string.
        """
        return f"{base_url}/{self.year:04d}/{self.year:04d}-{self.month:02d}/{self.filename}"
