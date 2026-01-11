"""
Time-range chunking utilities for XLR8.

This module splits time ranges into day-aligned chunks for parallel processing.
Each chunk becomes a work item that a worker can fetch independently.

WHY CHUNK BY TIME?
------------------

MongoDB time-series data is typically indexed by time. Chunking allows:
1. Parallel fetches - Multiple workers can fetch different time chunks
2. Incremental caching - Cache chunks separately, reuse when time range overlaps
3. Memory control - Each chunk fits in worker's RAM budget

CHUNKING ALGORITHM
------------------

INPUT:
  start = datetime(2024, 1, 5, 12, 30)  # Mid-day start
  end = datetime(2024, 1, 15, 8, 0)     # Mid-day end
  chunk_days = 3

OUTPUT (day-aligned chunks):

    Chunk 1: 2024-01-05 12:30 -> 2024-01-08 00:00 (partial first chunk)
    Chunk 2: 2024-01-08 00:00 -> 2024-01-11 00:00 (full 3-day chunk)
    Chunk 3: 2024-01-11 00:00 -> 2024-01-14 00:00 (full 3-day chunk)
    Chunk 4: 2024-01-14 00:00 -> 2024-01-15 08:00 (partial last chunk)

Note: First boundary is aligned to day start + step after the start time.

TYPICAL USAGE
-------------

6-month query with 14-day chunks:
  start = 2024-01-01
  end = 2024-07-01
  chunk_days = 14 (default)

Result: ~13 chunks
  Chunk 1: Jan 1-15
  Chunk 2: Jan 15-29
  Chunk 3: Jan 29 - Feb 12
  ...
  Chunk 13: Jun 17 - Jul 1

With 10 workers, chunks are processed in parallel:
  Workers 0-9 grab chunks 1-10 immediately
  As workers finish, they grab chunks 11-13
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

__all__ = [
    "chunk_time_range",
]


def chunk_time_range(
    start: datetime,
    end: datetime,
    chunk_size: Optional[timedelta] = None,
) -> List[Tuple[datetime, datetime]]:
    """
    Split time range into chunks.

    Creates chunks of specified size, aligned to boundaries.

    Args:
        start: Start datetime (inclusive)
        end: End datetime (exclusive)
        chunk_size: Size of each chunk as timedelta (default: 1 day)

    Returns:
        List of (chunk_start, chunk_end) tuples

    Examples:
        Day-level chunking:
        >>> start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        >>> end = datetime(2024, 1, 5, 8, 0, 0, tzinfo=timezone.utc)
        >>> chunks = chunk_time_range(start, end, chunk_size=timedelta(days=1))

        Hour-level chunking:
        >>> chunks = chunk_time_range(start, end, chunk_size=timedelta(hours=8))
    """
    # Ensure timezone-aware
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    if start >= end:
        return []

    # Determine step size
    if chunk_size is not None:
        step = chunk_size
    else:
        step = timedelta(days=1)  # Default to 1 day

    out: List[Tuple[datetime, datetime]] = []

    # First boundary strictly AFTER start, aligned to day start + step
    first_boundary = (
        datetime(start.year, start.month, start.day, tzinfo=timezone.utc) + step
    )

    lo = start
    cur = first_boundary

    while lo < end:
        chunk_end = cur if cur < end else end
        out.append((lo, chunk_end))
        lo = cur
        cur = cur + step

    return out
