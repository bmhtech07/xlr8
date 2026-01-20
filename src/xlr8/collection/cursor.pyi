"""Type stubs for XLR8 Cursor.

Inherits from PyMongoCursor for full IDE autocomplete of PyMongo methods.
Adds 4 XLR8-specific methods for DataFrame/Polars conversion.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import polars as pl
import pyarrow as pa
from pymongo.cursor import Cursor as PyMongoCursor

class XLR8Cursor(PyMongoCursor):
    """PyMongo-compatible cursor with optional acceleration.

    Inherits all PyMongo cursor methods. Adds 4 XLR8-specific methods:
    - to_dataframe(): Convert to Pandas DataFrame with acceleration
    - to_polars(): Convert to Polars DataFrame with acceleration
    - to_dataframe_batches(): Memory-efficient batch streaming
    - stream_to_callback(): Partitioned PyArrow table callbacks
    """

    def __init__(
        self,
        collection: Any,
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        sort: Optional[List[Tuple[str, int]]] = None,
        batch_size: int = 1000,
    ) -> None: ...

    # XLR8-specific accelerated methods
    def to_dataframe(
        self,
        accelerate: bool = True,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
    ) -> pd.DataFrame: ...
    def to_polars(
        self,
        accelerate: bool = True,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        flush_ram_limit_mb: int = 512,
    ) -> pl.DataFrame: ...
    def to_dataframe_batches(
        self,
        batch_size: int = 10000,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
    ) -> Generator[pd.DataFrame, None, None]: ...
    def stream_to_callback(
        self,
        callback: Callable[[pa.Table, Dict[str, Any]], None],
        *,
        partition_time_delta: timedelta,
        partition_by: Optional[Union[str, List[str]]] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
        cache_read: bool = True,
        cache_write: bool = True,
    ) -> Dict[str, Any]: ...
    def raw_cursor(self) -> PyMongoCursor: ...
    def explain_acceleration(self) -> Dict[str, Any]: ...
