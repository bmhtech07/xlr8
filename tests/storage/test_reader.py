"""
Tests for xlr8.storage.reader module.

Covers:
- ParquetReader initialization
- iter_documents() streaming
- to_dataframe() loading with pandas
- Statistics and metadata
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from xlr8.schema import Schema
from xlr8.schema.types import Float, String, Timestamp
from xlr8.storage.reader import ParquetReader


@pytest.fixture
def simple_schema():
    """Simple schema for testing."""
    return Schema(
        time_field="timestamp",
        fields={
            "timestamp": Timestamp(unit="ms", tz="UTC"),
            "value": Float(),
            "name": String(),
        },
    )


@pytest.fixture
def sample_parquet_cache(tmp_path, simple_schema):
    """Create sample parquet files for testing."""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()

    # Create test parquet file directly using pandas/pyarrow
    df = pd.DataFrame(
        {
            "timestamp": [
                datetime(2024, 1, 15, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 2, tzinfo=timezone.utc),
            ],
            "value": [42.5, 43.1, 44.2],
            "name": ["test1", "test2", "test3"],
        }
    )

    # Write to parquet file
    parquet_file = cache_dir / "test_part_0000.parquet"
    df.to_parquet(parquet_file, index=False)

    return cache_dir


class TestParquetReaderInit:
    """Test ParquetReader initialization."""

    def test_finds_parquet_files_in_directory(self, sample_parquet_cache):
        """Reader should find all parquet files in cache directory."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        assert len(reader.parquet_files) > 0
        assert all(f.suffix == ".parquet" for f in reader.parquet_files)

    def test_handles_empty_directory(self, tmp_path):
        """Reader should handle empty cache directory (no results)."""
        empty_cache = tmp_path / "empty_cache"
        empty_cache.mkdir()

        reader = ParquetReader(cache_dir=empty_cache)

        assert len(reader.parquet_files) == 0

    def test_raises_error_for_missing_directory(self, tmp_path):
        """Reader should raise error for non-existent directory."""
        missing_dir = tmp_path / "missing"

        with pytest.raises(FileNotFoundError):
            ParquetReader(cache_dir=missing_dir)


class TestIterDocuments:
    """Test iter_documents() streaming."""

    def test_streams_documents_from_files(self, sample_parquet_cache):
        """iter_documents() should stream all documents."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        documents = list(reader.iter_documents())

        assert len(documents) == 3
        assert all(isinstance(doc, dict) for doc in documents)
        assert all("timestamp" in doc for doc in documents)
        assert all("value" in doc for doc in documents)

    def test_respects_batch_size(self, sample_parquet_cache):
        """iter_documents() should respect batch_size parameter."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        # Should still iterate all documents regardless of batch size
        documents = list(reader.iter_documents(batch_size=1))

        assert len(documents) == 3

    def test_returns_correct_document_count(self, sample_parquet_cache):
        """iter_documents() should return all documents."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        doc_count = sum(1 for _ in reader.iter_documents())

        assert doc_count == 3


class TestToDataFrame:
    """Test to_dataframe() loading."""

    def test_loads_all_files_into_pandas(self, sample_parquet_cache, simple_schema):
        """to_dataframe() should load all files into pandas DataFrame."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        df = reader.to_dataframe(engine="pandas", schema=simple_schema)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "timestamp" in df.columns
        assert "value" in df.columns
        assert "name" in df.columns

    def test_empty_files_return_empty_dataframe(self, tmp_path, simple_schema):
        """to_dataframe() should return empty DataFrame for empty cache."""
        empty_cache = tmp_path / "empty_cache"
        empty_cache.mkdir()

        reader = ParquetReader(cache_dir=empty_cache)
        df = reader.to_dataframe(engine="pandas", schema=simple_schema)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_date_filtering(self, sample_parquet_cache, simple_schema):
        """to_dataframe() should support date filtering."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        df = reader.to_dataframe(
            engine="pandas",
            schema=simple_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 15, 3, tzinfo=timezone.utc),
        )

        # Should filter to only rows between 1-3
        assert len(df) >= 1  # At least the 1am and 2am rows


class TestStatistics:
    """Test reader statistics."""

    def test_get_statistics_returns_metadata(self, sample_parquet_cache):
        """get_statistics() should return cache metadata."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        stats = reader.get_statistics()

        assert isinstance(stats, dict)
        assert "file_count" in stats
        assert stats["file_count"] > 0

    def test_len_returns_file_count(self, sample_parquet_cache):
        """__len__() should return number of parquet files."""
        reader = ParquetReader(cache_dir=sample_parquet_cache)

        assert len(reader) > 0


class TestDatetimeTimezoneHandling:
    """Test datetime filtering with timezone-aware and timezone-naive datetimes."""

    @pytest.fixture
    def tz_aware_parquet_cache(self, tmp_path):
        """Create parquet files with timezone-aware timestamps."""
        cache_dir = tmp_path / "tz_aware_cache"
        cache_dir.mkdir()

        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-15 00:00:00",
                        "2024-01-15 01:00:00",
                        "2024-01-15 02:00:00",
                        "2024-01-15 03:00:00",
                        "2024-01-15 04:00:00",
                    ]
                ).tz_localize("UTC"),
                "value": [1.0, 2.0, 3.0, 4.0, 5.0],
                "name": ["a", "b", "c", "d", "e"],
            }
        )

        parquet_file = cache_dir / "tz_aware_part_0000.parquet"
        df.to_parquet(parquet_file, index=False)

        return cache_dir

    @pytest.fixture
    def tz_naive_parquet_cache(self, tmp_path):
        """Create parquet files with timezone-naive timestamps."""
        cache_dir = tmp_path / "tz_naive_cache"
        cache_dir.mkdir()

        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-15 00:00:00",
                        "2024-01-15 01:00:00",
                        "2024-01-15 02:00:00",
                        "2024-01-15 03:00:00",
                        "2024-01-15 04:00:00",
                    ]
                ),  # No tz_localize - naive
                "value": [1.0, 2.0, 3.0, 4.0, 5.0],
                "name": ["a", "b", "c", "d", "e"],
            }
        )

        parquet_file = cache_dir / "tz_naive_part_0000.parquet"
        df.to_parquet(parquet_file, index=False)

        return cache_dir

    @pytest.fixture
    def test_schema(self):
        """Schema for datetime tests."""
        return Schema(
            time_field="timestamp",
            fields={
                "timestamp": Timestamp(unit="ms", tz="UTC"),
                "value": Float(),
                "name": String(),
            },
        )

    # --- Pandas engine tests ---

    def test_pandas_tz_aware_parquet_with_tz_aware_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """Pandas: tz-aware parquet + tz-aware datetime filter should work."""
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        df = reader.to_dataframe(
            engine="pandas",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 15, 3, tzinfo=timezone.utc),
        )

        assert len(df) == 2  # 01:00 and 02:00
        assert df["value"].tolist() == [2.0, 3.0]

    def test_pandas_tz_aware_parquet_with_naive_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """Pandas: tz-aware parquet + naive datetime filter should work."""
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        # Pass naive datetimes - should be converted to tz-aware internally
        df = reader.to_dataframe(
            engine="pandas",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1),  # naive
            end_date=datetime(2024, 1, 15, 3),  # naive
        )

        assert len(df) == 2  # 01:00 and 02:00
        assert df["value"].tolist() == [2.0, 3.0]

    def test_pandas_tz_naive_parquet_with_naive_datetime(
        self, tz_naive_parquet_cache, test_schema
    ):
        """Pandas: tz-naive parquet + naive datetime filter should work."""
        reader = ParquetReader(cache_dir=tz_naive_parquet_cache)

        df = reader.to_dataframe(
            engine="pandas",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1),  # naive
            end_date=datetime(2024, 1, 15, 3),  # naive
        )

        assert len(df) == 2  # 01:00 and 02:00
        assert df["value"].tolist() == [2.0, 3.0]

    def test_pandas_tz_naive_parquet_with_tz_aware_datetime(
        self, tz_naive_parquet_cache, test_schema
    ):
        """Pandas: tz-naive parquet + tz-aware datetime filter should work."""
        reader = ParquetReader(cache_dir=tz_naive_parquet_cache)

        df = reader.to_dataframe(
            engine="pandas",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 15, 3, tzinfo=timezone.utc),
        )

        assert len(df) == 2  # 01:00 and 02:00
        assert df["value"].tolist() == [2.0, 3.0]

    # --- Polars engine tests ---

    def test_polars_tz_aware_parquet_with_tz_aware_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """Polars: tz-aware parquet + tz-aware datetime filter should work."""
        pl = pytest.importorskip("polars")
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        df = reader.to_dataframe(
            engine="polars",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 15, 3, tzinfo=timezone.utc),
        )

        assert len(df) == 2
        assert df["value"].to_list() == [2.0, 3.0]

    def test_polars_tz_aware_parquet_with_naive_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """Polars: tz-aware parquet + naive datetime filter should work."""
        pl = pytest.importorskip("polars")
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        df = reader.to_dataframe(
            engine="polars",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1),  # naive
            end_date=datetime(2024, 1, 15, 3),  # naive
        )

        assert len(df) == 2
        assert df["value"].to_list() == [2.0, 3.0]

    def test_polars_tz_naive_parquet_with_naive_datetime(
        self, tz_naive_parquet_cache, test_schema
    ):
        """Polars: tz-naive parquet + naive datetime filter should work."""
        pl = pytest.importorskip("polars")
        reader = ParquetReader(cache_dir=tz_naive_parquet_cache)

        df = reader.to_dataframe(
            engine="polars",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1),  # naive
            end_date=datetime(2024, 1, 15, 3),  # naive
        )

        assert len(df) == 2
        assert df["value"].to_list() == [2.0, 3.0]

    def test_polars_tz_naive_parquet_with_tz_aware_datetime(
        self, tz_naive_parquet_cache, test_schema
    ):
        """Polars: tz-naive parquet + tz-aware datetime filter should work."""
        pl = pytest.importorskip("polars")
        reader = ParquetReader(cache_dir=tz_naive_parquet_cache)

        df = reader.to_dataframe(
            engine="polars",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 15, 3, tzinfo=timezone.utc),
        )

        assert len(df) == 2
        assert df["value"].to_list() == [2.0, 3.0]

    # --- iter_dataframe_batches tests ---

    def test_iter_batches_tz_aware_parquet_with_naive_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """iter_dataframe_batches: tz-aware parquet + naive datetime should work."""
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        batches = list(
            reader.iter_dataframe_batches(
                schema=test_schema,
                time_field="timestamp",
                start_date=datetime(2024, 1, 15, 1),  # naive
                end_date=datetime(2024, 1, 15, 4),  # naive
                batch_size=100,
            )
        )

        assert len(batches) == 1
        df = batches[0]
        assert len(df) == 3  # 01:00, 02:00, 03:00
        assert df["value"].tolist() == [2.0, 3.0, 4.0]

    def test_iter_batches_tz_naive_parquet_with_tz_aware_datetime(
        self, tz_naive_parquet_cache, test_schema
    ):
        """iter_dataframe_batches: tz-naive parquet + tz-aware datetime should work."""
        reader = ParquetReader(cache_dir=tz_naive_parquet_cache)

        batches = list(
            reader.iter_dataframe_batches(
                schema=test_schema,
                time_field="timestamp",
                start_date=datetime(2024, 1, 15, 1, tzinfo=timezone.utc),
                end_date=datetime(2024, 1, 15, 4, tzinfo=timezone.utc),
                batch_size=100,
            )
        )

        assert len(batches) == 1
        df = batches[0]
        assert len(df) == 3  # 01:00, 02:00, 03:00
        assert df["value"].tolist() == [2.0, 3.0, 4.0]

    # --- Edge case tests ---

    def test_start_date_only_with_naive_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """Filtering with only start_date (naive) should work."""
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        df = reader.to_dataframe(
            engine="pandas",
            schema=test_schema,
            time_field="timestamp",
            start_date=datetime(2024, 1, 15, 3),  # naive, no end_date
        )

        assert len(df) == 2  # 03:00 and 04:00
        assert df["value"].tolist() == [4.0, 5.0]

    def test_end_date_only_with_naive_datetime(
        self, tz_aware_parquet_cache, test_schema
    ):
        """Filtering with only end_date (naive) should work."""
        reader = ParquetReader(cache_dir=tz_aware_parquet_cache)

        df = reader.to_dataframe(
            engine="pandas",
            schema=test_schema,
            time_field="timestamp",
            end_date=datetime(2024, 1, 15, 2),  # naive, no start_date
        )

        assert len(df) == 2  # 00:00 and 01:00
        assert df["value"].tolist() == [1.0, 2.0]
