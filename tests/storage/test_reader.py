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
