"""
Tests for cache.py module.

Cache correctness is critical for avoiding redundant MongoDB queries.
"""

from datetime import datetime

from bson import ObjectId

from xlr8.storage.cache import CacheManager, hash_query


class TestHashQuery:
    """Test hash_query() determinism and normalization."""

    def test_same_query_same_hash(self):
        """Same query parameters should produce same hash."""
        filter1 = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        filter2 = {"timestamp": {"$gte": datetime(2024, 1, 1)}}

        hash1 = hash_query(filter1)
        hash2 = hash_query(filter2)

        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hex length

    def test_different_filters_different_hash(self):
        """Different filters should produce different hashes."""
        filter1 = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        filter2 = {"timestamp": {"$gte": datetime(2024, 1, 2)}}

        hash1 = hash_query(filter1)
        hash2 = hash_query(filter2)

        assert hash1 != hash2

    def test_datetime_normalization(self):
        """Datetimes should be normalized to ISO format."""
        dt_naive = datetime(2024, 1, 1, 12, 30, 0)

        filter1 = {"timestamp": {"$gte": dt_naive}}
        hash1 = hash_query(filter1)

        # Same datetime should produce same hash
        filter2 = {"timestamp": {"$gte": datetime(2024, 1, 1, 12, 30, 0)}}
        hash2 = hash_query(filter2)

        assert hash1 == hash2

    def test_objectid_normalization(self):
        """ObjectIds should be normalized to strings."""
        oid = ObjectId("507f1f77bcf86cd799439011")

        filter1 = {"_id": oid}
        filter2 = {"_id": ObjectId("507f1f77bcf86cd799439011")}

        hash1 = hash_query(filter1)
        hash2 = hash_query(filter2)

        assert hash1 == hash2

    def test_projection_included_in_hash(self):
        """Projection should affect hash."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}

        hash1 = hash_query(filter_dict, projection={"value": 1})
        hash2 = hash_query(filter_dict, projection={"value": 1, "ts": 1})
        hash3 = hash_query(filter_dict)  # No projection

        assert hash1 != hash2
        assert hash1 != hash3
        assert hash2 != hash3

    def test_sort_included_in_hash(self):
        """Sort should affect hash."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}

        hash1 = hash_query(filter_dict, sort=[("timestamp", 1)])
        hash2 = hash_query(filter_dict, sort=[("timestamp", -1)])
        hash3 = hash_query(filter_dict)  # No sort

        assert hash1 != hash2
        assert hash1 != hash3

    def test_dict_key_ordering_ignored(self):
        """Dict key ordering shouldn't affect hash (sorted internally)."""
        filter1 = {"a": 1, "b": 2, "c": 3}
        filter2 = {"c": 3, "a": 1, "b": 2}
        filter3 = {"b": 2, "c": 3, "a": 1}

        hash1 = hash_query(filter1)
        hash2 = hash_query(filter2)
        hash3 = hash_query(filter3)

        assert hash1 == hash2 == hash3


class TestCacheManager:
    """Test CacheManager class."""

    def test_initialization_generates_hash(self, tmp_path):
        """Initialization should generate correct hash."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        assert cache.query_hash is not None
        assert len(cache.query_hash) == 32
        assert cache.cache_dir == tmp_path / cache.query_hash

    def test_ensure_cache_dir_creates_directory(self, tmp_path):
        """ensure_cache_dir() should create directory."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        assert not cache.cache_dir.exists()

        result = cache.ensure_cache_dir()

        assert cache.cache_dir.exists()
        assert cache.cache_dir.is_dir()
        assert result == cache.cache_dir

    def test_exists_false_for_empty(self, tmp_path):
        """exists() should return False for empty cache."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        assert not cache.exists()

        # Even with directory created
        cache.ensure_cache_dir()
        assert not cache.exists()

    def test_exists_true_with_parquet_files(self, tmp_path):
        """exists() should return True when parquet files exist."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        cache.ensure_cache_dir()

        # Create a dummy parquet file
        (cache.cache_dir / "test.parquet").touch()

        assert cache.exists()

    def test_list_parquet_files_returns_sorted(self, tmp_path):
        """list_parquet_files() should return sorted paths."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        cache.ensure_cache_dir()

        # Create files in non-sorted order
        (cache.cache_dir / "chunk_2.parquet").touch()
        (cache.cache_dir / "chunk_0.parquet").touch()
        (cache.cache_dir / "chunk_1.parquet").touch()
        (cache.cache_dir / "other.txt").touch()  # Non-parquet

        files = cache.list_parquet_files()

        assert len(files) == 3
        assert files[0].name == "chunk_0.parquet"
        assert files[1].name == "chunk_1.parquet"
        assert files[2].name == "chunk_2.parquet"

    def test_clean_removes_directory(self, tmp_path):
        """clean() should remove cache directory."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        cache.ensure_cache_dir()
        (cache.cache_dir / "test.parquet").touch()

        assert cache.cache_dir.exists()

        result = cache.clean()

        assert result is True
        assert not cache.cache_dir.exists()

    def test_clean_returns_false_if_not_exists(self, tmp_path):
        """clean() should return False if cache doesn't exist."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        result = cache.clean()

        assert result is False

    def test_get_metadata_returns_accurate_info(self, tmp_path):
        """get_metadata() should return accurate cache info."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        cache.ensure_cache_dir()

        # Create files with known sizes
        file1 = cache.cache_dir / "test1.parquet"
        file2 = cache.cache_dir / "test2.parquet"
        file1.write_bytes(b"x" * 1024 * 1024)  # 1 MB
        file2.write_bytes(b"y" * 512 * 1024)  # 0.5 MB

        meta = cache.get_metadata()

        assert meta["query_hash"] == cache.query_hash
        assert meta["exists"] is True
        assert meta["file_count"] == 2
        assert 1.4 < meta["total_size_mb"] < 1.6  # ~1.5 MB

    def test_repr_shows_useful_info(self, tmp_path):
        """__repr__() should show useful debug info."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        repr_str = repr(cache)

        assert "CacheManager" in repr_str
        assert cache.query_hash[:8] in repr_str
        assert "exists=" in repr_str
        assert "files=" in repr_str
        assert "size=" in repr_str


class TestCacheLifecycle:
    """Integration tests for cache lifecycle."""

    def test_full_lifecycle(self, tmp_path):
        """Create -> write files -> verify exists -> clean -> gone."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        cache = CacheManager(filter_dict, cache_root=tmp_path)

        # Initially doesn't exist
        assert not cache.exists()

        # Create and write
        cache.ensure_cache_dir()
        (cache.cache_dir / "chunk_0.parquet").touch()
        (cache.cache_dir / "chunk_1.parquet").touch()

        # Now exists
        assert cache.exists()
        assert len(cache.list_parquet_files()) == 2

        # Clean
        cache.clean()

        # Gone
        assert not cache.exists()
        assert not cache.cache_dir.exists()

    def test_multiple_queries_different_directories(self, tmp_path):
        """Multiple queries should get different directories."""
        filter1 = {"timestamp": {"$gte": datetime(2024, 1, 1)}}
        filter2 = {"timestamp": {"$gte": datetime(2024, 1, 2)}}

        cache1 = CacheManager(filter1, cache_root=tmp_path)
        cache2 = CacheManager(filter2, cache_root=tmp_path)

        assert cache1.query_hash != cache2.query_hash
        assert cache1.cache_dir != cache2.cache_dir

    def test_same_query_reuses_directory(self, tmp_path):
        """Same query should reuse cache directory."""
        filter_dict = {"timestamp": {"$gte": datetime(2024, 1, 1)}}

        cache1 = CacheManager(filter_dict, cache_root=tmp_path)
        cache2 = CacheManager(filter_dict, cache_root=tmp_path)

        assert cache1.query_hash == cache2.query_hash
        assert cache1.cache_dir == cache2.cache_dir

        # Write with first, read with second
        cache1.ensure_cache_dir()
        (cache1.cache_dir / "data.parquet").touch()

        assert cache2.exists()
        assert len(cache2.list_parquet_files()) == 1
