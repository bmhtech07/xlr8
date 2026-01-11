"""
Cache management for XLR8 Parquet storage.

This module provides query-specific caching for MongoDB results:

1. Query Hashing (hash_query):
   - Creates deterministic MD5 hash from query parameters (filter, projection, sort)
   - Normalizes datetimes to ISO format, ObjectIds to strings
   - Recursively sorts dicts for determinism
   - Same query always produces same hash

2. Cache Lifecycle (CacheManager):
   - Each query gets unique directory: .cache/{query_hash}/
   - Manages Parquet file storage per query
   - Provides cache existence checking, file listing, cleanup

Usage:
    # Hash a query
    query_hash = hash_query(filter_dict={"timestamp": {"$gte": start_date}})

    # Manage cache lifecycle
    cache = CacheManager(filter_dict={"timestamp": {"$gte": start_date}})
    cache.ensure_cache_dir()
    # ... write parquet files to cache.cache_dir ...
    if cache.exists():
        files = cache.list_parquet_files()
    cache.clean()  # Remove when done
"""
