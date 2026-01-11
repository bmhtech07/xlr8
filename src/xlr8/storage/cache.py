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

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Optional

from bson import ObjectId


def hash_query(
    filter_dict: Dict[str, Any],
    projection: Optional[Dict[str, Any]] = None,
    sort: Optional[list] = None,
) -> str:
    """
    Create deterministic hash of query parameters.

    Uses MD5 hash of canonicalized JSON to create unique cache directory name.
    Same query parameters will always produce the same hash.

    Args:
        filter_dict: MongoDB filter dictionary
        projection: Field projection
        sort: Sort specification

    Returns:
        Hex string hash (32 characters)

    Example:
        >>> hash_query({"timestamp": {"$gte": "2024-01-01"}})
        'a3f5c9d2e1b4f6a8c7e9d1b3f5a7c9e1'
    """

    def normalize_value(obj):
        """
        Recursively normalize query values for deterministic hashing.

        Converts datetimes to ISO strings, ObjectIds to strings,
        and sorts dict keys to ensure same query always hashes identically.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: normalize_value(v) for k, v in sorted(obj.items())}
        elif isinstance(obj, list):
            return [normalize_value(v) for v in obj]
        return obj

    # Build canonical representation
    query_repr = {
        "filter": normalize_value(filter_dict),
    }

    if projection:
        query_repr["projection"] = normalize_value(projection)

    if sort:
        query_repr["sort"] = normalize_value(sort)

    # Create deterministic JSON (sorted keys)
    json_str = json.dumps(query_repr, sort_keys=True, separators=(",", ":"))

    # Hash it
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()
