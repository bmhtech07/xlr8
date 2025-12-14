"""
MongoDB Query Chunkability Inspector for XLR8.

This module determines whether a MongoDB find() query can be safely split by time
for parallel execution. A query is "chunkable" if running it on time-based chunks
and merging results is equivalent to running on the full dataset.

================================================================================
CORE PRINCIPLE: DOCUMENT LOCALITY
================================================================================

A query operator is SAFE for chunking if it evaluates each document independently
using only data within that document. Examples:

    SAFE (document-local):
        {"value": {"$gt": 100}}              # Compare field to constant
        {"tags": {"$all": ["a", "b"]}}       # Check array contents
        {"status": {"$in": ["x", "y"]}}      # Check set membership

    UNSAFE (cross-document or stateful):
        {"$near": {"$geometry": ...}}         # Sorts by distance across ALL docs
        {"$text": {"$search": "..."}}         # Uses corpus-wide IDF scores
        {"$expr": {"$gt": ["$a", "$b"]}}      # Cannot statically analyze

================================================================================
OPERATOR CATEGORIES
================================================================================

    ALWAYS_ALLOWED (23 operators)
        Document-local operators that are always safe to chunk.
        Includes: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin,
                  $exists, $type, $all, $elemMatch, $size,
                  $regex, $mod, $jsonSchema, $comment, $options,
                  $bitsAllClear, $bitsAllSet, $bitsAnyClear, $bitsAnySet, $and

    CONDITIONAL (3 operators)
        Safe only under specific conditions:
        - $or: Allowed at depth 1 (top-level), rejected if nested
            (This is a specific rule for XLR8 to avoid too much complexity
            but might be relaxed in future)
        - $nor: Allowed if not referencing the time field
        - $not: Allowed if not applied to the time field

    NEVER_ALLOWED (17 operators)
        Cannot be chunked under any circumstances:
        - Geospatial: $near, $nearSphere, $geoWithin, $geoIntersects, etc.
        - Text search: $text
        - Expression: $expr, $where
        - Atlas: $search, $vectorSearch

================================================================================
USAGE
================================================================================
TODO: Fill this section with usage examples after implementation is complete.
"""

from __future__ import annotations

_all__ = [
    # Classification sets
    "ALWAYS_ALLOWED",
    "CONDITIONAL",
]

# =============================================================================
# OPERATOR CLASSIFICATION
# =============================================================================

ALWAYS_ALLOWED: frozenset[str] = frozenset(
    {
        # ── Comparison ──────────────────────────────────────────────────────────
        # Compare field value against a constant. Always document-local.
        #
        # Example: Find all sensors with readings above threshold
        #   {"value": {"$gt": 100}, "recordedAt": {"$gte": t1, "$lt": t2}}
        #
        "$eq",  # {"status": {"$eq": "active"}}  — equals
        "$ne",  # {"status": {"$ne": "deleted"}} — not equals
        "$gt",  # {"value": {"$gt": 100}}        — greater than
        "$gte",  # {"value": {"$gte": 100}}       — greater or equal
        "$lt",  # {"value": {"$lt": 0}}          — less than
        "$lte",  # {"value": {"$lte": 100}}       — less or equal
        "$in",  # {"type": {"$in": ["A", "B"]}}  — in set
        "$nin",  # {"type": {"$nin": ["X", "Y"]}} — not in set
        # ── Element ─────────────────────────────────────────────────────────────
        # Check field existence or BSON type. Document-local metadata checks.
        #
        # Example: Only include documents with validated readings
        #   {"validated_at": {"$exists": true}, "value": {"$type": "double"}}
        #
        "$exists",  # {"email": {"$exists": true}}
        "$type",  # {"value": {"$type": "double"}}
        # ── Array ───────────────────────────────────────────────────────────────
        # Evaluate array fields within a single document.
        #
        # Example: Find sensors with all required tags
        #   {"tags": {"$all": ["calibrated", "production"]}}
        #
        "$all",  # {"tags": {"$all": ["a", "b"]}}
        "$elemMatch",  # {"readings": {"$elemMatch": {"value": {"$gt": 100}}}}
        "$size",  # {"items": {"$size": 3}}
        # ── Bitwise ─────────────────────────────────────────────────────────────
        # Compare integer bits against a bitmask. Document-local.
        #
        # Example: Find flags with specific bits set
        #   {"flags": {"$bitsAllSet": [0, 2, 4]}}
        #
        "$bitsAllClear",
        "$bitsAllSet",
        "$bitsAnyClear",
        "$bitsAnySet",
        # ── Evaluation (safe) ───────────────────────────────────────────────────
        # Pattern matching and validation that is document-local.
        #
        # Example: Match sensor names by pattern
        #   {"sensor_id": {"$regex": "^TEMP_", "$options": "i"}}
        #
        "$regex",  # {"name": {"$regex": "^sensor_"}}
        "$options",  # Modifier for $regex
        "$mod",  # {"value": {"$mod": [10, 0]}}  — divisible by 10
        "$jsonSchema",  # {"$jsonSchema": {"required": ["name"]}}
        "$comment",  # {"$comment": "audit query"}  — annotation only
        # ── Logical (safe) ──────────────────────────────────────────────────────
        # $and is always safe: conjunctions preserve correctness.
        #
        # Example: Multiple conditions all must match
        #   {"$and": [{"value": {"$gt": 0}}, {"status": "active"}]}
        #
        "$and",
    }
)

CONDITIONAL: frozenset[str] = frozenset(
    {
        # ── $or ─────────────────────────────────────────────────────────────────
        # ALLOWED at depth 1 only. Top-level $or is decomposed into "brackets"
        # which are executed and cached independently.
        #
        #  ALLOWED (depth 1):
        #   {"$or": [
        #       {"sensor_id": "A", "recordedAt": {"$gte": t1, "$lt": t2}},
        #       {"sensor_id": "B", "recordedAt": {"$gte": t1, "$lt": t2}}
        #   ]}
        #
        #  REJECTED (depth 2 - nested $or):
        #   {"$or": [{"$or": [{...}, {...}]}, {...}]}
        #
        "$or",
        # ── $nor ────────────────────────────────────────────────────────────────
        # ALLOWED if not referencing time field. Negating time bounds creates
        # unpredictable behavior when chunking.
        #
        #  ALLOWED (excludes status values):
        #   {"$nor": [{"status": "deleted"}, {"status": "draft"}],
        #    "recordedAt": {"$gte": t1, "$lt": t2}}
        #
        #  REJECTED (negates time constraint):
        #   {"$nor": [{"recordedAt": {"$lt": "2024-01-01"}}]}
        #
        "$nor",
        # ── $not ────────────────────────────────────────────────────────────────
        # ALLOWED if not applied to time field. Same reasoning as $nor.
        #
        #  ALLOWED (negates value constraint):
        #   {"value": {"$not": {"$lt": 0}}}   — equivalent to value >= 0
        #
        #  REJECTED (negates time constraint):
        #   {"recordedAt": {"$not": {"$lt": "2024-01-15"}}}
        #
        "$not",
    }
)
