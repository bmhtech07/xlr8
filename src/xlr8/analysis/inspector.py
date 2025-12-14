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
"""
