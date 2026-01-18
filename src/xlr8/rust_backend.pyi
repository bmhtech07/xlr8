"""Type stubs for rust_backend module.

Provides type hints for Rust-compiled functions to enable intellisense.
"""

from typing import Any, Dict, List

def fetch_chunks_bson(
    mongodb_uri: str,
    db_name: str,
    collection_name: str,
    chunks_bson: bytes,
    schema_json: str,
    cache_dir: str,
    num_workers: int,
    batch_size: int,
    flush_trigger_mb: int,
    avg_doc_size_bytes: int,
    sort_spec_json: str,
    time_field: str,
    projection_json: str,
    row_group_size: int | None = None,
) -> Dict[str, Any]:
    """Fetch MongoDB documents in parallel chunks and write to Parquet.

    Args:
        mongodb_uri: MongoDB connection string
        db_name: Database name
        collection_name: Collection name
        chunks_bson: BSON-encoded chunk definitions
        schema_json: JSON string describing Arrow schema
        cache_dir: Directory where Parquet files will be written
        num_workers: Number of parallel workers
        batch_size: Documents per MongoDB batch
        flush_trigger_mb: Memory threshold for flushing to disk (MB)
        avg_doc_size_bytes: Average document size for memory estimation
        sort_spec_json: JSON sort specification
        time_field: Field name containing timestamps
        projection_json: MongoDB projection as JSON
        row_group_size: Parquet row group size (None = use Arrow default)

    Returns:
        Dictionary with total_docs, total_files, duration_secs
    """
    ...

def decode_any_struct_arrow(
    arrow_array: Any,  # pyarrow.StructArray
) -> List[Any]:
    """Decode PyArrow StructArray (Any type) to Python values.

    Args:
        arrow_array: PyArrow StructArray with 13-field Any encoding

    Returns:
        List of decoded Python values
    """
    ...

def encode_any_values_to_arrow(
    values: List[Any],
) -> Any:  # pyarrow.StructArray
    """Encode Python values to PyArrow StructArray (Any type).

    Args:
        values: List of Python values to encode

    Returns:
        PyArrow StructArray with 13-field Any encoding
    """
    ...
