"""Python bridge for the native Rust backend.

This module provides the Rust-accelerated functions for XLR8.
The Rust backend is REQUIRED - not optional.

Why Rust?
---------
- 10-15x faster than Python for encoding/decoding operations
- GIL-free execution enables true parallelism
- Zero-copy Arrow operations
- Memory-efficient BSON processing

Key Functions:
--------------
- fetch_chunks_bson: Parallel MongoDB fetches with GIL-free execution
- encode_any_values_to_arrow: Fast encoding for Types.Any fields
- decode_any_struct_arrow: Fast decoding from Arrow structs

All functions are implemented in Rust (see rust/xlr8_rust/) and
exposed via PyO3 bindings.

Usage:
------
    from xlr8.rust_backend import encode_any_values_to_arrow

    values = [42.5, "hello", None, True]
    arrow_array = encode_any_values_to_arrow(values)
"""

from xlr8._xlr8_rust import (  # type: ignore[import-not-found]
    decode_any_struct_arrow,
    encode_any_values_to_arrow,
    fetch_chunks_bson,
)

__all__ = [
    "fetch_chunks_bson",
    "decode_any_struct_arrow",
    "encode_any_values_to_arrow",
]
