"""
Value encoder for XLR8 (Python reference implementation).

NOTE: This module provides a pure Python implementation for reference
and testing. The Rust backend (encode_any_values_to_arrow) provides
an optimized version that is much faster.

================================================================================
DATA FLOW - POLYMORPHIC VALUE ENCODING (Types.Any)
================================================================================

This module handles encoding/decoding of polymorphic values (Types.Any) to/from
union structs. This is the "bitmap struct" pattern from cetolib.

THE PROBLEM:
────────────────────────────────────────────────────────────────────────────────

MongoDB "value" fields often contain mixed types:
  doc1: {"value": 42.5}      # float
  doc2: {"value": 100}       # int
  doc3: {"value": "active"}  # string
  doc4: {"value": true}      # bool

Parquet columns must be homogeneous (one type per column).
How do we store mixed types?

THE SOLUTION - UNION STRUCT:
────────────────────────────────────────────────────────────────────────────────

Store as a struct with ONE field populated, others null:

┌─────────────────────────────────────────────────────────────────────────────┐
│ ENCODE: Python value -> Struct                                               │
│                                                                             │
│ encode_any(42.5) returns:                                                   │
│ {                                                                           │
│     "float_value": 42.5,      ← VALUE IS HERE                               │
│     "int_value": null,                                                      │
│     "string_value": null,                                                   │
│     "bool_value": null,                                                     │
│     "datetime_value": null,                                                 │
│     "objectid_value": null,                                                 │
│     "null_value": null,                                                     │
│ }                                                                           │
│                                                                             │
│ encode_any("active") returns:                                               │
│ {                                                                           │
│     "float_value": null,                                                    │
│     "int_value": null,                                                      │
│     "string_value": "active",  ← VALUE IS HERE                              │
│     "bool_value": null,                                                     │
│     "datetime_value": null,                                                 │
│     "objectid_value": null,                                                 │
│     "null_value": null,                                                     │
│ }                                                                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ DECODE: Struct -> Python value                                               │
│                                                                             │
│ decode_any({"float_value": 42.5, ...others null}) returns: 42.5             │
│ decode_any({"string_value": "active", ...others null}) returns: "active"    │
│                                                                             │
│ Algorithm: Check each field in order, return first non-null                 │
└─────────────────────────────────────────────────────────────────────────────┘

TYPE MAPPING:
────────────────────────────────────────────────────────────────────────────────

Python type      -> Struct field
──────────────────────────────────
None             -> null_value: True
bool             -> bool_value (CHECK BEFORE int!)
int              -> int_value
float            -> float_value
str              -> string_value
datetime         -> datetime_value
ObjectId         -> objectid_value (as string)
other            -> string_value (JSON serialized)

NOTE: bool must be checked BEFORE int because isinstance(True, int) is True!

================================================================================
"""

import json
from datetime import datetime
from typing import Any as AnyPython
from typing import Dict

from bson import ObjectId

__all__ = [
    "ValueEncoder",
]


class ValueEncoder:
    """
    Encodes and decodes values according to schema types.

    For Types.Any fields, encodes Python values into union structs
    where only one field is populated based on the value's type.

    Example:
        encoder = ValueEncoder()

        # Encode different types
        encoder.encode_any(42.5)      # {"float_value": 42.5, ...others null}
        encoder.encode_any("hello")   # {"string_value": "hello", ...others null}
        encoder.encode_any(True)      # {"bool_value": True, ...others null}

        # Decode back
        struct = {"float_value": 42.5, "int_value": None, ...}
        encoder.decode_any(struct)    # Returns: 42.5
    """

    @staticmethod
    def encode_any(value: AnyPython) -> Dict[str, AnyPython]:
        """
        Encode a polymorphic value to union struct.

        Maps Python types to appropriate struct fields:
        - None -> null_value: True
        - bool -> bool_value
        - int -> int_value
        - float -> float_value
        - str -> string_value
        - datetime -> datetime_value
        - ObjectId -> objectid_value (as string)
        - other -> string_value (JSON serialized)

        Args:
            value: Python value to encode

        Returns:
            Dict with one field populated, others None
        """
        result: Dict[str, AnyPython] = {
            "float_value": None,
            "int_value": None,
            "string_value": None,
            "bool_value": None,
            "datetime_value": None,
            "objectid_value": None,
            "null_value": None,
        }

        if value is None:
            result["null_value"] = True
        elif isinstance(value, bool):
            # Check bool BEFORE int (bool is subclass of int in Python)
            result["bool_value"] = value
        elif isinstance(value, int):
            result["int_value"] = value
        elif isinstance(value, float):
            result["float_value"] = value
        elif isinstance(value, str):
            result["string_value"] = value
        elif isinstance(value, datetime):
            result["datetime_value"] = value
        elif isinstance(value, ObjectId):
            result["objectid_value"] = str(value)
        else:
            # Fallback: JSON serialize complex types
            try:
                result["string_value"] = json.dumps(value, default=str)
            except (TypeError, ValueError):
                result["string_value"] = str(value)

        return result

    @staticmethod
    def decode_any(struct_value: Dict[str, AnyPython]) -> AnyPython:
        """
        Decode union struct back to Python value.

        Checks fields in priority order and returns the first non-null value.

        Args:
            struct_value: Dict with union struct fields

        Returns:
            Decoded Python value
        """
        if struct_value.get("null_value"):
            return None

        # Check in order of specificity
        if struct_value.get("float_value") is not None:
            return struct_value["float_value"]

        if struct_value.get("int_value") is not None:
            return struct_value["int_value"]

        if struct_value.get("bool_value") is not None:
            return struct_value["bool_value"]

        if struct_value.get("datetime_value") is not None:
            return struct_value["datetime_value"]

        if struct_value.get("objectid_value") is not None:
            return ObjectId(struct_value["objectid_value"])

        if struct_value.get("string_value") is not None:
            return struct_value["string_value"]

        # All fields None (shouldn't happen with valid data)
        return None

    @staticmethod
    def encode_batch(values: list) -> list:
        """
        Encode a batch of values.

        Args:
            values: List of Python values

        Returns:
            List of encoded structs
        """
        return [ValueEncoder.encode_any(v) for v in values]

    @staticmethod
    def decode_batch(struct_values: list) -> list:
        """
        Decode a batch of struct values.

        Args:
            struct_values: List of union structs

        Returns:
            List of decoded Python values
        """
        return [ValueEncoder.decode_any(s) for s in struct_values]
