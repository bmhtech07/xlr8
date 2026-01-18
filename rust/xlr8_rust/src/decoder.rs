//! Arrow StructArray decoder for Types.Any columns.
//!
//! This module provides fast decoding of polymorphic struct columns from Arrow memory
//! directly into Python objects. Operates on Arrow memory without Python dict iteration.
//!
//! Used by xlr8.storage.reader for decoding cached Parquet data.

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::ToPyObject;
use pyo3_arrow::PyArray;

// For parsing JSON document/array values to Python objects

/// Decode a Types.Any struct column directly from a PyArrow StructArray.
///
/// Operates on Arrow memory without Python dict iteration.
///
/// Input: PyArrow StructArray with 13 fields supporting all MongoDB BSON types:
///   - Numeric: float_value (f64), int32_value (i32), int64_value (i64)
///   - String types: string_value, objectid_value, decimal128_value, regex_value
///   - Complex types: binary_value, document_value, array_value (all stored as strings)
///   - Boolean and temporal: bool_value, datetime_value
///   - Null indicator: null_value
///
/// Output: Python list of native values (int, float, str, bool, None)
#[pyfunction]
pub fn decode_any_struct_arrow(py: Python<'_>, arrow_array: PyArray) -> PyResult<PyObject> {
    let array = arrow_array.array();

    // Cast to StructArray
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| PyValueError::new_err("Expected a StructArray"))?;

    let n = struct_array.len();

    // Get typed references to each field (zero-copy access to Arrow memory)
    // Numeric types
    let float_col = struct_array
        .column_by_name("float_value")
        .and_then(|a| a.as_any().downcast_ref::<Float64Array>());

    let int32_col = struct_array
        .column_by_name("int32_value")
        .and_then(|a| a.as_any().downcast_ref::<Int32Array>());

    let int64_col = struct_array
        .column_by_name("int64_value")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());

    // Fallback: old 7-field schema used "int_value" instead of int32/int64 split
    // Keep for reading any older cached Parquet files
    let int_col_old_format = struct_array
        .column_by_name("int_value")
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>());

    // String types
    let string_col = struct_array
        .column_by_name("string_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let objectid_col = struct_array
        .column_by_name("objectid_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let decimal128_col = struct_array
        .column_by_name("decimal128_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let regex_col = struct_array
        .column_by_name("regex_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    // Complex types (stored as strings)
    let binary_col = struct_array
        .column_by_name("binary_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let document_col = struct_array
        .column_by_name("document_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let array_col = struct_array
        .column_by_name("array_value")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    // Boolean
    let bool_col = struct_array
        .column_by_name("bool_value")
        .and_then(|a| a.as_any().downcast_ref::<BooleanArray>());

    // Datetime - handle both millisecond and nanosecond timestamps
    let datetime_ms_col = struct_array
        .column_by_name("datetime_value")
        .and_then(|a| a.as_any().downcast_ref::<TimestampMillisecondArray>());

    let datetime_ns_col = struct_array
        .column_by_name("datetime_value")
        .and_then(|a| a.as_any().downcast_ref::<TimestampNanosecondArray>());

    // Build result list - coalesce non-null values
    let mut result: Vec<PyObject> = Vec::with_capacity(n);

    // Cache json.loads function ONCE before the loop (critical optimization!)
    // Try orjson first (Rust-backed, 5-10x faster), fall back to stdlib json
    let json_loads = py
        .import_bound("orjson")
        .and_then(|m| m.getattr("loads"))
        .or_else(|_| py.import_bound("json").and_then(|m| m.getattr("loads")))
        .ok();

    for i in 0..n {
        let val: PyObject = coalesce_struct_row(
            py,
            i,
            float_col,
            int32_col,
            int64_col,
            int_col_old_format,
            string_col,
            objectid_col,
            decimal128_col,
            regex_col,
            binary_col,
            document_col,
            array_col,
            bool_col,
            datetime_ms_col,
            datetime_ns_col,
            json_loads.as_ref(),
        );
        result.push(val);
    }

    Ok(result.to_object(py))
}

/// Coalesce a single row from the struct arrays, returning the first non-null value.
/// Handles all 13 MongoDB BSON type fields.
#[inline]
fn coalesce_struct_row(
    py: Python<'_>,
    i: usize,
    float_col: Option<&Float64Array>,
    int32_col: Option<&Int32Array>,
    int64_col: Option<&Int64Array>,
    int_col_old_format: Option<&Int64Array>,
    string_col: Option<&StringArray>,
    objectid_col: Option<&StringArray>,
    decimal128_col: Option<&StringArray>,
    regex_col: Option<&StringArray>,
    binary_col: Option<&StringArray>,
    document_col: Option<&StringArray>,
    array_col: Option<&StringArray>,
    bool_col: Option<&BooleanArray>,
    datetime_ms_col: Option<&TimestampMillisecondArray>,
    datetime_ns_col: Option<&TimestampNanosecondArray>,
    json_loads: Option<&pyo3::Bound<'_, pyo3::PyAny>>,
) -> PyObject {
    // Check float first (most common for sensor data)
    if let Some(arr) = float_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Then int32
    if let Some(arr) = int32_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Then int64
    if let Some(arr) = int64_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Old format fallback (7-field schema used combined int_value)
    if let Some(arr) = int_col_old_format {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Then string
    if let Some(arr) = string_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Then objectid (as string)
    if let Some(arr) = objectid_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Decimal128 (as string)
    if let Some(arr) = decimal128_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Regex (as pattern:flags string)
    if let Some(arr) = regex_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Binary - decode base64 back to Python bytes
    if let Some(arr) = binary_col {
        if !arr.is_null(i) {
            let base64_str = arr.value(i);
            // Decode base64 to bytes
            use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
            use pyo3::types::PyBytes;
            if let Ok(bytes_vec) = BASE64.decode(base64_str) {
                return PyBytes::new_bound(py, &bytes_vec).to_object(py);
            }
            // Fallback: return base64 string if decoding fails
            return base64_str.to_object(py);
        }
    }

    // Document - parse JSON string to Python dict using CACHED json_loads (FAST!)
    if let Some(arr) = document_col {
        if !arr.is_null(i) {
            let json_str = arr.value(i);
            // Use cached json_loads (orjson or stdlib json, resolved once per batch)
            if let Some(loads) = json_loads {
                if let Ok(py_obj) = loads.call1((json_str,)) {
                    return py_obj.to_object(py);
                }
            }
            // Final fallback: return string if no json_loads available
            return json_str.to_object(py);
        }
    }

    // Array - parse JSON string to Python list using CACHED json_loads (FAST!)
    if let Some(arr) = array_col {
        if !arr.is_null(i) {
            let json_str = arr.value(i);
            // Use cached json_loads (orjson or stdlib json, resolved once per batch)
            if let Some(loads) = json_loads {
                if let Ok(py_obj) = loads.call1((json_str,)) {
                    return py_obj.to_object(py);
                }
            }
            // Final fallback: return string if no json_loads available
            return json_str.to_object(py);
        }
    }

    // Then bool
    if let Some(arr) = bool_col {
        if !arr.is_null(i) {
            return arr.value(i).to_object(py);
        }
    }

    // Datetime millisecond
    if let Some(arr) = datetime_ms_col {
        if !arr.is_null(i) {
            // Return milliseconds - Python will convert to datetime
            return arr.value(i).to_object(py);
        }
    }

    // Datetime nanosecond (old 7-field format fallback)
    if let Some(arr) = datetime_ns_col {
        if !arr.is_null(i) {
            // Return nanoseconds - Python will convert to datetime
            return arr.value(i).to_object(py);
        }
    }

    // All null - return None
    py.None()
}
