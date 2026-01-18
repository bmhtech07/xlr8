//! Arrow Builder - converts BSON documents to Arrow columnar format.
//!
//! Flow: MongoDB cursor -> BSON docs -> Arrow arrays -> Parquet files

use bson::Document;
use std::sync::Arc;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

// Import types from types module
use super::types::SchemaSpec;

/// Build Arrow schema from user-provided SchemaSpec.
///
/// Type mappings: string/objectid->Utf8, datetime->Timestamp, int/int32->Int32,
/// int64->Int64, float->Float64, bool->Boolean, any->Struct(13 fields), list:T->List<T>
pub fn build_arrow_schema_from_spec(schema_spec: &SchemaSpec) -> Arc<ArrowSchema> {
    let mut fields = Vec::new();
    
    for field in &schema_spec.fields {
        let data_type = match field.field_type.as_str() {
            "string" | "objectid" => DataType::Utf8,
            "datetime" | "timestamp" => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            "int" | "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "float" | "float64" | "double" => DataType::Float64,
            "bool" => DataType::Boolean,
            "any" => {
                use crate::schema::create_any_struct_type_full;
                create_any_struct_type_full()
            }
            s if s.starts_with("list:") => {
                // Parse element type from "list:float", "list:int", etc.
                let element_type = &s[5..];
                match element_type {
                    "float" => DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    "int" => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    "string" | "objectid" => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    "bool" => DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                    "datetime" => DataType::List(Arc::new(Field::new("item", 
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), true))),
                    _ => {
                        // Unknown element type - default to Float64
                        eprintln!("Warning: Unknown list element type '{}', defaulting to Float64", element_type);
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                    }
                }
            }
            _ => DataType::Utf8,  // Default to string
        };
        
        fields.push(Field::new(&field.name, data_type, true));
    }
    
    Arc::new(ArrowSchema::new(fields))
}

/// Extract a single field from a batch of BSON documents into a typed Arrow array
///
/// This is the core BSON-to-Arrow conversion function. For each field in the schema,
/// this function iterates through all documents and extracts the field value, handling
/// type conversions and nested field access (via dotted paths like "user.address.city").
///
/// Called by:
/// - build_record_batch() at line 335 in this file
///
/// Purpose: Convert one column of data from row-based BSON format to columnar Arrow format.
/// For example, extracting "price" field from 10,000 documents creates a Float64Array
/// with 10,000 elements.
///
/// Field path handling:
/// - Simple fields: "name" -> doc["name"]
/// - Nested fields: "user.email" -> doc["user"]["email"]
/// - Uses get_nested_value() to traverse document hierarchy
///
/// Type-specific handling:
/// Extract values from documents into a typed Arrow array.
/// Handles scalar types, lists, and "any" (13-field struct). Missing/mismatched fields become nulls.
pub fn extract_field_to_array(
    docs: &[Document],
    field_name: &str,
    field_type: &str,
) -> PyResult<Arc<dyn arrow::array::Array>> {
    // Handle dotted field names
    let parts: Vec<&str> = field_name.split('.').collect();
    
    match field_type {
        "datetime" | "timestamp" => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(docs.len());
            for doc in docs {
                let value = get_nested_value(doc, &parts);
                match value {
                    Some(bson::Bson::DateTime(dt)) => {
                        builder.append_value(dt.timestamp_millis());
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        "float" | "float64" | "double" => {
            let mut builder = Float64Builder::with_capacity(docs.len());
            for doc in docs {
                let value = get_nested_value(doc, &parts);
                match value {
                    Some(bson::Bson::Double(v)) => builder.append_value(*v),
                    Some(bson::Bson::Int32(v)) => builder.append_value(*v as f64),
                    Some(bson::Bson::Int64(v)) => builder.append_value(*v as f64),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }

        "string" => {
            let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 32);
            for doc in docs {
                let value = get_nested_value(doc, &parts);
                match value {
                    Some(bson::Bson::String(s)) => builder.append_value(&s),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        "objectid" => {
            let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 24);
            for doc in docs {
                let value = get_nested_value(doc, &parts);
                match value {
                    Some(bson::Bson::ObjectId(oid)) => builder.append_value(&oid.to_hex()),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        "any" => {
            // MongoDB Any() type - use full 13-field struct encoding
            use crate::encoder::AnyEncoderBuilder;
            let mut builder = AnyEncoderBuilder::with_capacity(docs.len());
            for doc in docs {
                let value = get_nested_value(doc, &parts);
                // Convert BSON to appropriate field
                match value {
                    None => builder.append_null(),
                    Some(bson::Bson::Double(v)) => builder.append_float(*v),
                    Some(bson::Bson::Int32(v)) => builder.append_int32(*v),
                    Some(bson::Bson::Int64(v)) => builder.append_int64(*v),
                    Some(bson::Bson::String(v)) => builder.append_string(v),
                    Some(bson::Bson::ObjectId(v)) => builder.append_objectid(&v.to_hex()),
                    Some(bson::Bson::Boolean(v)) => builder.append_bool(*v),
                    Some(bson::Bson::DateTime(v)) => builder.append_datetime_ms(v.timestamp_millis()),
                    Some(bson::Bson::Decimal128(v)) => builder.append_decimal128(&v.to_string()),
                    Some(bson::Bson::Document(v)) => builder.append_document(&v.to_string()),
                    Some(bson::Bson::Array(v)) => {
                        let json_str = format!("[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","));
                        builder.append_array(&json_str);
                    }
                    Some(bson::Bson::Binary(v)) => {
                        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
                        let encoded = BASE64.encode(&v.bytes);
                        builder.append_binary(&encoded);
                    }
                    Some(bson::Bson::Null) | Some(_) => builder.append_null(),
                }
            }
            Ok(builder.finish())
        }
        // List type - native Arrow ListArray encoding with type-specific builders
        s if s.starts_with("list:") => {
            let element_type = &s[5..];
            match element_type {
                "float" => {
                    let values_builder = Float64Builder::new();
                    let mut builder = ListBuilder::new(values_builder);
                    
                    for doc in docs {
                        let value = get_nested_value(doc, &parts);
                        match value {
                            Some(bson::Bson::Array(arr)) => {
                                for elem in arr.iter() {
                                    match elem {
                                        bson::Bson::Double(v) => builder.values().append_value(*v),
                                        bson::Bson::Int32(v) => builder.values().append_value(*v as f64),
                                        bson::Bson::Int64(v) => builder.values().append_value(*v as f64),
                                        _ => builder.values().append_null(),  // Type mismatch -> null
                                    }
                                }
                                builder.append(true);
                            }
                            None => builder.append(false),
                            _ => builder.append(false),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                "int" => {
                    let values_builder = Int64Builder::new();
                    let mut builder = ListBuilder::new(values_builder);
                    
                    for doc in docs {
                        let value = get_nested_value(doc, &parts);
                        match value {
                            Some(bson::Bson::Array(arr)) => {
                                for elem in arr.iter() {
                                    match elem {
                                        bson::Bson::Int32(v) => builder.values().append_value(*v as i64),
                                        bson::Bson::Int64(v) => builder.values().append_value(*v),
                                        bson::Bson::Double(v) if v.fract() == 0.0 => 
                                            builder.values().append_value(*v as i64),
                                        _ => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            }
                            None => builder.append(false),
                            _ => builder.append(false),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                "string" | "objectid" => {
                    let values_builder = StringBuilder::new();
                    let mut builder = ListBuilder::new(values_builder);
                    
                    for doc in docs {
                        let value = get_nested_value(doc, &parts);
                        match value {
                            Some(bson::Bson::Array(arr)) => {
                                for elem in arr.iter() {
                                    match elem {
                                        bson::Bson::String(v) => builder.values().append_value(v),
                                        bson::Bson::ObjectId(v) => builder.values().append_value(&v.to_hex()),
                                        _ => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            }
                            None => builder.append(false),
                            _ => builder.append(false),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                "bool" => {
                    let values_builder = BooleanBuilder::new();
                    let mut builder = ListBuilder::new(values_builder);
                    
                    for doc in docs {
                        let value = get_nested_value(doc, &parts);
                        match value {
                            Some(bson::Bson::Array(arr)) => {
                                for elem in arr.iter() {
                                    match elem {
                                        bson::Bson::Boolean(v) => builder.values().append_value(*v),
                                        _ => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            }
                            None => builder.append(false),
                            _ => builder.append(false),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                "datetime" => {
                    let values_builder = TimestampMillisecondBuilder::new();
                    let mut builder = ListBuilder::new(values_builder);
                    
                    for doc in docs {
                        let value = get_nested_value(doc, &parts);
                        match value {
                            Some(bson::Bson::Array(arr)) => {
                                for elem in arr.iter() {
                                    match elem {
                                        bson::Bson::DateTime(v) => builder.values().append_value(v.timestamp_millis()),
                                        _ => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            }
                            None => builder.append(false),
                            _ => builder.append(false),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => {
                    // Unknown element type - default to Float64
                    let values_builder = Float64Builder::new();
                    let mut builder = ListBuilder::new(values_builder);
                    
                    for doc in docs {
                        let value = get_nested_value(doc, &parts);
                        match value {
                            Some(bson::Bson::Array(arr)) => {
                                for elem in arr.iter() {
                                    match elem {
                                        bson::Bson::Double(v) => builder.values().append_value(*v),
                                        bson::Bson::Int32(v) => builder.values().append_value(*v as f64),
                                        bson::Bson::Int64(v) => builder.values().append_value(*v as f64),
                                        _ => builder.values().append_null(),
                                    }
                                }
                                builder.append(true);
                            }
                            None => builder.append(false),
                            _ => builder.append(false),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        }
        _ => {
            // Default: extract as string
            let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 32);
            for doc in docs {
                let value = get_nested_value(doc, &parts);
                match value {
                    Some(v) => builder.append_value(&v.to_string()),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Navigate nested BSON document using dotted path (e.g., "user.address.city").
pub fn get_nested_value<'a>(doc: &'a Document, parts: &[&str]) -> Option<&'a bson::Bson> {
    if parts.is_empty() {
        return None;
    }
    
    let mut current: Option<&bson::Bson> = doc.get(parts[0]);
    
    for part in &parts[1..] {
        match current {
            Some(bson::Bson::Document(d)) => {
                current = d.get(*part);
            }
            _ => return None,
        }
    }
    
    current
}

/// Build Arrow RecordBatch from BSON documents using the provided schema.
pub fn build_record_batch(
    docs: &[Document],
    schema_spec: &SchemaSpec,
    arrow_schema: &Arc<ArrowSchema>,
) -> PyResult<RecordBatch> {
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    
    for field_spec in &schema_spec.fields {
        let array = extract_field_to_array(docs, &field_spec.name, &field_spec.field_type)?;
        arrays.push(array);
    }
    
    RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| PyValueError::new_err(format!("Failed to create RecordBatch: {e}")))
}

