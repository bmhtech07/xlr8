//! Parquet Writer - serializes Arrow RecordBatches to Parquet files with ZSTD compression.

use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::WriterProperties,
};

/// Write a RecordBatch to a Parquet file with ZSTD compression.
///
/// If row_group_size is None, uses Arrow's default (1024 rows).
pub fn write_parquet_file(
    batch: &RecordBatch,
    filepath: &str,
    row_group_size: Option<usize>,
) -> PyResult<()> {
    let file = std::fs::File::create(filepath)
        .map_err(|e| PyValueError::new_err(format!("Failed to create file: {e}")))?;
    
    let mut props_builder = WriterProperties::builder().set_compression(Compression::ZSTD(Default::default()));

    // Set row group size if provided
    if let Some(size) = row_group_size {
        props_builder = props_builder.set_max_row_group_size(size);
    }

    let props = props_builder.build();
    
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| PyValueError::new_err(format!("Failed to create writer: {e}")))?;
    
    writer.write(batch)
        .map_err(|e| PyValueError::new_err(format!("Failed to write batch: {e}")))?;
    
    writer.close()
        .map_err(|e| PyValueError::new_err(format!("Failed to close writer: {e}")))?;
    
    Ok(())
}
