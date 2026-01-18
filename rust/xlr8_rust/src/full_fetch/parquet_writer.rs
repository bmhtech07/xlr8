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
pub fn write_parquet_file(batch: &RecordBatch, filepath: &str) -> PyResult<()> {
    let file = std::fs::File::create(filepath)
        .map_err(|e| PyValueError::new_err(format!("Failed to create file: {e}")))?;
    
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| PyValueError::new_err(format!("Failed to create writer: {e}")))?;
    
    writer.write(batch)
        .map_err(|e| PyValueError::new_err(format!("Failed to write batch: {e}")))?;
    
    writer.close()
        .map_err(|e| PyValueError::new_err(format!("Failed to close writer: {e}")))?;
    
    Ok(())
}
