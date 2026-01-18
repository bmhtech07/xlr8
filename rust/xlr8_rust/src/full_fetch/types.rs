//! Type definitions for full_fetch module: buffers, schemas, chunks.

use bson::Document;

// ============================================================================
// MEMORY-AWARE BUFFERING
// ============================================================================

/// Document buffer that triggers flush when reaching memory limit.
/// Uses 15x multiplier for heap size estimation (measured empirically).
#[derive(Debug)]
pub struct MemoryAwareBuffer {
    docs: Vec<Document>,
    max_bytes: usize,
    actual_bytes_per_doc: usize,  // Measured from first batch
    measured: bool,                // Have we done one-time measurement?
    estimated_current_bytes: usize,
}

impl MemoryAwareBuffer {
    pub fn new(max_memory_mb: usize, avg_doc_size_bytes: usize) -> Self {
        let max_bytes = max_memory_mb * 1024 * 1024;
        
        // Conservative initial estimate: 15x multiplier (measured 14.8x in tests)
        // This will be replaced by actual measurement after first 10 docs
        let initial_estimate = avg_doc_size_bytes * 15;
        
        // Memory tracking with 15x multiplier (measured 14.8x)
        
        Self {
            docs: Vec::new(),
            max_bytes,
            actual_bytes_per_doc: initial_estimate,
            measured: false,
            estimated_current_bytes: 0,
        }
    }
    
    pub fn add(&mut self, doc: Document) {
        self.docs.push(doc);
        
        // One-time measurement after first 10 docs
        if !self.measured && self.docs.len() == 10 {
            self.measure_actual_memory();
            self.measured = true;
        }
        
        self.estimated_current_bytes += self.actual_bytes_per_doc;
    }
    
    pub fn measure_actual_memory(&mut self) {
        // Measure actual memory by sampling first 10 docs
        let mut total_serialized = 0;
        for doc in &self.docs {
            if let Ok(bson_bytes) = bson::to_vec(doc) {
                total_serialized += bson_bytes.len();
            }
        }
        
        let avg_serialized = total_serialized / self.docs.len();
        
        // Use measured 15x multiplier (empirically determined from measure_doc_memory test)
        let measured_bytes_per_doc = avg_serialized * 15;
        
        // One-time measurement complete
        
        // Update our estimate
        self.actual_bytes_per_doc = measured_bytes_per_doc;
        
        // Recalculate current bytes based on actual measurement
        self.estimated_current_bytes = self.docs.len() * self.actual_bytes_per_doc;
    }
    
    pub fn approx_mb(&self) -> f64 {
        self.estimated_current_bytes as f64 / (1024.0 * 1024.0)
    }
    
    pub fn should_flush(&self) -> bool {
        self.estimated_current_bytes >= self.max_bytes
    }
    
    pub fn len(&self) -> usize {
        self.docs.len()
    }
    
    pub fn clear(&mut self) {
        self.docs.clear();
        self.estimated_current_bytes = 0;
        // Keep measured flag and actual_bytes_per_doc for next batch
    }
    
    pub fn take_docs(&mut self) -> Vec<Document> {
        self.estimated_current_bytes = 0;
        std::mem::take(&mut self.docs)
    }
}

// ============================================================================
// SORT SPECIFICATION
// ============================================================================

/// Re-exported from bson_sort module. Format: Vec<(field_name, direction)> where direction is 1 (ASC) or -1 (DESC).
pub use super::bson_sort::SortSpec;

/// Result summary returned to Python after fetch completion.
#[derive(Debug)]
pub struct FetchResult {
    pub total_docs: usize,
    pub total_files: usize,
    pub duration_secs: f64,
    pub stats_file: Option<String>,
}

/// Single field definition: name and type (e.g., "float", "list:int", "any").
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "kind")]
    pub field_type: String,
}


/// Schema from Python with time_field and column definitions.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SchemaSpec {
    pub time_field: String,
    pub fields: Vec<FieldSpec>,
}

// ============================================================================
// CHUNK DEFINITIONS
// ============================================================================

/// BSON chunk with MongoDB filter. Supports ObjectId, datetime, and complex queries.
#[derive(Debug, Clone)]
pub struct BsonChunk {
    pub filter: Document,
    pub chunk_idx: i32,
    pub start_ms: Option<i64>,  // For metadata/reporting; None for partial brackets
    pub end_ms: Option<i64>,
}
