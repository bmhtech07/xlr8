"""
Execution Planner for XLR8.

================================================================================
MEMORY MODEL FOR RUST BACKEND
================================================================================

The Rust backend uses a memory-aware buffering system to control RAM usage
during parallel MongoDB fetches. Key concepts:

1. BSON DOCUMENT MEMORY OVERHEAD (15x Multiplier)
   When MongoDB sends documents over the wire (avg_doc_size_bytes), they expand
   to ~15x in memory due to heap allocations, pointers, and HashMap
   structures. Measured: 14.8x, rounded to 15x for safety. Obviously this varies
   by document shape, but 15x is a reasonable upper bound for planning as per
    benchmarks.

2. BUFFER MANAGEMENT
   Each async worker maintains its own MemoryAwareBuffer that:
   - Tracks estimated memory using the 15x multiplier
   - Flushes to Parquet when estimated bytes >= flush_trigger_mb
   - Dynamically calibrates after first 10 documents

3. MEMORY FORMULA
   Given user's flush_ram_limit_mb and max_workers:

   Per-Worker Allocation:
     available_ram = flush_ram_limit_mb - BASELINE_MB
     cursor_overhead = max_workers × CURSOR_OVERHEAD_MB_PER_WORKER
     ram_for_data = available_ram - cursor_overhead
     worker_allocation = ram_for_data / max_workers
     flush_trigger_mb = worker_allocation  # Rust handles 15x internally

================================================================================
"""

import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# BACKEND CONFIGURATION
# =============================================================================


class Backend(Enum):
    """Supported execution backends."""

    RUST = "rust"
    PYTHON = "python"  # Future use


@dataclass(frozen=True)
class BackendConfig:
    """
    Configuration constants for a specific backend.

    All values are empirically measured. See:
    - Rust: rust/xlr8_rust/tests/doc_memory_test.rs
    - Python: tests/test_schema_memory.py
    """

    # Baseline memory before any data processing
    baseline_mb: int

    # Memory expansion factor during flush/encoding
    # - Python: Arrow conversion spike (lists + arrays coexist)
    # - Rust: BSON Document heap overhead (15x serialized size)
    memory_multiplier: float

    # Per-worker MongoDB cursor overhead
    cursor_overhead_mb: int

    # Memory retention factor (Python GC holds onto freed memory)
    retention_factor: float

    # Description for logging
    description: str


# Rust backend: optimized for async workers in single process
RUST_CONFIG = BackendConfig(
    baseline_mb=7,  # Minimal Rust runtime overhead
    memory_multiplier=15.0,  # BSON Document heap overhead (measured 14.8x)
    cursor_overhead_mb=8,  # Async MongoDB cursor buffer
    retention_factor=1.0,  # Rust drops immediately, no GC retention
    description="Rust async (single process, tokio threads)",
)

# Python backend: for future multiprocessing implementation
PYTHON_CONFIG = BackendConfig(
    baseline_mb=120,  # pymongo + pandas + pyarrow imports
    memory_multiplier=3.0,  # Arrow conversion spike
    cursor_overhead_mb=16,  # Python async cursor overhead
    retention_factor=1.25,  # Python GC retention
    description="Python async (future implementation)",
)

# Default backend for current implementation
DEFAULT_BACKEND = Backend.RUST
DEFAULT_CONFIG = RUST_CONFIG
