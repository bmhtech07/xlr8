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

# =============================================================================
# SHARED CONSTANTS TODO: Might move to constants.py
# =============================================================================

# MongoDB cursor efficiency: below this, network overhead dominates
MIN_BATCH_SIZE = 2_000

# Buffer headroom for in-flight batch (flush check happens after batch added)
BATCH_HEADROOM_RATIO = 0.2


# =============================================================================
# MEMORY CALCULATION
# =============================================================================


def calculate_flush_trigger(
    peak_ram_limit_mb: int,
    worker_count: int,
    avg_doc_size_bytes: int,
    config: BackendConfig = DEFAULT_CONFIG,
) -> tuple[int, int]:
    """
    Calculate flush trigger and batch size from memory constraints.

    This is the core memory planning function. It divides available RAM
    among workers while accounting for baseline overhead and cursor buffers.

    Args:
        peak_ram_limit_mb: Total RAM budget from user
        worker_count: Number of parallel workers
        avg_doc_size_bytes: Average document size for batch sizing
        config: Backend-specific memory constants

    Returns:
        Tuple of (flush_trigger_mb, batch_size_docs)

    Example:
        >>> trigger, batch = calculate_flush_trigger(5000, 16, 250)
        >>> print(f"Per-worker: {trigger}MB, batch: {batch} docs")
        Per-worker: 300MB, batch: 500000 docs
    """
    # Available RAM after baseline overhead
    available_ram_mb = peak_ram_limit_mb - config.baseline_mb

    if available_ram_mb <= 0:
        raise ValueError(
            f"peak_ram_limit_mb ({peak_ram_limit_mb} MB) must be greater than "
            f"baseline ({config.baseline_mb} MB). "
            f"Minimum viable: {config.baseline_mb + 50} MB."
        )

    # Account for GC retention (Python holds onto freed memory)
    effective_ram_mb = available_ram_mb / config.retention_factor

    # Subtract cursor overhead (each worker has a live MongoDB cursor)
    cursor_overhead_total = worker_count * config.cursor_overhead_mb
    ram_for_data = effective_ram_mb - cursor_overhead_total

    # Ensure we have at least some RAM for data
    ram_for_data = max(ram_for_data, worker_count * 1)  # At least 1 MB per worker

    # Each worker's allocation
    worker_allocation_mb = ram_for_data / worker_count

    # For Rust backend: the 15x multiplier is handled INSIDE the Rust buffer
    # So flush_trigger_mb is the actual MB limit the buffer should use
    # No need to divide by memory_multiplier here - Rust does that internally

    # Split: 80% flush trigger, 20% batch headroom
    flush_trigger_mb = worker_allocation_mb * (1 - BATCH_HEADROOM_RATIO)
    batch_headroom_mb = worker_allocation_mb * BATCH_HEADROOM_RATIO

    # Batch size from headroom
    batch_headroom_bytes = batch_headroom_mb * 1024 * 1024
    batch_size_docs = int(batch_headroom_bytes / avg_doc_size_bytes)

    # Floor at MIN_BATCH_SIZE for MongoDB efficiency
    batch_size_docs = max(MIN_BATCH_SIZE, batch_size_docs)

    # Floor flush trigger at 1 MB (sanity check)
    flush_trigger_mb = max(1, int(flush_trigger_mb))

    return flush_trigger_mb, batch_size_docs
