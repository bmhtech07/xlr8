"""
Execution engine for parallel query execution via Rust backend.

All parallel execution now goes through the Rust backend for GIL-free performance.

Components:
- executor: High-level parallel execution (execute_parallel_stream_to_cache)
- callback: Partitioned streaming for data lake population
- planner: Memory-aware execution planning and worker configuration

Python handles:
- Query planning and bracketing
- Memory budget calculations
- Result reading and DataFrame construction

Rust backend handles:
- Parallel MongoDB fetches (GIL-free)
- BSON decoding and Arrow encoding
- Memory-aware buffering
- Parquet writing
"""

from .callback import PartitionWorkItem, execute_partitioned_callback
from .executor import execute_parallel_stream_to_cache
from .planner import (
    Backend,
    BackendConfig,
    ExecutionPlan,
    build_execution_plan,
)

__all__ = [
    # Executor
    "execute_parallel_stream_to_cache",
    # Callback
    "PartitionWorkItem",
    "execute_partitioned_callback",
    # Planner
    "Backend",
    "BackendConfig",
    "ExecutionPlan",
    "build_execution_plan",
]
