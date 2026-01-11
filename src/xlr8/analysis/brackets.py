"""Bracket-based query analysis for XLR8.

================================================================================
DATA FLOW - QUERY TO BRACKETS
================================================================================

This module transforms a MongoDB query into "Brackets" - the fundamental unit
of work for parallel execution.

WHAT IS A BRACKET?
--------------------------------------------------------------------------------

A Bracket = static_filter + TimeRange

It represents ONE chunk of work that can be executed independently:
- static_filter: Non-time conditions (e.g., {"region_id": "64a..."})
- timerange: Time bounds (lo, hi) that can be further chunked

EXAMPLE TRANSFORMATION:
--------------------------------------------------------------------------------

INPUT QUERY:
    {
        "$or": [
            {"region_id": ObjectId("64a...")},
            {"region_id": ObjectId("64b...")},
            {"region_id": ObjectId("64c...")},
        ],
        "account_id": ObjectId("123..."),  # Global AND condition
        "timestamp": {"$gte": datetime(2024,1,1), "$lt": datetime(2024,7,1)}
    }

STEP 1: split_global_and() extracts:
  global_and = {"account_id": ObjectId("123..."),
                "timestamp": {"$gte": ..., "$lt": ...}}
  or_list = [{"region_id": "64a..."},
             {"region_id": "64b..."}, ...]

STEP 2: For each $or branch, merge with global_and:
  Branch 1: {"account_id": "123...", "region_id": "64a...", "timestamp": {...}}
  Branch 2: {"account_id": "123...", "region_id": "64b...", "timestamp": {...}}
  ...

STEP 3: Extract time bounds and create Brackets:

    OUTPUT: List[Bracket]

    Bracket(
        static_filter={"account_id": "123...", "region_id": "64a..."},
        timerange=TimeRange(lo=2024-01-01, hi=2024-07-01, is_full=True)
    )

    Bracket(
        static_filter={"account_id": "123...", "region_id": "64b..."},
        timerange=TimeRange(lo=2024-01-01, hi=2024-07-01, is_full=True)
    )
    ...

NEXT STEP: Each bracket's timerange is chunked (14-day chunks) and queued
           for parallel execution.

WHY BRACKETS?
--------------------------------------------------------------------------------
1. Parallelization: Each bracket can be fetched independently
2. Caching: Same static_filter can reuse cached data
3. Time chunking: TimeRange can be split into smaller chunks for workers

================================================================================
"""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from src.xlr8.analysis.inspector import (
    extract_time_bounds_recursive,
)

# =============================================================================
# OVERLAP DETECTION HELPERS
# =============================================================================
# These helpers detect when $or branches may have overlapping result sets,
# which would cause duplicates when executing brackets independently.
#
# NEGATION OPERATORS: $nin, $ne, $not, $nor in an $or branch can overlap with
# other branches that use positive filters on the same field.
#
# $in OVERLAP: Two branches with $in on the same field may share values.
# Example: {"field": {"$in": [1,2,3]}} and {"field": {"$in": [3,4,5]}}
#
# INHERENTLY OVERLAPPING OPERATORS: Some operators can match the same document
# across different branches even with different values:
# - $all: {"tags": {"$all": ["a","b"]}} and {"tags": {"$all": ["b","c"]}}
#         both match a document with tags: ["a","b","c"]
# - $elemMatch: array element matching can overlap
# - $regex: pattern matching can overlap
# - $mod: modulo conditions can overlap
# - Comparison operators ($gt, $lt, etc.): ranges can overlap
# =============================================================================

# Operators that create negation/exclusion filters
NEGATION_OPERATORS: Set[str] = {"$nin", "$ne", "$not", "$nor"}

# Operators that can cause overlap between branches even with different values
# These should trigger single-bracket execution when used on differentiating fields
OVERLAP_PRONE_OPERATORS: Set[str] = {
    "$all",  # Array superset matching
    "$elemMatch",  # Array element matching
    "$regex",  # Pattern matching
    "$mod",  # Modulo matching
    "$gt",  # Greater than - ranges can overlap
    "$gte",  # Greater than or equal
    "$lt",  # Less than - ranges can overlap
    "$lte",  # Less than or equal
    "$bitsAllSet",  # Bitwise operations can overlap
    "$bitsAnySet",
    "$bitsAllClear",
    "$bitsAnyClear",
}
#          both match documents where field=3.
# =============================================================================

# Operators that create negation/exclusion filters
NEGATION_OPERATORS: Set[str] = {"$nin", "$ne", "$not", "$nor"}


@dataclass
class TimeRange:
    """
    Time range for a bracket.

    Example:
        TimeRange(
            lo=datetime(2024, 1, 1, tzinfo=UTC),
            hi=datetime(2024, 7, 1, tzinfo=UTC),
            is_full=True  # Both lo and hi are specified
        )
    """

    lo: Optional[datetime]
    hi: Optional[datetime]
    is_full: bool


@dataclass
class Bracket:
    """
    A unit of work for parallel execution.

    Example:
        Bracket(
            static_filter={"account_id": ObjectId("123..."),
                          "region_id": ObjectId("64a...")},
            timerange=TimeRange(lo=2024-01-01, hi=2024-07-01, is_full=True)
        )

    This bracket will be converted to a MongoDB query:
        {
            "account_id": ObjectId("123..."),
            "region_id": ObjectId("64a..."),
            "timestamp": {"$gte": 2024-01-01, "$lt": 2024-07-01}
        }
    """

    static_filter: Dict[str, Any]
    timerange: TimeRange


# =============================================================================
# Add overlap detection helpers
# =============================================================================


def _has_negation_operators(query: Dict[str, Any]) -> bool:
    """
    Check if query contains any negation operators.

    Negation operators ($nin, $ne, $not, $nor) in an $or branch create
    potential overlap with other branches, leading to duplicate results.

    Args:
        query: A query dict (typically an $or branch)

    Returns:
        True if any negation operator is found at any nesting level

    Examples:
        >>> _has_negation_operators({"field": {"$in": [1,2,3]}})
        False
        >>> _has_negation_operators({"field": {"$nin": [1,2,3]}})
        True
        >>> _has_negation_operators({"$and": [{"field": {"$ne": 5}}]})
        True
    """

    def _check(obj: Any) -> bool:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in NEGATION_OPERATORS:
                    return True
                if _check(value):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if _check(item):
                    return True
        return False

    return _check(query)


def _has_overlap_prone_operators(
    query: Dict[str, Any], time_field: str
) -> Tuple[bool, Optional[str]]:
    """
    Check if query contains operators that can cause overlap between branches.

    These operators can match the same document even with different values:
    - $all: array superset matching
    - $elemMatch: array element matching
    - $regex: pattern matching
    - $mod: modulo matching
    - Comparison operators ($gt, $lt, etc.): ranges can overlap

    NOTE: Comparison operators on the TIME FIELD are allowed (that's how we chunk).
    Only comparison operators on OTHER fields trigger this check.

    Args:
        query: A query dict (typically an $or branch)
        time_field: The time field name (excluded from comparison operator check)

    Returns:
        Tuple of (has_overlap_prone, operator_name)

    Examples:
        >>> _has_overlap_prone_operators({"tags": {"$all": ["a", "b"]}}, "ts")
        (True, '$all')
        >>> _has_overlap_prone_operators({"name": {"$regex": "^John"}}, "ts")
        (True, '$regex')
        >>> _has_overlap_prone_operators({"ts": {"$gte": t1, "$lt": t2}}, "ts")
        (False, None)  # Time field comparison is OK
        >>> _has_overlap_prone_operators({"value": {"$gt": 10}}, "ts")
        (True, '$gt')  # Non-time field comparison is problematic
    """
    # Operators that are always problematic (not context-dependent)
    always_problematic = {
        "$all",
        "$elemMatch",
        "$regex",
        "$mod",
        "$bitsAllSet",
        "$bitsAnySet",
        "$bitsAllClear",
        "$bitsAnyClear",
    }

    # Comparison operators - only problematic on non-time fields
    comparison_ops = {"$gt", "$gte", "$lt", "$lte"}

    def _check(obj: Any, current_field: Optional[str] = None) -> Optional[str]:
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Track current field for comparison operator check
                field = key if not key.startswith("$") else current_field

                if key in always_problematic:
                    return key

                # Comparison operators are only problematic on non-time fields
                if key in comparison_ops and current_field != time_field:
                    return key

                result = _check(value, field)
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = _check(item, current_field)
                if result:
                    return result
        return None

    op = _check(query)
    return (True, op) if op else (False, None)


def _extract_in_values(query: Dict[str, Any], field: str) -> Optional[Set[Any]]:
    """
    Extract $in values for a specific field from query.

    Args:
        query: Query dict to search
        field: Field name to look for $in on

    Returns:
        Set of values if $in found, None if field uses different operator or not present

    Examples:
        >>> _extract_in_values({"field": {"$in": [1, 2, 3]}}, "field")
        {1, 2, 3}
        >>> _extract_in_values({"field": 5}, "field")  # Equality, not $in
        None
        >>> _extract_in_values({"other": {"$in": [1]}}, "field")  # Different field
        None
    """
    if field not in query:
        return None

    val = query[field]
    if isinstance(val, dict) and "$in" in val:
        in_vals = val["$in"]
        if isinstance(in_vals, list):
            # Convert to set of hashable representations
            result = set()
            for v in in_vals:
                try:
                    result.add(v)
                except TypeError:
                    # Unhashable value - convert to string
                    result.add(str(v))
            return result

    return None


def _find_in_fields(query: Dict[str, Any]) -> Dict[str, Set[Any]]:
    """
    Find all fields that use $in operator and their values.

    Only looks at top-level fields (not nested in $and, etc.)

    Args:
        query: Query dict (typically an $or branch)

    Returns:
        Dict mapping field name to set of $in values

    Examples:
        >>> _find_in_fields({"a": {"$in": [1,2]}, "b": {"$in": [3,4]}})
        {"a": {1, 2}, "b": {3, 4}}
        >>> _find_in_fields({"a": 5, "b": {"$gt": 10}})
        {}
    """
    result: Dict[str, Set[Any]] = {}

    for field, value in query.items():
        if field.startswith("$"):
            continue  # Skip operators
        if isinstance(value, dict) and "$in" in value:
            in_vals = value["$in"]
            if isinstance(in_vals, list):
                try:
                    result[field] = set(in_vals)
                except TypeError:
                    # Contains unhashable - convert to strings
                    result[field] = {str(v) for v in in_vals}

    return result


def _get_non_time_fields(branch: Dict[str, Any], time_field: str) -> Set[str]:
    """Get all top-level field names except the time field and operators."""
    return {k for k in branch.keys() if not k.startswith("$") and k != time_field}


def _check_or_branch_safety(
    branches: List[Dict[str, Any]], global_and: Dict[str, Any], time_field: str
) -> Tuple[bool, str, Optional[List[Dict[str, Any]]]]:
    """
    Analyze $or branches for safety (no overlapping result sets).

    This function implements the safe algorithm for detecting when $or
    branches can be executed independently as brackets vs when they must
    be executed as a single query to avoid duplicates.

    SAFETY RULES:
    1. If ANY branch has negation operators → UNSAFE (cannot transform)
    2. If branches have different field sets → UNSAFE (cannot determine overlap)
    3. If exactly ONE $in field differs → TRANSFORM (subtract overlapping values)
    4. If multiple $in fields differ → UNSAFE (explosion of combinations)
    5. If same $in fields with disjoint values → SAFE
    6. If same equality values → SAFE (same static_filter, handled by grouping)

    Args:
        branches: List of $or branch dicts
        global_and: Global conditions applied to all branches
        time_field: Time field name (excluded from field comparison)

    Returns:
        Tuple of (is_safe, reason, transformed_branches)
        - is_safe: True if brackets can be executed independently
        - reason: Description of why unsafe (empty if safe)
        - transformed_branches: Modified branches if transformation applied,
          None otherwise
    """
    if len(branches) <= 1:
        return True, "", None  # Single branch is always safe

    # Rule 1a: Check for negation operators in any branch
    for i, branch in enumerate(branches):
        if _has_negation_operators(branch):
            return (
                False,
                f"branch {i} contains negation operator ($nin/$ne/$not/$nor)",
                None,
            )

    # Rule 1b: Check for overlap-prone operators in any branch
    # These operators can match the same document across branches even with
    # different values
    for i, branch in enumerate(branches):
        has_overlap_op, op = _has_overlap_prone_operators(branch, time_field)
        if has_overlap_op:
            return False, f"branch {i} contains overlap-prone operator ({op})", None

    # Merge each branch with global_and for analysis
    effective_branches = []
    for br in branches:
        eff = {**global_and, **br}
        # Remove time field for field comparison
        if time_field in eff:
            eff_copy = dict(eff)
            eff_copy.pop(time_field)
            effective_branches.append(eff_copy)
        else:
            effective_branches.append(eff)

    # Rule 2: Check if all branches have the same field set
    field_sets = [_get_non_time_fields(eb, time_field) for eb in effective_branches]
    first_fields = field_sets[0]
    for i, fs in enumerate(field_sets[1:], 1):
        if fs != first_fields:
            return False, f"branch {i} has different field set than branch 0", None

    # All branches have same fields - now check for $in overlap
    # Find all $in fields in each branch
    all_in_fields: List[Dict[str, Set[Any]]] = [
        _find_in_fields(eb) for eb in effective_branches
    ]

    # Collect all $in field names across all branches
    in_field_names: Set[str] = set()
    for in_dict in all_in_fields:
        in_field_names.update(in_dict.keys())

    if not in_field_names:
        # No $in fields - check for equality overlap
        # Branches with identical static_filters will be grouped/merged by
        # the main algorithm. Different equality values are always disjoint (safe)
        return True, "", None

    # For each $in field, check if all branches use $in on it
    # and identify overlapping values
    fields_with_overlap: Dict[str, List[Tuple[int, int, Set[Any]]]] = {}

    for field in in_field_names:
        # Get $in values for this field from each branch
        branch_values: List[Optional[Set[Any]]] = []
        for in_dict in all_in_fields:
            branch_values.append(in_dict.get(field))

        # Check for overlap between any pair of branches
        overlaps: List[Tuple[int, int, Set[Any]]] = []
        for i in range(len(branches)):
            vals_i = branch_values[i]
            if vals_i is None:
                # This branch doesn't use $in on this field - could be equality
                # This creates potential overlap issues
                continue
            for j in range(i + 1, len(branches)):
                vals_j = branch_values[j]
                if vals_j is None:
                    continue
                common = vals_i & vals_j
                if common:
                    overlaps.append((i, j, common))

        if overlaps:
            fields_with_overlap[field] = overlaps

    if not fields_with_overlap:
        # No overlapping $in values - safe!
        return True, "", None

    # Rule 3 & 4: Handle overlapping $in values
    # IMPORTANT: Transformation is ONLY safe when all branches have the SAME
    # time bounds! If time bounds differ, we cannot subtract $in values because:
    #   - Branch A (IDs 1,2,3) with time [t1, t2]
    #   - Branch B (IDs 2,3,4) with time [t0, t3] (wider)
    #   If we remove 2,3 from Branch B, documents with IDs 2,3 in [t0,t1) and (t2,t3]
    #   would be LOST - not covered by either branch!
    #
    # So if overlapping $in values exist AND time ranges differ → fall back
    # to single bracket

    # Extract time bounds from each branch to check if they're identical
    time_bounds = []
    for br in branches:
        combined = {**global_and, **br}
        bounds, _ = extract_time_bounds_recursive(combined, time_field)
        if bounds is None:
            lo, hi = None, None
        else:
            lo, hi = bounds
        time_bounds.append((lo, hi))

    # Check if all time bounds are identical
    first_bounds = time_bounds[0]
    all_same_time = all(bounds == first_bounds for bounds in time_bounds)

    if not all_same_time:
        # Overlapping $in with different time ranges - CANNOT safely transform
        return (
            False,
            (
                f"overlapping $in on '{list(fields_with_overlap.keys())[0]}' "
                "with different time ranges"
            ),
            None,
        )

    if len(fields_with_overlap) > 1:
        # Multiple $in fields have overlap - too complex to transform
        return (
            False,
            f"multiple $in fields have overlap: {list(fields_with_overlap.keys())}",
            None,
        )

    # Exactly one $in field has overlap AND same time ranges - we can transform
    field = list(fields_with_overlap.keys())[0]
    overlaps = fields_with_overlap[field]

    # Transform: For each pair with overlap, subtract overlapping values from one branch
    # Strategy: Build a "seen" set and subtract from later branches
    transformed = [deepcopy(br) for br in branches]
    seen_values: Set[Any] = set()

    for i, branch in enumerate(transformed):
        # Get current $in values for this branch (merged with global)
        eff = {**global_and, **branch}
        in_vals = _extract_in_values(eff, field)

        if in_vals is None:
            # Branch uses equality on this field - add to seen
            if field in eff and not isinstance(eff.get(field), dict):
                try:
                    seen_values.add(eff[field])
                except TypeError:
                    seen_values.add(str(eff[field]))
            continue

        # Subtract already-seen values
        remaining = in_vals - seen_values

        if not remaining:
            # All values already covered - mark branch for removal
            transformed[i] = None  # type: ignore
        elif remaining != in_vals:
            # Some values removed - update the $in
            if (
                field in branch
                and isinstance(branch.get(field), dict)
                and "$in" in branch[field]
            ):
                branch[field]["$in"] = list(remaining)
            elif field in global_and:
                # Field is in global_and - need to override in branch
                branch[field] = {"$in": list(remaining)}

        # Add all original values to seen (they're now covered by this bracket)
        seen_values.update(in_vals)

    # Filter out None branches (fully covered)
    transformed = [b for b in transformed if b is not None]

    if not transformed:
        # Edge case: all branches were fully covered (shouldn't happen normally)
        return True, "", None

    return True, "", transformed
