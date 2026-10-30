"""
models.py – Pydantic v2 data models for reconciliation run metrics,
tier results, and the top-level report structure.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class RunStatus(str, Enum):
    PASSED = "PASSED"
    PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
    FAILED = "FAILED"


class DiscrepancyType(str, Enum):
    MISSING_IN_TARGET = "MISSING_IN_TARGET"
    ORPHAN_IN_TARGET = "ORPHAN_IN_TARGET"
    DUPLICATE_KEY_SOURCE = "DUPLICATE_KEY_SOURCE"
    DUPLICATE_KEY_TARGET = "DUPLICATE_KEY_TARGET"
    CELL_MISMATCH = "CELL_MISMATCH"
    AGGREGATE_MISMATCH = "AGGREGATE_MISMATCH"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    VOLUME_MISMATCH = "VOLUME_MISMATCH"
    NULL_PRIMARY_KEY = "NULL_PRIMARY_KEY"


# ---------------------------------------------------------------------------
# Tier Result Models
# ---------------------------------------------------------------------------


class Tier1Result(BaseModel):
    """Schema & Volume reconciliation results."""

    source_columns: list[str]
    target_columns: list[str]
    missing_columns: list[str]
    extra_columns: list[str]
    source_row_count: int
    target_row_count: int
    row_count_delta: int
    schema_match: bool
    volume_status: str  # e.g. "MATCH", "DELTA"


class DuplicateInfo(BaseModel):
    """Information about duplicate primary keys."""

    source_duplicates: list[str] = Field(default_factory=list)
    target_duplicates: list[str] = Field(default_factory=list)
    source_duplicate_count: int = 0
    target_duplicate_count: int = 0


class Tier2Result(BaseModel):
    """Business Key Completeness results."""

    missing_in_target: list[str] = Field(default_factory=list)   # In source, not in target
    orphan_in_target: list[str] = Field(default_factory=list)     # In target, not in source
    missing_count: int = 0
    orphan_count: int = 0
    duplicates: DuplicateInfo = Field(default_factory=DuplicateInfo)
    key_status: str = "MATCH"


class AggregateMetric(BaseModel):
    """A single aggregate comparison result."""

    metric: str           # e.g. "SUM_amount_USD_COMPLETED"
    source_value: float | None
    target_value: float | None
    delta: float | None
    within_tolerance: bool
    tolerance_used: float


class Tier3Result(BaseModel):
    """Aggregate Metric Reconciliation results."""

    metrics: list[AggregateMetric] = Field(default_factory=list)
    total_metrics_checked: int = 0
    metrics_within_tolerance: int = 0
    metrics_out_of_tolerance: int = 0
    aggregate_status: str = "MATCH"


class CellMismatch(BaseModel):
    """A single cell-level discrepancy record."""

    transaction_id: str
    column_name: str
    source_value: Any
    target_value: Any


class Tier4Result(BaseModel):
    """Field-Level Attribute Comparison results."""

    mismatches: list[CellMismatch] = Field(default_factory=list)
    total_rows_compared: int = 0
    total_mismatches: int = 0
    columns_with_mismatches: list[str] = Field(default_factory=list)
    field_status: str = "MATCH"


# ---------------------------------------------------------------------------
# Top-Level Report
# ---------------------------------------------------------------------------


class ReconciliationReport(BaseModel):
    """Machine-readable reconciliation run report."""

    run_id: str
    timestamp_utc: datetime
    input_source_path: str
    input_target_path: str
    status: RunStatus
    execution_duration_seconds: float

    # Tier summaries
    tier1: Tier1Result
    tier2: Tier2Result
    tier3: Tier3Result
    tier4: Tier4Result

    # Convenience summary counts
    total_discrepancies: int
    missing_records: int
    orphan_records: int
    cell_mismatches: int
    aggregate_mismatches: int
    duplicate_keys_detected: int

    # Quarantine output path
    quarantine_path: str

    model_config = ConfigDict()

    @field_serializer("timestamp_utc")
    def serialize_timestamp(self, v: datetime, _info) -> str:  # noqa: D401
        return v.isoformat()
