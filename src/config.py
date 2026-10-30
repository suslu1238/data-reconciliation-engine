"""
config.py – Central schema definitions, data types, column configurations,
and configurable tolerance thresholds for the Data Reconciliation Engine.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Transaction Schema
# ---------------------------------------------------------------------------

TRANSACTION_COLUMNS: Final[list[str]] = [
    "transaction_id",
    "customer_id",
    "transaction_timestamp",
    "amount",
    "currency",
    "status",
]

PRIMARY_KEY: Final[str] = "transaction_id"

# DuckDB-compatible column type mapping
COLUMN_DTYPES: Final[dict[str, str]] = {
    "transaction_id": "VARCHAR",
    "customer_id": "VARCHAR",
    "transaction_timestamp": "VARCHAR",
    "amount": "DOUBLE",
    "currency": "VARCHAR",
    "status": "VARCHAR",
}

# Columns that participate in cell-level attribute comparison
COMPARABLE_COLUMNS: Final[list[str]] = [c for c in TRANSACTION_COLUMNS if c != PRIMARY_KEY]

# Controlled vocabulary
CURRENCIES: Final[list[str]] = ["USD", "EUR", "INR", "GBP"]
STATUSES: Final[list[str]] = ["COMPLETED", "PENDING", "FAILED", "REFUNDED"]

# ---------------------------------------------------------------------------
# Anomaly Injection Rates (applied to target dataset during generation)
# ---------------------------------------------------------------------------

ANOMALY_RATES: Final[dict[str, float]] = {
    "missing_rows": 0.02,          # % of source records dropped from target
    "orphan_rows": 0.01,           # % of target-only phantom records
    "cell_discrepancies": 0.03,    # % of matching records with altered field
    "duplicate_keys": 0.01,        # % of records duplicated in source/target
    "malformed_records": 0.01,     # % of records with null key or bad timestamp
}

# ---------------------------------------------------------------------------
# Reconciliation Tolerances
# ---------------------------------------------------------------------------

AMOUNT_TOLERANCE: Final[float] = 0.001   # Absolute tolerance for floating-point aggregates

# ---------------------------------------------------------------------------
# Output Paths (relative to project root; overridable via CLI)
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR: Final[str] = "output"
DEFAULT_SAMPLE_DIR: Final[str] = "data/sample"
DEFAULT_LARGE_DIR: Final[str] = "data/large"

REPORT_FILENAME: Final[str] = "reconciliation_report.json"
QUARANTINE_FILENAME: Final[str] = "quarantined_mismatches.csv"

QUARANTINE_COLUMNS: Final[list[str]] = [
    "run_id",
    "transaction_id",
    "column_name",
    "source_value",
    "target_value",
    "discrepancy_type",
    "detected_at",
]
