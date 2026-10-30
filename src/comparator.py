"""
comparator.py – 4-Tier DuckDB-based Data Reconciliation Engine.

Tier 1 – Schema & Volume
Tier 2 – Business Key Completeness (anti-joins, duplicate detection)
Tier 3 – Aggregate Metric Reconciler (with configurable tolerance)
Tier 4 – Field-Level Attribute Comparator (cell-by-cell diffs)
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from src.config import (
    AMOUNT_TOLERANCE,
    COLUMN_DTYPES,
    COMPARABLE_COLUMNS,
    CURRENCIES,
    PRIMARY_KEY,
    STATUSES,
    TRANSACTION_COLUMNS,
)
from src.models import (
    AggregateMetric,
    CellMismatch,
    DuplicateInfo,
    Tier1Result,
    Tier2Result,
    Tier3Result,
    Tier4Result,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DuckDB Session Management
# ---------------------------------------------------------------------------


def _enforce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure columns have the correct dtypes regardless of whether the frame is empty.

    When a DataFrame is empty Pandas defaults every column to ``object``.
    DuckDB then maps ``object`` → VARCHAR and fails when comparing those
    columns against a non-empty frame that has ``DOUBLE`` / ``INT64``.
    """
    df = df.copy()
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    # Ensure string columns stay as object (not float) even when empty
    for col in ("transaction_id", "customer_id", "transaction_timestamp", "currency", "status"):
        if col in df.columns:
            df[col] = df[col].astype(object)
    return df


def _load_dataframes(
    con: duckdb.DuckDBPyConnection,
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
) -> None:
    """Register DataFrames as DuckDB views named 'source' and 'target'."""
    con.register("source", _enforce_dtypes(source_df))
    con.register("target", _enforce_dtypes(target_df))


# ---------------------------------------------------------------------------
# Tier 1 – Schema & Volume
# ---------------------------------------------------------------------------


def run_tier1(
    con: duckdb.DuckDBPyConnection,
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
) -> Tier1Result:
    """Validate column names, data types, and row count delta."""
    source_cols = list(source_df.columns)
    target_cols = list(target_df.columns)

    missing_cols = [c for c in source_cols if c not in target_cols]
    extra_cols = [c for c in target_cols if c not in source_cols]
    schema_match = len(missing_cols) == 0 and len(extra_cols) == 0

    src_count = int(con.execute("SELECT COUNT(*) FROM source").fetchone()[0])
    tgt_count = int(con.execute("SELECT COUNT(*) FROM target").fetchone()[0])
    delta = src_count - tgt_count

    volume_status = "MATCH" if delta == 0 else "DELTA"

    logger.info(
        "Tier 1 | schema_match=%s source_rows=%d target_rows=%d delta=%d",
        schema_match,
        src_count,
        tgt_count,
        delta,
    )

    return Tier1Result(
        source_columns=source_cols,
        target_columns=target_cols,
        missing_columns=missing_cols,
        extra_columns=extra_cols,
        source_row_count=src_count,
        target_row_count=tgt_count,
        row_count_delta=delta,
        schema_match=schema_match,
        volume_status=volume_status,
    )


# ---------------------------------------------------------------------------
# Tier 2 – Business Key Completeness
# ---------------------------------------------------------------------------


def run_tier2(con: duckdb.DuckDBPyConnection) -> Tier2Result:
    """Anti-joins plus duplicate key detection on primary key column."""

    # ---- Duplicate detection ----
    src_dups_q = f"""
        SELECT {PRIMARY_KEY}
        FROM source
        WHERE {PRIMARY_KEY} IS NOT NULL
        GROUP BY {PRIMARY_KEY}
        HAVING COUNT(*) > 1
    """
    src_dup_ids = [r[0] for r in con.execute(src_dups_q).fetchall()]

    tgt_dups_q = f"""
        SELECT {PRIMARY_KEY}
        FROM target
        WHERE {PRIMARY_KEY} IS NOT NULL
        GROUP BY {PRIMARY_KEY}
        HAVING COUNT(*) > 1
    """
    tgt_dup_ids = [r[0] for r in con.execute(tgt_dups_q).fetchall()]

    dup_info = DuplicateInfo(
        source_duplicates=src_dup_ids,
        target_duplicates=tgt_dup_ids,
        source_duplicate_count=len(src_dup_ids),
        target_duplicate_count=len(tgt_dup_ids),
    )

    # ---- Set difference: missing in target (source - target) ----
    # Use NOT EXISTS for broad DuckDB version compatibility
    missing_q = f"""
        SELECT DISTINCT s.{PRIMARY_KEY}
        FROM source s
        WHERE s.{PRIMARY_KEY} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM target t
              WHERE t.{PRIMARY_KEY} = s.{PRIMARY_KEY}
          )
    """
    missing_ids = [r[0] for r in con.execute(missing_q).fetchall()]

    # ---- Set difference: orphans in target (target - source) ----
    orphan_q = f"""
        SELECT DISTINCT t.{PRIMARY_KEY}
        FROM target t
        WHERE t.{PRIMARY_KEY} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM source s
              WHERE s.{PRIMARY_KEY} = t.{PRIMARY_KEY}
          )
    """
    orphan_ids = [r[0] for r in con.execute(orphan_q).fetchall()]

    key_status = "MATCH" if not missing_ids and not orphan_ids else "DISCREPANCY"

    logger.info(
        "Tier 2 | missing=%d orphan=%d src_dups=%d tgt_dups=%d",
        len(missing_ids),
        len(orphan_ids),
        len(src_dup_ids),
        len(tgt_dup_ids),
    )

    return Tier2Result(
        missing_in_target=missing_ids,
        orphan_in_target=orphan_ids,
        missing_count=len(missing_ids),
        orphan_count=len(orphan_ids),
        duplicates=dup_info,
        key_status=key_status,
    )


# ---------------------------------------------------------------------------
# Tier 3 – Aggregate Metric Reconciler
# ---------------------------------------------------------------------------


def run_tier3(
    con: duckdb.DuckDBPyConnection,
    tolerance: float = AMOUNT_TOLERANCE,
) -> Tier3Result:
    """Compare aggregates globally and grouped by currency × status."""

    metrics: list[AggregateMetric] = []

    def _agg_query(table: str, where: str = "") -> dict[str, float | None]:
        clause = f"WHERE {where}" if where else ""
        row = con.execute(
            f"""
            SELECT
                SUM(amount),
                AVG(amount),
                MIN(amount),
                MAX(amount)
            FROM {table}
            WHERE {PRIMARY_KEY} IS NOT NULL
            {('AND ' + where) if where else ''}
            """
        ).fetchone()
        if row is None:
            return {k: None for k in ("sum", "avg", "min", "max")}
        return {
            "sum": row[0],
            "avg": row[1],
            "min": row[2],
            "max": row[3],
        }

    def _record_metrics(label: str, src_agg: dict, tgt_agg: dict) -> None:
        for agg_name in ("sum", "avg", "min", "max"):
            sv = src_agg[agg_name]
            tv = tgt_agg[agg_name]
            if sv is None and tv is None:
                continue
            delta = abs((sv or 0.0) - (tv or 0.0))
            within = delta <= tolerance
            metrics.append(
                AggregateMetric(
                    metric=f"{agg_name.upper()}_amount_{label}",
                    source_value=sv,
                    target_value=tv,
                    delta=delta,
                    within_tolerance=within,
                    tolerance_used=tolerance,
                )
            )

    # Global aggregates
    _record_metrics("GLOBAL", _agg_query("source"), _agg_query("target"))

    # Per-currency aggregates
    for currency in CURRENCIES:
        where = f"currency = '{currency}'"
        _record_metrics(currency, _agg_query("source", where), _agg_query("target", where))

    # Per-status aggregates
    for status in STATUSES:
        where = f"status = '{status}'"
        _record_metrics(status, _agg_query("source", where), _agg_query("target", where))

    # Per currency×status
    for currency in CURRENCIES:
        for status in STATUSES:
            where = f"currency = '{currency}' AND status = '{status}'"
            _record_metrics(
                f"{currency}_{status}",
                _agg_query("source", where),
                _agg_query("target", where),
            )

    out_of_tolerance = [m for m in metrics if not m.within_tolerance]
    agg_status = "MATCH" if not out_of_tolerance else "DISCREPANCY"

    logger.info(
        "Tier 3 | total_metrics=%d out_of_tolerance=%d",
        len(metrics),
        len(out_of_tolerance),
    )

    return Tier3Result(
        metrics=metrics,
        total_metrics_checked=len(metrics),
        metrics_within_tolerance=len(metrics) - len(out_of_tolerance),
        metrics_out_of_tolerance=len(out_of_tolerance),
        aggregate_status=agg_status,
    )


# ---------------------------------------------------------------------------
# Tier 4 – Field-Level Attribute Comparator
# ---------------------------------------------------------------------------


def run_tier4(con: duckdb.DuckDBPyConnection) -> Tier4Result:
    """Inner-join matching keys and compare all non-key columns cell by cell."""

    comparable = COMPARABLE_COLUMNS

    # Build a SELECT that evaluates mismatches for each comparable column.
    # We use NULLSAFE_EQUALS for null-aware comparison.
    case_exprs = ", ".join(
        [
            f"(s.{c} IS DISTINCT FROM t.{c}) AS mismatch_{c}"
            for c in comparable
        ]
    )

    # Build the explicit column list once (no * to avoid leaking rn into the join)
    all_tx_cols = ", ".join(
        [PRIMARY_KEY] + [c for c in COMPARABLE_COLUMNS]
    )

    join_q = f"""
        WITH src_dedup AS (
            SELECT {all_tx_cols}
            FROM (
                SELECT {all_tx_cols},
                       ROW_NUMBER() OVER (PARTITION BY {PRIMARY_KEY}) AS rn
                FROM source
                WHERE {PRIMARY_KEY} IS NOT NULL
            ) sub
            WHERE rn = 1
        ),
        tgt_dedup AS (
            SELECT {all_tx_cols}
            FROM (
                SELECT {all_tx_cols},
                       ROW_NUMBER() OVER (PARTITION BY {PRIMARY_KEY}) AS rn
                FROM target
                WHERE {PRIMARY_KEY} IS NOT NULL
            ) sub
            WHERE rn = 1
        )
        SELECT
            s.{PRIMARY_KEY},
            {', '.join([f's.{c} AS src_{c}, t.{c} AS tgt_{c}' for c in comparable])},
            {case_exprs}
        FROM src_dedup s
        INNER JOIN tgt_dedup t ON s.{PRIMARY_KEY} = t.{PRIMARY_KEY}
    """

    rows = con.execute(join_q).fetchdf()
    total_rows = len(rows)

    mismatches: list[CellMismatch] = []
    mismatch_cols: set[str] = set()

    for _, row in rows.iterrows():
        tid = row[PRIMARY_KEY]
        for col in comparable:
            if row.get(f"mismatch_{col}", False):
                src_val = row.get(f"src_{col}")
                tgt_val = row.get(f"tgt_{col}")
                mismatches.append(
                    CellMismatch(
                        transaction_id=str(tid),
                        column_name=col,
                        source_value=src_val,
                        target_value=tgt_val,
                    )
                )
                mismatch_cols.add(col)

    field_status = "MATCH" if not mismatches else "DISCREPANCY"

    logger.info(
        "Tier 4 | rows_compared=%d cell_mismatches=%d",
        total_rows,
        len(mismatches),
    )

    return Tier4Result(
        mismatches=mismatches,
        total_rows_compared=total_rows,
        total_mismatches=len(mismatches),
        columns_with_mismatches=sorted(mismatch_cols),
        field_status=field_status,
    )


# ---------------------------------------------------------------------------
# Unified Reconciliation Entry Point
# ---------------------------------------------------------------------------


def reconcile(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    tolerance: float = AMOUNT_TOLERANCE,
) -> tuple[Tier1Result, Tier2Result, Tier3Result, Tier4Result]:
    """
    Run all four tiers against loaded DataFrames.

    Returns
    -------
    tuple[Tier1Result, Tier2Result, Tier3Result, Tier4Result]
    """
    with duckdb.connect(database=":memory:") as con:
        _load_dataframes(con, source_df, target_df)

        t1 = run_tier1(con, source_df, target_df)
        t2 = run_tier2(con)
        t3 = run_tier3(con, tolerance=tolerance)
        t4 = run_tier4(con)

    return t1, t2, t3, t4
