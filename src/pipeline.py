"""
pipeline.py – CLI orchestrator for the Data Reconciliation Engine.

Responsibilities:
  * Load source & target CSVs
  * Invoke the 4-tier reconciliation comparator
  * Emit quarantined_mismatches.csv (atomic write, idempotent)
  * Emit reconciliation_report.json (atomic write, idempotent)
  * Compute overall run status
  * Structured console logging

Usage:
  python -m src.pipeline \\
      --source data/sample/source.csv \\
      --target data/sample/target.csv \\
      --output-dir output
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.comparator import reconcile
from src.config import (
    AMOUNT_TOLERANCE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SAMPLE_DIR,
    QUARANTINE_COLUMNS,
    QUARANTINE_FILENAME,
    REPORT_FILENAME,
)
from src.models import (
    DiscrepancyType,
    ReconciliationReport,
    RunStatus,
    Tier1Result,
    Tier2Result,
    Tier3Result,
    Tier4Result,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _atomic_write(path: Path, content: str) -> None:
    """Write *content* to *path* atomically via temp-file rename."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content, encoding="utf-8")
    if path.exists():
        path.unlink()
    tmp.rename(path)


def _determine_status(
    t1: Tier1Result, t2: Tier2Result, t3: Tier3Result, t4: Tier4Result
) -> RunStatus:
    """Derive overall run status from tier results."""
    has_failure = (
        not t1.schema_match
        or t2.missing_count > 0
        or t2.orphan_count > 0
        or t4.total_mismatches > 0
    )
    has_warning = (
        t1.row_count_delta != 0
        or t3.metrics_out_of_tolerance > 0
        or t2.duplicates.source_duplicate_count > 0
        or t2.duplicates.target_duplicate_count > 0
    )

    if has_failure:
        return RunStatus.FAILED
    if has_warning:
        return RunStatus.PASSED_WITH_WARNINGS
    return RunStatus.PASSED


# ---------------------------------------------------------------------------
# Quarantine builder
# ---------------------------------------------------------------------------


def _build_quarantine_df(
    run_id: str,
    t2: Tier2Result,
    t4: Tier4Result,
    detected_at: datetime,
) -> pd.DataFrame:
    """Assemble all discrepancies into the quarantine schema."""
    rows: list[dict] = []
    ts_str = detected_at.isoformat()

    # Missing records
    for tid in t2.missing_in_target:
        rows.append(
            {
                "run_id": run_id,
                "transaction_id": tid,
                "column_name": "transaction_id",
                "source_value": tid,
                "target_value": None,
                "discrepancy_type": DiscrepancyType.MISSING_IN_TARGET.value,
                "detected_at": ts_str,
            }
        )

    # Orphan records
    for tid in t2.orphan_in_target:
        rows.append(
            {
                "run_id": run_id,
                "transaction_id": tid,
                "column_name": "transaction_id",
                "source_value": None,
                "target_value": tid,
                "discrepancy_type": DiscrepancyType.ORPHAN_IN_TARGET.value,
                "detected_at": ts_str,
            }
        )

    # Source duplicates
    for tid in t2.duplicates.source_duplicates:
        rows.append(
            {
                "run_id": run_id,
                "transaction_id": tid,
                "column_name": "transaction_id",
                "source_value": tid,
                "target_value": None,
                "discrepancy_type": DiscrepancyType.DUPLICATE_KEY_SOURCE.value,
                "detected_at": ts_str,
            }
        )

    # Target duplicates
    for tid in t2.duplicates.target_duplicates:
        rows.append(
            {
                "run_id": run_id,
                "transaction_id": tid,
                "column_name": "transaction_id",
                "source_value": None,
                "target_value": tid,
                "discrepancy_type": DiscrepancyType.DUPLICATE_KEY_TARGET.value,
                "detected_at": ts_str,
            }
        )

    # Cell mismatches
    for mm in t4.mismatches:
        rows.append(
            {
                "run_id": run_id,
                "transaction_id": mm.transaction_id,
                "column_name": mm.column_name,
                "source_value": str(mm.source_value) if mm.source_value is not None else None,
                "target_value": str(mm.target_value) if mm.target_value is not None else None,
                "discrepancy_type": DiscrepancyType.CELL_MISMATCH.value,
                "detected_at": ts_str,
            }
        )

    if not rows:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    return pd.DataFrame(rows, columns=QUARANTINE_COLUMNS)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_pipeline(
    source_path: str,
    target_path: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    tolerance: float = AMOUNT_TOLERANCE,
    run_id: str | None = None,
) -> ReconciliationReport:
    """
    Full reconciliation pipeline. Returns the populated ReconciliationReport.

    This function is idempotent: re-running with the same inputs atomically
    overwrites the output files.
    """
    run_id = run_id or str(uuid.uuid4())
    started_at = datetime.now(tz=timezone.utc)
    t_start = time.perf_counter()

    logger.info("=== Reconciliation Run %s ===", run_id)
    logger.info("Source: %s | Target: %s", source_path, target_path)

    # ---- Load data ----
    source_df = pd.read_csv(source_path, dtype=str)
    target_df = pd.read_csv(target_path, dtype=str)

    # Cast amount to float for aggregation tiers (keep NaN as NaN)
    for df in (source_df, target_df):
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # ---- Reconcile ----
    t1, t2, t3, t4 = reconcile(source_df, target_df, tolerance=tolerance)

    duration = time.perf_counter() - t_start

    # ---- Status ----
    status = _determine_status(t1, t2, t3, t4)

    # ---- Quarantine output ----
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    q_path = out_dir / QUARANTINE_FILENAME

    quarantine_df = _build_quarantine_df(run_id, t2, t4, started_at)
    tmp_q = q_path.with_suffix(".tmp")
    quarantine_df.to_csv(tmp_q, index=False)
    if q_path.exists():
        q_path.unlink()
    tmp_q.rename(q_path)

    # ---- Build report ----
    total_discrepancies = (
        t2.missing_count
        + t2.orphan_count
        + t4.total_mismatches
        + t3.metrics_out_of_tolerance
        + t2.duplicates.source_duplicate_count
        + t2.duplicates.target_duplicate_count
    )

    report = ReconciliationReport(
        run_id=run_id,
        timestamp_utc=started_at,
        input_source_path=source_path,
        input_target_path=target_path,
        status=status,
        execution_duration_seconds=round(duration, 4),
        tier1=t1,
        tier2=t2,
        tier3=t3,
        tier4=t4,
        total_discrepancies=total_discrepancies,
        missing_records=t2.missing_count,
        orphan_records=t2.orphan_count,
        cell_mismatches=t4.total_mismatches,
        aggregate_mismatches=t3.metrics_out_of_tolerance,
        duplicate_keys_detected=(
            t2.duplicates.source_duplicate_count + t2.duplicates.target_duplicate_count
        ),
        quarantine_path=str(q_path),
    )

    # ---- Write JSON report ----
    report_path = out_dir / REPORT_FILENAME
    report_json = json.dumps(report.model_dump(mode="json"), indent=2)
    _atomic_write(report_path, report_json)

    # ---- Console summary ----
    logger.info("Status         : %s", status.value)
    logger.info("Duration       : %.4fs", duration)
    logger.info("Discrepancies  : %d", total_discrepancies)
    logger.info("  Missing rows : %d", t2.missing_count)
    logger.info("  Orphan rows  : %d", t2.orphan_count)
    logger.info("  Cell diffs   : %d", t4.total_mismatches)
    logger.info("  Agg mismatches: %d", t3.metrics_out_of_tolerance)
    logger.info("  Dup keys     : %d", report.duplicate_keys_detected)
    logger.info("Report         : %s", report_path)
    logger.info("Quarantine     : %s", q_path)

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Data Reconciliation Engine – Pipeline")
    parser.add_argument(
        "--source",
        default=f"{DEFAULT_SAMPLE_DIR}/source.csv",
        help="Path to source CSV",
    )
    parser.add_argument(
        "--target",
        default=f"{DEFAULT_SAMPLE_DIR}/target.csv",
        help="Path to target CSV",
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=AMOUNT_TOLERANCE,
        help="Floating-point tolerance for aggregate comparison",
    )
    args = parser.parse_args()

    report = run_pipeline(
        source_path=args.source,
        target_path=args.target,
        output_dir=args.output_dir,
        tolerance=args.tolerance,
    )
    print(f"\n{'='*60}")
    print(f"  RUN ID  : {report.run_id}")
    print(f"  STATUS  : {report.status.value}")
    print(f"  ELAPSED : {report.execution_duration_seconds}s")
    print(f"  ISSUES  : {report.total_discrepancies}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    _cli()
