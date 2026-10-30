"""
test_comparator.py – Tests for the 4-tier DuckDB reconciliation engine.

Covers:
  - Happy path: identical source & target → 0 mismatches, PASSED
  - Empty inputs: source empty, target empty, both empty
  - Set discrepancies: exact isolation of missing and phantom IDs
  - Duplicate primary keys: detection in source and target
  - Floating-point & null handling
  - Aggregate tolerance behavior
  - Cell-level diff isolation
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.comparator import reconcile, run_tier1, run_tier2, run_tier3, run_tier4
from src.config import TRANSACTION_COLUMNS
import duckdb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _df(rows: list[dict]) -> pd.DataFrame:
    """Build a transaction DataFrame from a list of dicts."""
    base = {c: [] for c in TRANSACTION_COLUMNS}
    df = pd.DataFrame(rows if rows else base, columns=TRANSACTION_COLUMNS)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df


def _clean_row(
    tid="TX001", cid="CUST1000", ts="2024-01-01T00:00:00Z",
    amount=100.0, currency="USD", status="COMPLETED"
) -> dict:
    return {
        "transaction_id": tid,
        "customer_id": cid,
        "transaction_timestamp": ts,
        "amount": amount,
        "currency": currency,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Happy Path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_identical_dataframes_produce_zero_mismatches(self):
        rows = [
            _clean_row("TX001", amount=100.0),
            _clean_row("TX002", cid="CUST2000", amount=200.0, status="PENDING"),
        ]
        src = _df(rows)
        tgt = _df(rows)
        t1, t2, t3, t4 = reconcile(src, tgt)

        assert t1.schema_match is True
        assert t1.row_count_delta == 0
        assert t2.missing_count == 0
        assert t2.orphan_count == 0
        assert t4.total_mismatches == 0
        assert t3.metrics_out_of_tolerance == 0

    def test_status_is_passed_for_identical(self):
        from src.pipeline import _determine_status
        rows = [_clean_row("TX001")]
        src = tgt = _df(rows)
        t1, t2, t3, t4 = reconcile(src, tgt)
        from src.models import RunStatus
        assert _determine_status(t1, t2, t3, t4) == RunStatus.PASSED


# ---------------------------------------------------------------------------
# Empty Input Handling
# ---------------------------------------------------------------------------


class TestEmptyInputs:
    def test_both_empty(self):
        src = _df([])
        tgt = _df([])
        t1, t2, t3, t4 = reconcile(src, tgt)
        assert t1.source_row_count == 0
        assert t1.target_row_count == 0
        assert t2.missing_count == 0
        assert t2.orphan_count == 0
        assert t4.total_mismatches == 0

    def test_empty_source_non_empty_target(self):
        src = _df([])
        tgt = _df([_clean_row("TX001")])
        t1, t2, t3, t4 = reconcile(src, tgt)
        assert t1.source_row_count == 0
        assert t1.target_row_count == 1
        assert t2.orphan_count == 1
        assert "TX001" in t2.orphan_in_target

    def test_non_empty_source_empty_target(self):
        src = _df([_clean_row("TX001")])
        tgt = _df([])
        t1, t2, t3, t4 = reconcile(src, tgt)
        assert t1.source_row_count == 1
        assert t1.target_row_count == 0
        assert t2.missing_count == 1
        assert "TX001" in t2.missing_in_target


# ---------------------------------------------------------------------------
# Set Discrepancies
# ---------------------------------------------------------------------------


class TestSetDiscrepancies:
    def test_missing_id_detected_in_tier2(self):
        src = _df([_clean_row("TX001"), _clean_row("TX002")])
        tgt = _df([_clean_row("TX001")])          # TX002 missing
        _, t2, _, _ = reconcile(src, tgt)
        assert "TX002" in t2.missing_in_target
        assert t2.missing_count == 1

    def test_orphan_id_detected_in_tier2(self):
        src = _df([_clean_row("TX001")])
        tgt = _df([_clean_row("TX001"), _clean_row("TX_PHANTOM")])
        _, t2, _, _ = reconcile(src, tgt)
        assert "TX_PHANTOM" in t2.orphan_in_target
        assert t2.orphan_count == 1

    def test_both_missing_and_orphan_detected(self):
        src = _df([_clean_row("TX001"), _clean_row("TX002")])
        tgt = _df([_clean_row("TX001"), _clean_row("TX_PHANTOM")])
        _, t2, _, _ = reconcile(src, tgt)
        assert "TX002" in t2.missing_in_target
        assert "TX_PHANTOM" in t2.orphan_in_target

    def test_matching_rows_not_flagged_as_missing_or_orphan(self):
        src = _df([_clean_row("TX001"), _clean_row("TX002")])
        tgt = _df([_clean_row("TX001"), _clean_row("TX002")])
        _, t2, _, _ = reconcile(src, tgt)
        assert "TX001" not in t2.missing_in_target
        assert "TX001" not in t2.orphan_in_target


# ---------------------------------------------------------------------------
# Duplicate Primary Keys
# ---------------------------------------------------------------------------


class TestDuplicateKeys:
    def test_source_duplicates_detected(self):
        src = _df([_clean_row("TX001"), _clean_row("TX001")])  # duplicate
        tgt = _df([_clean_row("TX001")])
        _, t2, _, _ = reconcile(src, tgt)
        assert t2.duplicates.source_duplicate_count >= 1
        assert "TX001" in t2.duplicates.source_duplicates

    def test_target_duplicates_detected(self):
        src = _df([_clean_row("TX001")])
        tgt = _df([_clean_row("TX001"), _clean_row("TX001")])  # duplicate
        _, t2, _, _ = reconcile(src, tgt)
        assert t2.duplicates.target_duplicate_count >= 1
        assert "TX001" in t2.duplicates.target_duplicates

    def test_duplicates_do_not_cause_false_missing(self):
        src = _df([_clean_row("TX001"), _clean_row("TX001")])
        tgt = _df([_clean_row("TX001")])
        _, t2, _, _ = reconcile(src, tgt)
        # TX001 is present in both (dedup logic), so NOT in missing list
        assert "TX001" not in t2.missing_in_target


# ---------------------------------------------------------------------------
# Null / Malformed Handling
# ---------------------------------------------------------------------------


class TestNullHandling:
    def test_null_pk_row_excluded_from_key_reconciliation(self):
        src = _df([_clean_row("TX001"), {"transaction_id": None, "customer_id": "X",
                   "transaction_timestamp": "2024-01-01T00:00:00Z",
                   "amount": 10.0, "currency": "USD", "status": "PENDING"}])
        tgt = _df([_clean_row("TX001")])
        _, t2, _, _ = reconcile(src, tgt)
        # Null PKs are excluded, so no false "missing" for the null row
        assert t2.missing_count == 0

    def test_null_amount_handled_in_aggregates(self):
        src = _df([_clean_row("TX001", amount=None)])
        tgt = _df([_clean_row("TX001", amount=None)])
        _, _, t3, _ = reconcile(src, tgt)
        # No error raised; metrics may be empty or zero
        assert isinstance(t3.total_metrics_checked, int)


# ---------------------------------------------------------------------------
# Floating-point Tolerance
# ---------------------------------------------------------------------------


class TestAggregateTolerances:
    def test_within_tolerance_passes(self):
        src = _df([_clean_row("TX001", amount=100.0000)])
        tgt = _df([_clean_row("TX001", amount=100.0005)])   # delta < tolerance
        _, _, t3, _ = reconcile(src, tgt, tolerance=0.001)
        assert t3.metrics_out_of_tolerance == 0

    def test_outside_tolerance_fails(self):
        src = _df([_clean_row("TX001", amount=100.0)])
        tgt = _df([_clean_row("TX001", amount=200.0)])   # large delta
        _, _, t3, _ = reconcile(src, tgt, tolerance=0.001)
        assert t3.metrics_out_of_tolerance > 0

    def test_exact_match_within_tolerance(self):
        rows = [_clean_row("TX001", amount=250.50)]
        src = _df(rows)
        tgt = _df(rows)
        _, _, t3, _ = reconcile(src, tgt)
        assert t3.aggregate_status == "MATCH"


# ---------------------------------------------------------------------------
# Cell-Level Diff (Tier 4)
# ---------------------------------------------------------------------------


class TestCellLevelDiffs:
    def test_amount_mismatch_detected(self):
        src = _df([_clean_row("TX001", amount=100.0)])
        tgt = _df([_clean_row("TX001", amount=200.0)])
        _, _, _, t4 = reconcile(src, tgt)
        assert t4.total_mismatches >= 1
        assert any(m.column_name == "amount" for m in t4.mismatches)

    def test_status_mismatch_detected(self):
        src = _df([_clean_row("TX001", status="COMPLETED")])
        tgt = _df([_clean_row("TX001", status="FAILED")])
        _, _, _, t4 = reconcile(src, tgt)
        assert t4.total_mismatches >= 1
        assert any(m.column_name == "status" for m in t4.mismatches)

    def test_no_mismatch_if_values_match(self):
        rows = [_clean_row("TX001", amount=500.0, status="PENDING")]
        _, _, _, t4 = reconcile(_df(rows), _df(rows))
        assert t4.total_mismatches == 0

    def test_mismatch_has_correct_transaction_id(self):
        src = _df([_clean_row("TX_ABC", amount=10.0)])
        tgt = _df([_clean_row("TX_ABC", amount=99.0)])
        _, _, _, t4 = reconcile(src, tgt)
        ids = {m.transaction_id for m in t4.mismatches}
        assert "TX_ABC" in ids

    def test_multiple_column_mismatches_on_same_row(self):
        src = _df([_clean_row("TX001", amount=100.0, status="COMPLETED")])
        tgt = _df([_clean_row("TX001", amount=999.0, status="FAILED")])
        _, _, _, t4 = reconcile(src, tgt)
        assert t4.total_mismatches >= 2

    def test_tier4_field_status_discrepancy(self):
        src = _df([_clean_row("TX001", status="COMPLETED")])
        tgt = _df([_clean_row("TX001", status="REFUNDED")])
        _, _, _, t4 = reconcile(src, tgt)
        assert t4.field_status == "DISCREPANCY"
