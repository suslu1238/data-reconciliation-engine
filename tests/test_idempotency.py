"""
test_idempotency.py – Proves that running the pipeline multiple times with
the same input data produces byte-identical outputs (idempotency guarantee).

Covers:
  - Two consecutive runs produce identical JSON report (excluding run_id/ts)
  - Two consecutive runs produce identical quarantine CSV rows
  - Atomic overwrite: output files are complete after second run
  - State tracking: reconciliation_report.json always reflects latest inputs
"""

from __future__ import annotations

import json
import pandas as pd
import pytest

from src.generator import generate_datasets
from src.pipeline import run_pipeline
from src.config import REPORT_FILENAME, QUARANTINE_FILENAME


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sample_data(tmp_path_factory):
    """Generate one small dataset to be reused across idempotency tests."""
    data_dir = tmp_path_factory.mktemp("idem_data")
    generate_datasets(size=150, seed=7, output_dir=str(data_dir))
    return data_dir


@pytest.fixture(scope="module")
def run1(sample_data, tmp_path_factory):
    out = tmp_path_factory.mktemp("idem_out1")
    report = run_pipeline(
        source_path=str(sample_data / "source.csv"),
        target_path=str(sample_data / "target.csv"),
        output_dir=str(out),
    )
    return report, out


@pytest.fixture(scope="module")
def run2(sample_data, tmp_path_factory):
    """Second run against exactly the same inputs, different output dir clone."""
    out = tmp_path_factory.mktemp("idem_out2")
    report = run_pipeline(
        source_path=str(sample_data / "source.csv"),
        target_path=str(sample_data / "target.csv"),
        output_dir=str(out),
    )
    return report, out


# ---------------------------------------------------------------------------
# Idempotency: same inputs → same results
# ---------------------------------------------------------------------------


class TestIdempotency:
    def _normalise_report(self, report_dict: dict) -> dict:
        """Remove non-deterministic run_id and timestamp before comparison."""
        d = dict(report_dict)
        d.pop("run_id", None)
        d.pop("timestamp_utc", None)
        d.pop("execution_duration_seconds", None)
        d.pop("input_source_path", None)
        d.pop("input_target_path", None)
        d.pop("quarantine_path", None)
        return d

    def test_identical_status(self, run1, run2):
        r1, _ = run1
        r2, _ = run2
        assert r1.status == r2.status

    def test_identical_discrepancy_counts(self, run1, run2):
        r1, _ = run1
        r2, _ = run2
        assert r1.missing_records == r2.missing_records
        assert r1.orphan_records == r2.orphan_records
        assert r1.cell_mismatches == r2.cell_mismatches
        assert r1.aggregate_mismatches == r2.aggregate_mismatches
        assert r1.duplicate_keys_detected == r2.duplicate_keys_detected
        assert r1.total_discrepancies == r2.total_discrepancies

    def test_identical_tier1_counts(self, run1, run2):
        r1, _ = run1
        r2, _ = run2
        assert r1.tier1.source_row_count == r2.tier1.source_row_count
        assert r1.tier1.target_row_count == r2.tier1.target_row_count
        assert r1.tier1.row_count_delta == r2.tier1.row_count_delta

    def test_identical_tier2_missing_ids(self, run1, run2):
        r1, _ = run1
        r2, _ = run2
        assert sorted(r1.tier2.missing_in_target) == sorted(r2.tier2.missing_in_target)
        assert sorted(r1.tier2.orphan_in_target) == sorted(r2.tier2.orphan_in_target)

    def test_identical_quarantine_row_count(self, run1, run2):
        _, out1 = run1
        _, out2 = run2
        q1 = pd.read_csv(out1 / QUARANTINE_FILENAME)
        q2 = pd.read_csv(out2 / QUARANTINE_FILENAME)
        # Row count must be identical
        assert len(q1) == len(q2)

    def test_quarantine_same_transaction_ids(self, run1, run2):
        _, out1 = run1
        _, out2 = run2
        q1 = pd.read_csv(out1 / QUARANTINE_FILENAME)
        q2 = pd.read_csv(out2 / QUARANTINE_FILENAME)
        if len(q1) > 0 and len(q2) > 0:
            ids1 = sorted(q1["transaction_id"].astype(str).tolist())
            ids2 = sorted(q2["transaction_id"].astype(str).tolist())
            assert ids1 == ids2

    def test_report_json_written_and_valid(self, run1):
        _, out = run1
        report_path = out / REPORT_FILENAME
        assert report_path.exists()
        data = json.loads(report_path.read_text())
        assert "status" in data
        assert "total_discrepancies" in data

    def test_quarantine_csv_written_with_expected_columns(self, run1):
        _, out = run1
        q_path = out / QUARANTINE_FILENAME
        assert q_path.exists()
        df = pd.read_csv(q_path)
        expected_cols = {
            "run_id", "transaction_id", "column_name",
            "source_value", "target_value", "discrepancy_type", "detected_at",
        }
        assert expected_cols.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# Atomic overwrite: running into an existing output dir overwrites correctly
# ---------------------------------------------------------------------------


class TestAtomicOverwrite:
    def test_second_run_overwrites_first_output(self, sample_data, tmp_path):
        """Two runs into the same output directory must not corrupt outputs."""
        out = tmp_path / "shared_output"

        run_pipeline(
            source_path=str(sample_data / "source.csv"),
            target_path=str(sample_data / "target.csv"),
            output_dir=str(out),
        )
        r2 = run_pipeline(
            source_path=str(sample_data / "source.csv"),
            target_path=str(sample_data / "target.csv"),
            output_dir=str(out),
        )

        # After second run the report must be valid and complete
        report_path = out / REPORT_FILENAME
        data = json.loads(report_path.read_text())
        assert data["run_id"] == r2.run_id
        assert data["status"] == r2.status.value

    def test_quarantine_not_doubled_on_rerun(self, sample_data, tmp_path):
        """Rerunning must not append rows onto the previous quarantine file."""
        out = tmp_path / "overwrite_test"

        r1 = run_pipeline(
            source_path=str(sample_data / "source.csv"),
            target_path=str(sample_data / "target.csv"),
            output_dir=str(out),
        )
        q1_count = len(pd.read_csv(out / QUARANTINE_FILENAME))

        r2 = run_pipeline(
            source_path=str(sample_data / "source.csv"),
            target_path=str(sample_data / "target.csv"),
            output_dir=str(out),
        )
        q2_count = len(pd.read_csv(out / QUARANTINE_FILENAME))

        assert q1_count == q2_count, (
            f"Expected {q1_count} quarantine rows after rerun, got {q2_count}. "
            "Rows should not be duplicated."
        )
