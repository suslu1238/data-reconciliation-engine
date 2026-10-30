"""
test_generator.py – Tests for the deterministic synthetic data generator.

Covers:
  - Row counts match requested size (approximately, accounting for dups/additions)
  - Determinism: same seed produces identical data
  - Anomaly distribution: missing, orphan, cell-diff, dup-key, malformed
  - Expected truth file correctness
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.generator import generate_datasets


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Generate a small dataset once for the module.

    generate_datasets() returns (source_df, target_df, truth_dict).
    We wrap it in a 4-tuple by including the output directory so tests
    can inspect the written files.
    """
    out = tmp_path_factory.mktemp("gen_output")
    result = generate_datasets(size=200, seed=42, output_dir=str(out))
    # result is (source_df, target_df, truth_dict)
    source, target, truth = result
    return source, target, truth, out


# ---------------------------------------------------------------------------
# Basic integrity
# ---------------------------------------------------------------------------


class TestBasicIntegrity:
    def test_source_has_expected_columns(self, generated):
        source, *_ = generated
        assert list(source.columns) == [
            "transaction_id", "customer_id", "transaction_timestamp",
            "amount", "currency", "status",
        ]

    def test_target_has_expected_columns(self, generated):
        _, target, *_ = generated
        assert list(target.columns) == [
            "transaction_id", "customer_id", "transaction_timestamp",
            "amount", "currency", "status",
        ]

    def test_source_size_is_at_least_requested(self, generated):
        source, _, truth, _ = generated
        # Source = requested rows + dup rows injected
        assert len(source) >= 200

    def test_currencies_are_valid(self, generated):
        source, target, *_ = generated
        valid = {"USD", "EUR", "INR", "GBP"}
        for df in (source, target):
            assert set(df["currency"].dropna().unique()).issubset(valid)

    def test_statuses_are_valid(self, generated):
        source, target, *_ = generated
        valid = {"COMPLETED", "PENDING", "FAILED", "REFUNDED"}
        for df in (source, target):
            assert set(df["status"].dropna().unique()).issubset(valid)


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_same_source(self, tmp_path):
        s1, _, _ = generate_datasets(size=100, seed=99, output_dir=str(tmp_path / "a"))
        s2, _, _ = generate_datasets(size=100, seed=99, output_dir=str(tmp_path / "b"))
        pd.testing.assert_frame_equal(s1.reset_index(drop=True), s2.reset_index(drop=True))

    def test_different_seeds_different_data(self, tmp_path):
        s1, _, _ = generate_datasets(size=100, seed=1, output_dir=str(tmp_path / "s1"))
        s2, _, _ = generate_datasets(size=100, seed=2, output_dir=str(tmp_path / "s2"))
        # Very unlikely to be identical
        assert not s1["transaction_id"].equals(s2["transaction_id"])


# ---------------------------------------------------------------------------
# Anomaly distribution
# ---------------------------------------------------------------------------


class TestAnomalyDistribution:
    def test_missing_rows_injected(self, generated):
        _, _, truth, _ = generated
        assert len(truth["missing_from_target"]) >= 1

    def test_orphan_rows_injected(self, generated):
        _, _, truth, _ = generated
        assert len(truth["orphan_in_target"]) >= 1

    def test_cell_discrepancies_injected(self, generated):
        _, _, truth, _ = generated
        assert len(truth["cell_discrepancies"]) >= 1

    def test_source_duplicates_injected(self, generated):
        _, _, truth, _ = generated
        assert len(truth["source_duplicates"]) >= 1

    def test_missing_ids_not_in_target(self, generated):
        source, target, truth, _ = generated
        target_ids = set(target["transaction_id"].dropna().tolist())
        for tid in truth["missing_from_target"]:
            assert tid not in target_ids, f"{tid} should be missing from target"

    def test_orphan_ids_not_in_source(self, generated):
        source, target, truth, _ = generated
        source_ids = set(source["transaction_id"].dropna().tolist())
        for tid in truth["orphan_in_target"]:
            assert tid not in source_ids, f"{tid} should be orphan (not in source)"

    def test_malformed_records_have_null_or_bad_ts(self, generated):
        source, _, truth, _ = generated
        null_pks = source[source["transaction_id"].isna()]
        bad_ts = source[source["transaction_timestamp"] == "NOT-A-DATE"]
        total_malformed = len(null_pks) + len(bad_ts)
        # At least one malformed record should exist
        assert total_malformed >= 1


# ---------------------------------------------------------------------------
# Expected truth file
# ---------------------------------------------------------------------------


class TestExpectedTruthFile:
    def test_truth_file_exists(self, generated):
        _, _, _, out = generated
        assert (out / "expected_truth.json").exists()

    def test_truth_file_valid_json(self, generated):
        _, _, _, out = generated
        content = json.loads((out / "expected_truth.json").read_text())
        assert isinstance(content, dict)

    def test_truth_has_required_keys(self, generated):
        _, _, truth, _ = generated
        for key in (
            "seed", "size", "missing_from_target", "orphan_in_target",
            "cell_discrepancies", "source_duplicates", "target_duplicates",
        ):
            assert key in truth, f"Missing key: {key}"

    def test_source_csv_written(self, generated):
        _, _, _, out = generated
        assert (out / "source.csv").exists()

    def test_target_csv_written(self, generated):
        _, _, _, out = generated
        assert (out / "target.csv").exists()
