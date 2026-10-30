"""
generator.py – Deterministic synthetic data generator for the
Data Reconciliation Engine.

Usage (CLI):
  python -m src.generator --size 1000 --seed 42 --output-dir data/sample

Injects controlled anomalies into the target dataset and emits an
expected_truth.json benchmark for test assertions.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.config import (
    ANOMALY_RATES,
    CURRENCIES,
    DEFAULT_SAMPLE_DIR,
    STATUSES,
    TRANSACTION_COLUMNS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _random_timestamp(rng: random.Random, base: datetime, window_days: int = 365) -> str:
    """Return a random ISO-8601 UTC timestamp within *window_days* of *base*."""
    offset = timedelta(seconds=rng.randint(0, window_days * 86_400))
    return (base - offset).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_transaction(rng: random.Random, base_date: datetime) -> dict:
    """Generate a single well-formed transaction record."""
    return {
        "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "customer_id": f"CUST{rng.randint(1000, 9999)}",
        "transaction_timestamp": _random_timestamp(rng, base_date),
        "amount": round(rng.uniform(0.01, 50_000.00), 4),
        "currency": rng.choice(CURRENCIES),
        "status": rng.choice(STATUSES),
    }


# ---------------------------------------------------------------------------
# Public generator API
# ---------------------------------------------------------------------------


def generate_datasets(
    size: int = 1000,
    seed: int = 42,
    output_dir: str = DEFAULT_SAMPLE_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Generate source and target DataFrames with injected anomalies.

    Returns
    -------
    tuple[source_df, target_df, truth_dict]
        *truth_dict* is the expected_truth benchmark for test assertions.
    """
    rng = random.Random(seed)
    base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # ------------------------------------------------------------------ #
    # 1. Generate clean source records                                     #
    # ------------------------------------------------------------------ #
    source_records: list[dict] = [_make_transaction(rng, base_date) for _ in range(size)]

    # Track truth
    truth: dict = {
        "seed": seed,
        "size": size,
        "missing_from_target": [],
        "orphan_in_target": [],
        "cell_discrepancies": [],
        "source_duplicates": [],
        "target_duplicates": [],
        "malformed_in_source": [],
    }

    # ------------------------------------------------------------------ #
    # 2. Malformed / null key records in source                            #
    # ------------------------------------------------------------------ #
    malformed_count = max(1, int(size * ANOMALY_RATES["malformed_records"]))
    malformed_indices = rng.sample(range(size), malformed_count)
    for idx in malformed_indices:
        rec = source_records[idx]
        truth["malformed_in_source"].append(rec["transaction_id"])
        if rng.random() < 0.5:
            source_records[idx]["transaction_id"] = None          # null PK
        else:
            source_records[idx]["transaction_timestamp"] = "NOT-A-DATE"  # bad timestamp

    # ------------------------------------------------------------------ #
    # 3. Duplicate primary keys in source (~1%)                            #
    # ------------------------------------------------------------------ #
    dup_count = max(1, int(size * ANOMALY_RATES["duplicate_keys"]))
    valid_indices = [i for i, r in enumerate(source_records) if r["transaction_id"] is not None]
    dup_indices = rng.sample(valid_indices, min(dup_count, len(valid_indices)))
    for idx in dup_indices:
        dup = copy.deepcopy(source_records[idx])
        truth["source_duplicates"].append(dup["transaction_id"])
        source_records.append(dup)

    source_df = pd.DataFrame(source_records, columns=TRANSACTION_COLUMNS)

    # ------------------------------------------------------------------ #
    # 4. Start building target from valid (non-null PK) source rows        #
    # ------------------------------------------------------------------ #
    valid_source = [r for r in source_records if r["transaction_id"] is not None]
    # Deduplicate for target baseline
    seen: set = set()
    unique_source: list[dict] = []
    for r in valid_source:
        if r["transaction_id"] not in seen:
            seen.add(r["transaction_id"])
            unique_source.append(copy.deepcopy(r))

    target_records: list[dict] = unique_source

    # ------------------------------------------------------------------ #
    # 5. Missing rows – drop ~2% from target                               #
    # ------------------------------------------------------------------ #
    missing_count = max(1, int(len(target_records) * ANOMALY_RATES["missing_rows"]))
    missing_indices = rng.sample(range(len(target_records)), min(missing_count, len(target_records)))
    missing_ids = [target_records[i]["transaction_id"] for i in missing_indices]
    truth["missing_from_target"] = missing_ids
    target_records = [r for i, r in enumerate(target_records) if i not in set(missing_indices)]

    # ------------------------------------------------------------------ #
    # 6. Orphan rows – inject ~1% phantom records in target                #
    # ------------------------------------------------------------------ #
    orphan_count = max(1, int(size * ANOMALY_RATES["orphan_rows"]))
    for _ in range(orphan_count):
        orphan = _make_transaction(rng, base_date)
        truth["orphan_in_target"].append(orphan["transaction_id"])
        target_records.append(orphan)

    # ------------------------------------------------------------------ #
    # 7. Cell-level discrepancies – alter ~3% of matching records          #
    # ------------------------------------------------------------------ #
    alterable = [r for r in target_records if r["transaction_id"] not in set(missing_ids + truth["orphan_in_target"])]
    cell_count = max(1, int(len(alterable) * ANOMALY_RATES["cell_discrepancies"]))
    cell_targets = rng.sample(alterable, min(cell_count, len(alterable)))
    mutable_columns = ["amount", "status"]
    for rec in cell_targets:
        col = rng.choice(mutable_columns)
        orig = rec[col]
        if col == "amount":
            rec[col] = round(orig * rng.uniform(1.05, 1.50), 4)   # inflate amount
        else:
            choices = [s for s in STATUSES if s != orig]
            rec[col] = rng.choice(choices)
        truth["cell_discrepancies"].append(
            {"transaction_id": rec["transaction_id"], "column": col}
        )

    # ------------------------------------------------------------------ #
    # 8. Duplicate primary keys in target (~1%)                            #
    # ------------------------------------------------------------------ #
    target_valid = [r for r in target_records if r["transaction_id"] is not None]
    tdup_count = max(1, int(len(target_valid) * ANOMALY_RATES["duplicate_keys"]))
    tdup_sample = rng.sample(target_valid, min(tdup_count, len(target_valid)))
    for rec in tdup_sample:
        truth["target_duplicates"].append(rec["transaction_id"])
        target_records.append(copy.deepcopy(rec))

    target_df = pd.DataFrame(target_records, columns=TRANSACTION_COLUMNS)

    # ------------------------------------------------------------------ #
    # 9. Persist to output_dir                                             #
    # ------------------------------------------------------------------ #
    _write_outputs(source_df, target_df, truth, output_dir)

    logger.info(
        "Generated source=%d rows, target=%d rows | missing=%d orphan=%d cells=%d",
        len(source_df),
        len(target_df),
        len(truth["missing_from_target"]),
        len(truth["orphan_in_target"]),
        len(truth["cell_discrepancies"]),
    )

    return source_df, target_df, truth


def _write_outputs(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    truth: dict,
    output_dir: str,
) -> None:
    """Write source.csv, target.csv, and expected_truth.json atomically."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    src_path = out / "source.csv"
    tgt_path = out / "target.csv"
    truth_path = out / "expected_truth.json"

    # Write to temps, then rename (atomic-ish on same filesystem)
    _atomic_write_csv(source_df, src_path)
    _atomic_write_csv(target_df, tgt_path)

    tmp_truth = truth_path.with_suffix(".tmp")
    tmp_truth.write_text(json.dumps(truth, indent=2, default=str), encoding="utf-8")
    _atomic_replace(tmp_truth, truth_path)

    logger.debug("Wrote %s, %s, %s", src_path, tgt_path, truth_path)


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    _atomic_replace(tmp, path)


def _atomic_replace(src: Path, dst: Path) -> None:
    """Cross-platform atomic rename."""
    if dst.exists():
        dst.unlink()
    src.rename(dst)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def _cli() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Data Reconciliation Engine – Data Generator")
    parser.add_argument("--size", type=int, default=1000, help="Number of source records")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output-dir", default=DEFAULT_SAMPLE_DIR, help="Output directory")
    args = parser.parse_args()
    generate_datasets(size=args.size, seed=args.seed, output_dir=args.output_dir)
    print(f"✓ Data generated in '{args.output_dir}'")


if __name__ == "__main__":
    _cli()
