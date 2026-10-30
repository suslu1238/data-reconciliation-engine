# Prompt History Log — Data Reconciliation Engine

**Project:** Enterprise-Grade Data Reconciliation Engine  
**Domain:** Financial / Payment Transactions  
**Tech Stack:** Python 3.10+ · DuckDB · Pandas · Pydantic v2 · pytest

---

## Prompt 1 — Data Contract & Schema Design

**Phase:** Architecture & Discovery  
**Objective:** Establish the canonical transaction schema and enforce it as a single source of truth.

> "You are a Senior Data Engineer designing a financial transaction reconciliation system. Define the canonical schema for a `transactions` entity with the following attributes: `transaction_id` (UUID, PK), `customer_id` (VARCHAR), `transaction_timestamp` (ISO-8601 UTC), `amount` (DOUBLE), `currency` (enum: USD/EUR/INR/GBP), `status` (enum: COMPLETED/PENDING/FAILED/REFUNDED).
>
> Provide: (1) the DuckDB-compatible column type map, (2) nullability constraints — specifically that `transaction_id` is the business key and must never be NULL for reconciliation purposes, (3) a Pydantic v2 configuration for runtime validation, and (4) the rationale for storing `transaction_timestamp` as VARCHAR rather than a native TIMESTAMP type in the ingestion layer."

**Design Decision:** VARCHAR timestamp allows ingestion of malformed date strings without silent coercion — reconciliation explicitly validates format quality rather than silently rejecting rows at parse time.

---

## Prompt 2 — Deterministic Anomaly Injection Strategy

**Phase:** Data Generation Design  
**Objective:** Define a reproducible, multi-class anomaly injection model for synthetic datasets.

> "Design a deterministic synthetic data generator for financial transaction reconciliation testing.  
> Requirements:  
> - Seed-controlled PRNG (`seed=42`) for full reproducibility  
> - Five independent anomaly classes injectable into the target dataset:  
>   (a) Missing rows (~2% of source records dropped),  
>   (b) Orphan/phantom rows (~1% target-only records),  
>   (c) Cell-level field corruption (~3% of matching records; amount inflated or status changed),  
>   (d) Duplicate primary keys (~1% duplicated in source and target independently),  
>   (e) Malformed/null key records (~1% null PKs or invalid timestamps).  
> - A separate `expected_truth.json` file specifying exact anomaly counts and IDs for test assertion.  
>
> Explain how to guarantee that orphan IDs cannot collide with existing source IDs, and how cell corruption avoids modifying records that are already in the missing-rows set."

**Design Decision:** Orphan IDs are generated via `uuid.UUID(int=rng.getrandbits(128))` using a fresh seeded RNG stream, making UUID collision probability cryptographically negligible. Cell corruption targets are drawn exclusively from non-missing, non-orphan rows using a set-difference filter.

---

## Prompt 3 — DuckDB Anti-Join Architecture for Key Completeness

**Phase:** Core Engine Design  
**Objective:** Implement Tier 2 — Business Key Completeness using DuckDB vectorized anti-joins.

> "Design a DuckDB-based Tier 2 reconciliation module that detects: (1) records present in source but absent from target (missing rows), (2) records present in target but absent from source (orphan rows), and (3) duplicate primary keys within each dataset.  
>
> Key constraints:  
> - Use `LEFT ANTI JOIN` syntax for set-difference operations (explain why LEFT ANTI JOIN is superior to NOT IN or NOT EXISTS for large datasets in columnar engines)  
> - Apply deduplication CTEs before anti-joins to prevent false-positive missing detections caused by source-side duplicate keys  
> - Exclude NULL primary keys from all set operations  
> - Return results as typed Pydantic v2 models  
>
> Provide the SQL and Python wrapper, and explain the implicit ordering guarantee (or lack thereof) in DuckDB result sets."

**Design Decision:** `LEFT ANTI JOIN` avoids the `NULL`-propagation pitfall of `NOT IN` and is natively optimized in DuckDB's hash-join executor. CTEs deduplicating before joins prevent the scenario where a duplicated source key incorrectly appears as missing when only one instance matches.

---

## Prompt 4 — Vectorized Aggregate Reconciler with Tolerance Bands

**Phase:** Tier 3 Design  
**Objective:** Compare financial aggregates across datasets with configurable floating-point tolerance.

> "Design a DuckDB-powered Tier 3 aggregate reconciler for financial transaction data.  
>
> Requirements:  
> - Compute SUM, AVG, MIN, MAX of `amount` from both source and target  
> - Run at three levels of granularity: global, per-currency, per-status, and per currency×status combination  
> - Apply a configurable absolute tolerance (`default=0.001`) for floating-point comparison  
> - Produce a list of `AggregateMetric` Pydantic objects, each containing: metric name, source value, target value, absolute delta, within_tolerance flag, and tolerance used  
>
> Explain why absolute tolerance is preferred over relative tolerance (`|delta/src|`) for financial amount aggregation, and what edge cases arise with zero-sum or null-amount partitions."

**Design Decision:** Absolute tolerance avoids division-by-zero when source aggregate is 0 or NULL. For financial systems, the materiality threshold is a fixed monetary unit (e.g., $0.001), not a percentage, aligning with accounting precision standards.

---

## Prompt 5 — Field-Level Cell Comparator with Null-Safe Semantics

**Phase:** Tier 4 Design  
**Objective:** Perform row-level cell-by-cell comparison on matched transaction records.

> "Implement a DuckDB Tier 4 field-level comparator that:  
> - Inner-joins source and target on `transaction_id` (using deduplication CTEs to handle duplicate PKs safely)  
> - Uses `IS DISTINCT FROM` for null-safe cell comparison (explain why `!=` is insufficient for nullable columns)  
> - Evaluates all non-key columns: `customer_id`, `transaction_timestamp`, `amount`, `currency`, `status`  
> - Returns a list of `CellMismatch` objects: one per (transaction_id, column_name) pair with source_value and target_value  
>
> Discuss the performance trade-off between fetching a wide pivot result and iterating column-by-column, particularly for datasets exceeding 500k rows."

**Design Decision:** `IS DISTINCT FROM` (SQL-standard null-safe inequality) correctly identifies `NULL IS DISTINCT FROM 'x'` as true, whereas `NULL != 'x'` evaluates to NULL and silently swallows the mismatch. For datasets >500k rows, pivoting to a narrow `(id, column, src_val, tgt_val)` format via `UNPIVOT` would reduce Python-side iteration but introduces cross-join complexity.

---

## Prompt 6 — Atomic File I/O and Idempotency Guarantees

**Phase:** Pipeline Reliability Design  
**Objective:** Ensure that repeated pipeline runs cannot corrupt outputs or accumulate duplicate records.

> "Design an idempotent output strategy for a reconciliation pipeline that writes: (1) `reconciliation_report.json` and (2) `quarantined_mismatches.csv`.  
>
> Constraints:  
> - Re-running the pipeline with identical inputs must produce semantically identical outputs (excluding run_id and timestamp)  
> - The quarantine CSV must never accumulate rows from prior runs  
> - File writes must be atomic to prevent partial/corrupt files in case of crash  
>
> Describe the write-then-rename pattern, why it is safe on POSIX but not fully atomic on Windows NTFS (and what the mitigation is), and how you would extend this to S3-based object store outputs using multipart upload + atomic PUT."

**Design Decision:** `write → rename` is the standard atomic write idiom. On Windows, `os.replace()` (used here) is atomic within the same volume. For S3, etag-based conditional PUTs with a generation tag achieve equivalent idempotency semantics.

---

## Prompt 7 — Comprehensive Edge-Case Test Matrix

**Phase:** QA Design  
**Objective:** Define the complete pytest test matrix covering all reconciliation failure modes.

> "Design a comprehensive pytest test matrix for a 4-tier data reconciliation engine.  
>
> The matrix must include:  
> - Happy path with N identical records (assert 0 discrepancies)  
> - Empty source, empty target, both empty  
> - Set discrepancies: exact isolation of missing and phantom IDs  
> - Duplicate primary keys in source only, target only, and both  
> - NULL primary keys — verify they are excluded from set operations without raising exceptions  
> - Floating-point aggregates: test within-tolerance and out-of-tolerance boundary conditions (use values at tolerance boundary ± ε)  
> - Cell-level mismatches: single column, multi-column on same row  
> - Idempotency: two sequential runs produce identical quarantine row counts  
> - Atomic overwrite: second run to same directory does not double-append quarantine rows  
>
> Explain how to share expensive fixture state across test classes using `scope='module'` and the trade-offs vs. `scope='function'`."

**Design Decision:** `scope='module'` for data generation fixtures reduces test runtime by ~60% for IO-heavy generation, at the cost of test isolation. For reconciliation tests we use `scope='function'` (default) to guarantee state independence per test case.

---

## Prompt 8 — Performance Benchmarking & Scalability Analysis

**Phase:** Performance Engineering  
**Objective:** Establish performance baselines and identify the in-memory DuckDB scalability ceiling.

> "Benchmark the Data Reconciliation Engine at 1,000 rows vs. 100,000 rows and report:  
> - Wall-clock execution time per tier  
> - Peak memory usage (RSS)  
> - Throughput in rows/second  
>
> Then analyze: at what dataset size does in-memory DuckDB become the wrong choice vs. a distributed engine like Apache Spark or Flink? Consider factors: RAM limits, network I/O for distributed shuffle, serialization overhead, and developer operational complexity.  
>
> Provide a decision matrix comparing DuckDB, Spark, and Flink across dimensions: dataset size, latency requirement, infra complexity, and cost."

**Performance Note (empirical — measured on Intel Core i7 / 16 GB RAM / Windows 11):**  
| Scale | Engine | Measured Time |
|---|---|---|
| 1,000 rows | DuckDB in-memory | **~0.32s** |
| 100,000 rows | DuckDB in-memory | **~4.44s** |
| 10M rows | DuckDB (disk-spillable) | ~45–90s (estimated) |
| 100M+ rows | Spark on cluster | minutes (horizontally scalable) |

---

## Prompt 9 — Design Trade-offs: DuckDB vs. Distributed Systems

**Phase:** Architecture Review  
**Objective:** Document the explicit engineering trade-offs of the chosen DuckDB-based approach.

> "You are an engineering lead presenting to a principal architect. Defend the choice of DuckDB as the reconciliation engine for datasets up to ~50M rows. Address:  
>
> (1) Columnar execution model and its advantage for aggregate-heavy workloads  
> (2) Single-node RAM constraint: what happens when data exceeds available memory?  
> (3) Operational simplicity: no cluster management vs. PySpark's YARN/Kubernetes dependency  
> (4) When to migrate: migration triggers (dataset size > RAM, multi-tenant parallelism, streaming requirements)  
> (5) Incremental migration path: DuckDB → Spark (or DuckDB → MotherDuck for cloud scale-out)  
>
> Format as a formal trade-off document with a Risk/Reward matrix."

**Key Trade-off Summary:**

| Dimension | DuckDB | PySpark |
|---|---|---|
| Dataset Limit | ~RAM (typically 8–64 GB) | Petabyte-scale |
| Setup Complexity | `pip install duckdb` | Cluster + JVM + YARN/K8s |
| Developer UX | Python-native | Requires Spark API knowledge |
| Latency (small data) | Sub-second | 10–30s cluster startup |
| Cost | Zero infra | EC2/GKE + licensing |
| Streaming | Not native | Structured Streaming |

---

## Prompt 10 — Packaging, Automation & Reproducibility

**Phase:** Packaging & DevOps  
**Objective:** Design a single-command, fully automated, cross-platform reproduction script that evaluators can run with zero manual steps.

> "Design a `run.sh` shell script that packages and reproduces the entire Data Reconciliation Engine pipeline end-to-end for an evaluator on a fresh Linux/macOS machine.
>
> Requirements:
> - POSIX-compliant `bash` (`#!/usr/bin/env bash`) with `set -euo pipefail` for strict error propagation
> - Cross-platform `PYTHON` path detection (Unix `bin/python` vs Windows `Scripts/python`)
> - Step 1: Create `.venv` virtual environment if absent, skip if already exists (idempotent)
> - Step 2: Install all dependencies from `requirements.txt` via pip
> - Step 3: Generate 1,000-row sample dataset with seed-controlled PRNG
> - Step 4: Execute the 4-tier reconciliation pipeline against generated sample
> - Step 5: Run the full pytest suite (52 tests, 0 warnings)
> - Step 6 (Bonus): Generate 100,000-row large dataset and execute a timed benchmark, printing elapsed time in milliseconds and seconds
>
> Explain the `.gitignore` strategy: `data/sample/` is committed so evaluators can run immediately; `data/large/` and `output/large/` are excluded to keep the repo lightweight."

**Design Decision:** `set -euo pipefail` ensures any failing sub-command propagates as a script failure — preventing silent partial runs. The `OSTYPE` detection block handles Git Bash (`msys`) / Cygwin vs native POSIX without requiring evaluators to modify the script. The timed benchmark uses `date +%s%N` (nanoseconds) for millisecond precision and `bc` for floating-point division, available on all POSIX systems. All source code paths use `pathlib.Path` / `os.path.join` — no hardcoded Windows backslashes appear anywhere in `src/` or `tests/`.
