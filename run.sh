#!/usr/bin/env bash
# =============================================================================
# run.sh – Single-command reproduction script for the Data Reconciliation Engine
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"

# On Windows (Git Bash / WSL), adjust paths:
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
  PYTHON="$VENV_DIR/Scripts/python"
fi

echo "============================================================"
echo "  Data Reconciliation Engine – Automated Run"
echo "============================================================"

# ── Step 1: Create virtual environment ──────────────────────────
if [[ ! -d "$VENV_DIR" ]]; then
  echo "[1/5] Creating virtual environment..."
  python3 -m venv "$VENV_DIR"
else
  echo "[1/5] Virtual environment already exists – skipping."
fi

# ── Step 2: Install dependencies ────────────────────────────────
echo "[2/5] Installing dependencies from requirements.txt..."
"$PYTHON" -m pip install --quiet --upgrade pip
"$PYTHON" -m pip install --quiet -r "$ROOT_DIR/requirements.txt"
echo "      ✓ Dependencies installed."

# ── Step 3: Generate sample dataset (1,000 rows) ────────────────
echo "[3/5] Generating sample dataset (1,000 rows)..."
"$PYTHON" -m src.generator --size 1000 --seed 42 --output-dir data/sample
echo "      ✓ data/sample/source.csv, target.csv, expected_truth.json written."

# ── Step 4: Run the reconciliation pipeline ─────────────────────
echo "[4/5] Running reconciliation pipeline..."
mkdir -p output
"$PYTHON" -m src.pipeline \
  --source data/sample/source.csv \
  --target data/sample/target.csv \
  --output-dir output \
  --tolerance 0.001
echo "      ✓ output/reconciliation_report.json"
echo "      ✓ output/quarantined_mismatches.csv"

# ── Step 5: Run pytest ───────────────────────────────────────────
echo "[5/5] Running pytest..."
"$PYTHON" -m pytest tests/ -v --tb=short 2>&1

echo ""
echo "============================================================"
echo "  ✅ All steps completed successfully!"
echo "  Report   : output/reconciliation_report.json"
echo "  Quarantine: output/quarantined_mismatches.csv"
echo "============================================================"

# ── Bonus: Generate + Benchmark large dataset (100,000 rows) ─────────────
echo ""
echo "[Bonus] Generating large dataset (100,000 rows)..."
mkdir -p data/large
"$PYTHON" -m src.generator --size 100000 --seed 42 --output-dir data/large
echo "        ✓ data/large/ written."

echo ""
echo "[Bonus] Running large-scale benchmark (100,000 rows)..."
mkdir -p output/large
BENCH_START=$(date +%s%N)
"$PYTHON" -m src.pipeline \
    --source data/large/source.csv \
    --target data/large/target.csv \
    --output-dir output/large \
    --tolerance 0.001
BENCH_END=$(date +%s%N)
ELAPSED_MS=$(( (BENCH_END - BENCH_START) / 1000000 ))
echo "        ✓ Benchmark complete."
printf "        ⏱  Elapsed: %d ms (%.2f s)\n" "$ELAPSED_MS" "$(echo "scale=2; $ELAPSED_MS / 1000" | bc)"

echo ""
echo "============================================================"
echo "  ✅ All steps completed (including large-scale benchmark)!"
echo "  Report    : output/reconciliation_report.json"
echo "  Quarantine: output/quarantined_mismatches.csv"
echo "  Benchmark : output/large/reconciliation_report.json"
echo "============================================================"
