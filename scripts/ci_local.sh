#!/bin/bash
# Alpha Miner CI — 本地质量门禁
# 用法: bash scripts/ci_local.sh

set -e
cd "$(dirname "$0")/.."

echo "[1/3] Lint (ruff)..."
uv run ruff check src/ cli/ tests/
echo "  OK"

echo "[2/3] Tests (non-live)..."
uv run pytest tests/ -m "not live" -x --tb=short -q
echo "  OK"

echo "[3/3] Coverage baseline..."
uv run pytest tests/ -m "not live" --cov=src --cov-report=term-missing -q 2>/dev/null | tail -30

echo ""
echo "CI passed."
