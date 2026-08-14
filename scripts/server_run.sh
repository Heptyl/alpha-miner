#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${ALPHA_MINER_IMAGE:-alpha-miner:local}"
RUNTIME_DIR="$ROOT/.server-runtime"
NATIVE_PYTHON="$RUNTIME_DIR/python/bin/python3"
ACTION="${1:-status}"
shift || true

build_image() {
  docker build -t "$IMAGE" "$ROOT"
}

prepare_native_runtime() {
  if [[ ! -x "$NATIVE_PYTHON" ]]; then
    archive="$(find "$RUNTIME_DIR" -maxdepth 1 -name 'cpython-3.12*-install_only_stripped.tar.gz' -print -quit 2>/dev/null || true)"
    if [[ -z "$archive" ]]; then
      echo "No Docker image or offline Python archive found." >&2
      echo "Run scripts/remote_compute.ps1 -Action build from Windows first." >&2
      return 1
    fi
    tar -xzf "$archive" -C "$RUNTIME_DIR"
  fi
  [[ -f "$RUNTIME_DIR/active-site.txt" ]] || {
    echo "Offline site-packages are missing; run the Windows build action." >&2
    return 1
  }
}

run_python() {
  if docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker run --rm --init \
      --user "$(id -u):$(id -g)" \
      -e PYTHONUTF8=1 \
      -e PYTHONIOENCODING=utf-8 \
      -e HTTP_PROXY -e HTTPS_PROXY -e ALL_PROXY \
      -e http_proxy -e https_proxy -e all_proxy \
      -e ALPHA_MINER_USE_PROXY \
      -v "$ROOT:/workspace" \
      -w /workspace \
      "$IMAGE" "$@"
    return
  fi

  prepare_native_runtime
  site_dir="$(tr -d '\r\n' < "$RUNTIME_DIR/active-site.txt")"
  PYTHONUTF8=1 PYTHONIOENCODING=utf-8 \
    PYTHONPATH="$ROOT:$RUNTIME_DIR/$site_dir${PYTHONPATH:+:$PYTHONPATH}" \
    "$NATIVE_PYTHON" "$@"
}

case "$ACTION" in
  build)
    if [[ -d "$RUNTIME_DIR" ]] && find "$RUNTIME_DIR" -maxdepth 1 -name 'cpython-3.12*-install_only_stripped.tar.gz' -print -quit | grep -q .; then
      prepare_native_runtime
      "$NATIVE_PYTHON" --version
      echo "runtime=native-offline"
    else
      build_image
    fi
    ;;
  collect)
    run_python -m cli.collect --today "$@"
    ;;
  evolve)
    generations="${ALPHA_MINER_GENERATIONS:-10}"
    population="${ALPHA_MINER_POPULATION:-16}"
    workers="${ALPHA_MINER_WORKERS:-16}"
    run_python -m cli.mine evolve \
      --generations "$generations" \
      --population "$population" \
      --workers "$workers" \
      "$@"
    ;;
  daily)
    workers="${ALPHA_MINER_WORKERS:-16}"
    run_python -m cli daily --evolution-workers "$workers" "$@"
    ;;
  snapshot)
    run_python -c 'import sqlite3; from pathlib import Path; src=Path("data/alpha_miner.db"); dst=Path("reports/alpha_miner.snapshot.db"); dst.parent.mkdir(parents=True, exist_ok=True); source=sqlite3.connect(src); target=sqlite3.connect(dst); source.backup(target); target.close(); source.close(); print(dst)'
    ;;
  activate-data)
    run_python scripts/activate_data.py
    ;;
  test)
    run_python -m pytest "$@"
    ;;
  python)
    run_python "$@"
    ;;
  status)
    echo "root=$ROOT"
    echo "image=$IMAGE"
    docker image inspect "$IMAGE" --format 'created={{.Created}} size={{.Size}}' 2>/dev/null || true
    if [[ -x "$NATIVE_PYTHON" ]]; then
      echo "runtime=native-offline"
      "$NATIVE_PYTHON" --version
    elif [[ -d "$RUNTIME_DIR" ]]; then
      echo "runtime=native-offline (archive staged, build pending)"
    fi
    if [[ -f "$ROOT/data/alpha_miner.db" ]]; then
      ls -lh "$ROOT/data/alpha_miner.db"
    else
      echo "database=missing"
    fi
    ;;
  *)
    echo "usage: $0 {build|collect|evolve|daily|snapshot|activate-data|test|python|status} [args...]" >&2
    exit 2
    ;;
esac
