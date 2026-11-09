#!/usr/bin/env bash
# One-command setup: start PostgreSQL, install deps, generate + load data, validate.
# Safe to rerun: the loader skips if the same dataset is already loaded, and a
# reload replaces data inside one transaction (never appends duplicates).
# Extra arguments are passed to the loader, e.g. scripts/setup.sh --count 100000
set -euo pipefail
cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "ERROR: docker not found" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon is not running" >&2; exit 1; }

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "created .env from .env.example (local development defaults)"
fi

echo "==> starting PostgreSQL (waits for health check)"
docker compose up -d --wait

if [[ ! -x .venv/bin/python ]]; then
  echo "==> creating virtualenv"
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -q -r requirements.txt

echo "==> generating and loading data"
.venv/bin/python scripts/load_data.py "$@"

echo "==> validating"
.venv/bin/python scripts/validate.py
