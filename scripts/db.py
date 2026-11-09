"""Shared database helpers: .env loading and connection handling (stdlib + psycopg)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "data"))  # make generate_transactions importable

EXIT_DB_UNAVAILABLE = 3


def load_env(path: Path = ROOT / ".env") -> None:
    """Minimal KEY=VALUE parser. Real environment variables take precedence."""
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def conninfo() -> dict:
    load_env()
    return {
        "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.environ.get("POSTGRES_HOST_PORT", "5433")),
        "dbname": os.environ.get("POSTGRES_DB", "txlab"),
        "user": os.environ.get("POSTGRES_USER", "txlab"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
        "connect_timeout": 10,
        "application_name": "sql-query-optimization-lab",
    }


def connect(**overrides) -> psycopg.Connection:
    """Connect or exit with a clear message and EXIT_DB_UNAVAILABLE."""
    info = conninfo() | overrides
    try:
        # autocommit: explicit `with conn.transaction()` blocks are then real
        # top-level transactions (not savepoints inside an implicit one).
        conn = psycopg.connect(**info, autocommit=True)
    except psycopg.OperationalError as exc:
        safe = {k: v for k, v in info.items() if k != "password"}
        print(f"ERROR: cannot connect to PostgreSQL {safe}: {exc}".strip(), file=sys.stderr)
        print("Is the database running? Try: docker compose up -d --wait", file=sys.stderr)
        sys.exit(EXIT_DB_UNAVAILABLE)
    with conn.cursor() as cur:
        cur.execute("SET TimeZone = 'UTC'")
    return conn
