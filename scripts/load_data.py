#!/usr/bin/env python3
"""Generate the synthetic dataset and bulk-load it into PostgreSQL with COPY.

Rows stream straight from the generator into COPY in batches, so memory is
bounded and no intermediate CSV is written.

Idempotency: the load runs in ONE transaction that TRUNCATEs the table, COPYs
the rows, verifies the inserted count and records metadata. Any failure rolls
the whole thing back, so the table is never left half-loaded or duplicated.
If the table already holds the dataset for the same configuration, the load is
skipped unless --force is given.

Exit codes: 0 ok / skipped, 1 load or verification failure, 2 bad arguments,
3 database unreachable.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import psycopg

from db import ROOT, connect

import generate_transactions as gen

SCHEMA_FILE = ROOT / "sql" / "schema.sql"
REPORT_FILE = ROOT / "results" / "load_report.json"


def apply_schema(conn: psycopg.Connection) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text())


def already_loaded(conn: psycopg.Connection, cfg: gen.GeneratorConfig) -> bool:
    with conn.cursor() as cur:
        cur.execute("""SELECT generator_version, seed, record_count, start_date, end_date,
                              num_merchants FROM dataset_metadata WHERE id = 1""")
        row = cur.fetchone()
        if row is None:
            return False
        expected = (gen.GENERATOR_VERSION, cfg.seed, cfg.count, cfg.start_date,
                    cfg.end_date, cfg.num_merchants)
        if tuple(row) != expected:
            return False
        cur.execute("SELECT count(*) FROM transactions")
        return cur.fetchone()[0] == cfg.count


def load(conn: psycopg.Connection, cfg: gen.GeneratorConfig, batch_size: int) -> dict:
    fp = gen.Fingerprinter()
    generated = 0
    gen_seconds = copy_seconds = 0.0
    started = time.perf_counter()

    assert conn.autocommit, "load() needs a top-level transaction, not a savepoint"
    with conn.transaction():
        with conn.cursor() as cur:
            # TRUNCATE in the same transaction as COPY: readers never see a
            # partially loaded table and a failure leaves the old data intact.
            cur.execute("TRUNCATE transactions, dataset_metadata")
            rows = gen.iter_transactions(cfg)
            with cur.copy(f"COPY transactions ({', '.join(gen.COLUMNS)}) FROM STDIN") as copy:
                exhausted = False
                while not exhausted:
                    t0 = time.perf_counter()
                    buf = []
                    for t in rows:
                        fp.update(t)
                        buf.append(gen.to_copy_line(t))
                        if len(buf) >= batch_size:
                            break
                    else:
                        exhausted = True
                    t1 = time.perf_counter()
                    if buf:
                        copy.write("".join(buf))
                    t2 = time.perf_counter()
                    generated += len(buf)
                    gen_seconds += t1 - t0
                    copy_seconds += t2 - t1
            t3 = time.perf_counter()
            inserted = cur.rowcount
            copy_seconds += time.perf_counter() - t3

            cur.execute("SELECT count(*) FROM transactions")
            table_count = cur.fetchone()[0]
            if not (generated == inserted == table_count == cfg.count):
                raise RuntimeError(
                    f"count mismatch: expected={cfg.count} generated={generated} "
                    f"copied={inserted} in_table={table_count}")

            cur.execute(
                """INSERT INTO dataset_metadata (id, generator_version, seed, record_count,
                       start_date, end_date, num_merchants, fingerprint_md5,
                       generation_seconds, load_seconds)
                   VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (gen.GENERATOR_VERSION, cfg.seed, cfg.count, cfg.start_date, cfg.end_date,
                 cfg.num_merchants, fp.hexdigest(), round(gen_seconds, 3),
                 round(copy_seconds, 3)))
    commit_done = time.perf_counter()

    # Fresh statistics and visibility map, so query plans start from a known state.
    # VACUUM can't run inside a transaction block.
    with conn.cursor() as cur:
        cur.execute("VACUUM (ANALYZE) transactions")
        cur.execute("""SELECT pg_relation_size('transactions'),
                              pg_indexes_size('transactions'),
                              pg_total_relation_size('transactions'),
                              pg_size_pretty(pg_total_relation_size('transactions'))""")
        heap, idx, total, pretty = cur.fetchone()
    vacuum_seconds = time.perf_counter() - commit_done

    return {
        "config": cfg.to_dict(),
        "generator_version": gen.GENERATOR_VERSION,
        "generated_records": generated,
        "inserted_records": inserted,
        "fingerprint_md5": fp.hexdigest(),
        "generation_seconds": round(gen_seconds, 3),
        "copy_seconds": round(copy_seconds, 3),
        "load_transaction_seconds": round(commit_done - started, 3),
        "vacuum_analyze_seconds": round(vacuum_seconds, 3),
        "heap_bytes": heap,
        "index_bytes": idx,
        "total_bytes": total,
        "total_size_pretty": pretty,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate and bulk-load transactions.")
    gen.add_generator_args(parser)
    parser.add_argument("--force", action="store_true",
                        help="reload even if the same dataset is already loaded")
    parser.add_argument("--no-report", action="store_true",
                        help=f"do not write {REPORT_FILE.relative_to(ROOT)}")
    args = parser.parse_args(argv)
    cfg = gen.config_from_args(parser, args)

    conn = connect()
    try:
        apply_schema(conn)
        if not args.force and already_loaded(conn, cfg):
            print(f"dataset already loaded for {cfg.to_dict()}; skipping (use --force to reload)")
            return 0
        print(f"loading {cfg.count:,} records with config {cfg.to_dict()} ...", flush=True)
        report = load(conn, cfg, args.batch_size)
    except (psycopg.Error, RuntimeError) as exc:
        print(f"ERROR: load failed and was rolled back: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    print(f"generated: {report['generated_records']:,}  inserted: {report['inserted_records']:,}")
    print(f"generation: {report['generation_seconds']}s  COPY: {report['copy_seconds']}s  "
          f"load txn total: {report['load_transaction_seconds']}s  "
          f"VACUUM ANALYZE: {report['vacuum_analyze_seconds']}s")
    print(f"table size: {report['total_size_pretty']}  fingerprint: {report['fingerprint_md5']}")
    if not args.no_report:
        REPORT_FILE.parent.mkdir(exist_ok=True)
        REPORT_FILE.write_text(json.dumps(report, indent=2) + "\n")
        print(f"report written to {REPORT_FILE.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
