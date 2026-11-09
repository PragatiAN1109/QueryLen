#!/usr/bin/env python3
"""Validate the loaded dataset and report its distributions.

Integrity checks (any failure -> exit 1):
  * transactions table exists, row count matches dataset_metadata
  * transaction_id unique, no NULLs, amounts >= 0
  * timestamps inside [start_date, end_date) from metadata
  * every category / region / payment method / status appears
  * order-defined MD5 fingerprint of the table equals the fingerprint recorded at load
  * --verify-generator: regenerate the dataset in Python from the recorded
    config and confirm the fingerprint matches (reproducibility)

Writes a Markdown report to results/validation_report.md.
Exit codes: 0 ok, 1 check failed, 3 database unreachable.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from db import ROOT, connect

import generate_transactions as gen

REPORT_FILE = ROOT / "results" / "validation_report.md"


class Report:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.failures: list[str] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        mark = "PASS" if ok else "FAIL"
        print(f"[{mark}] {name}{' - ' + detail if detail else ''}")
        self.lines.append(f"| {mark} | {name} | {detail} |")
        if not ok:
            self.failures.append(name)

    def table(self, title: str, headers: list[str], rows: list[tuple]) -> None:
        print(f"\n== {title}")
        widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) if rows else len(h)
                  for i, h in enumerate(headers)]
        print("  ".join(str(h).ljust(w) for h, w in zip(headers, widths)))
        for r in rows:
            print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
        self.lines += ["", f"### {title}", "",
                       "| " + " | ".join(headers) + " |",
                       "|" + "---|" * len(headers)]
        self.lines += ["| " + " | ".join(str(v) for v in r) + " |" for r in rows]


def q(cur, sql: str, params=None) -> list[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def share_query(column: str) -> str:
    # column comes only from the fixed list below, never from user input
    return f"""SELECT {column}, count(*) AS n,
                      round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
               FROM transactions GROUP BY {column} ORDER BY n DESC"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the loaded transactions dataset.")
    parser.add_argument("--verify-generator", action="store_true",
                        help="regenerate data in Python and compare fingerprints")
    parser.add_argument("--no-report", action="store_true")
    args = parser.parse_args(argv)

    rep = Report()
    conn = connect()
    with conn.cursor() as cur:
        version = q(cur, "SHOW server_version")[0][0]
        print(f"PostgreSQL {version}")

        exists = q(cur, "SELECT to_regclass('public.transactions') IS NOT NULL")[0][0]
        rep.check("transactions table exists", exists)
        if not exists:
            return finish(rep, args, version)

        meta = q(cur, """SELECT seed, record_count, start_date, end_date, num_merchants,
                                fingerprint_md5, generator_version FROM dataset_metadata""")
        rep.check("dataset_metadata row present", len(meta) == 1)
        if len(meta) != 1:
            return finish(rep, args, version)
        seed, expected, start, end, merchants, recorded_fp, gver = meta[0]
        cfg = gen.GeneratorConfig(count=expected, seed=seed, start_date=start,
                                  end_date=end, num_merchants=merchants)

        n, distinct_ids = q(cur, "SELECT count(*), count(DISTINCT transaction_id) FROM transactions")[0]
        rep.check("row count matches metadata", n == expected, f"{n:,} rows (expected {expected:,})")
        rep.check("transaction_id unique", distinct_ids == n, f"{distinct_ids:,} distinct")

        null_counts = q(cur, "SELECT " + ", ".join(
            f"count(*) FILTER (WHERE {c} IS NULL)" for c in gen.COLUMNS) + " FROM transactions")[0]
        rep.check("no NULLs in any column", sum(null_counts) == 0,
                  ", ".join(f"{c}={v}" for c, v in zip(gen.COLUMNS, null_counts) if v) or "0 NULLs")

        neg = q(cur, "SELECT count(*) FROM transactions WHERE amount < 0")[0][0]
        rep.check("amounts nonnegative", neg == 0, f"{neg} negative")

        out_of_range = q(cur, """SELECT count(*) FROM transactions
                                 WHERE transaction_date < %s OR transaction_date >= %s""",
                         (cfg.start_ts, cfg.end_ts))[0][0]
        rep.check("timestamps within configured range", out_of_range == 0,
                  f"[{start}, {end}) UTC, {out_of_range} outside")

        for column, vocab in (("category", gen.CATEGORIES), ("region", gen.REGIONS),
                              ("payment_method", gen.PAYMENT_METHODS), ("status", gen.STATUSES)):
            seen = {r[0] for r in q(cur, f"SELECT DISTINCT {column} FROM transactions")}
            missing, extra = set(vocab) - seen, seen - set(vocab)
            rep.check(f"all {column} values present", not missing and not extra,
                      f"missing={sorted(missing)} unexpected={sorted(extra)}"
                      if (missing or extra) else f"{len(seen)} values")

        idx = q(cur, """SELECT indexname FROM pg_indexes
                        WHERE schemaname = 'public' AND tablename = 'transactions'""")
        rep.check("baseline has only the primary-key index",
                  [r[0] for r in idx] == ["transactions_pkey"], ", ".join(r[0] for r in idx))

        t0 = time.perf_counter()
        db_fp = q(cur, gen.FINGERPRINT_SQL)[0][0]
        rep.check("table fingerprint matches load-time fingerprint", db_fp == recorded_fp,
                  f"{db_fp} ({time.perf_counter() - t0:.1f}s)")

        if args.verify_generator:
            if gver != gen.GENERATOR_VERSION:
                rep.check("generator version matches", False, f"loaded {gver}, code {gen.GENERATOR_VERSION}")
            t0 = time.perf_counter()
            regen = gen.dataset_fingerprint(cfg)
            rep.check("regenerated dataset reproduces fingerprint", regen == db_fp,
                      f"{regen} ({time.perf_counter() - t0:.1f}s)")

        # Distributions
        for col in ("category", "region", "status", "payment_method"):
            rep.table(f"Transactions by {col}", [col, "count", "pct"], q(cur, share_query(col)))

        rep.table("Top 10 merchants by transaction volume",
                  ["merchant_id", "count", "pct", "cumulative_pct"],
                  q(cur, """SELECT merchant_id, n, pct, sum(pct) OVER (ORDER BY n DESC, merchant_id)
                            FROM (SELECT merchant_id, count(*) AS n,
                                         round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
                                  FROM transactions GROUP BY merchant_id) m
                            ORDER BY n DESC, merchant_id LIMIT 10"""))
        rep.table("Merchant concentration",
                  ["merchants", "min_txns", "median_txns", "max_txns", "top_10pct_merchants_share_pct"],
                  q(cur, """WITH m AS (SELECT merchant_id, count(*) n FROM transactions GROUP BY 1),
                                 r AS (SELECT n, ntile(10) OVER (ORDER BY n DESC) d FROM m)
                            SELECT (SELECT count(*) FROM m), (SELECT min(n) FROM m),
                                   (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY n) FROM m),
                                   (SELECT max(n) FROM m),
                                   round(100.0 * (SELECT sum(n) FROM r WHERE d = 1) / (SELECT sum(n) FROM m), 2)"""))
        rep.table("Date range (UTC)", ["min_transaction_date", "max_transaction_date"],
                  q(cur, "SELECT min(transaction_date), max(transaction_date) FROM transactions"))
        rep.table("Amount statistics", ["min", "max", "avg", "median", "p99"],
                  q(cur, """SELECT min(amount), max(amount), round(avg(amount), 2),
                                   percentile_disc(0.5) WITHIN GROUP (ORDER BY amount),
                                   percentile_disc(0.99) WITHIN GROUP (ORDER BY amount)
                            FROM transactions"""))
        rep.table("Amount by category", ["category", "median", "avg", "p99"],
                  q(cur, """SELECT category, percentile_disc(0.5) WITHIN GROUP (ORDER BY amount),
                                   round(avg(amount), 2),
                                   percentile_disc(0.99) WITHIN GROUP (ORDER BY amount)
                            FROM transactions GROUP BY 1 ORDER BY 2 DESC"""))
        rep.table("Monthly transaction volume (UTC)", ["month", "count", "revenue_success"],
                  q(cur, """SELECT to_char(date_trunc('month', transaction_date), 'YYYY-MM'), count(*),
                                   sum(amount) FILTER (WHERE status = 'SUCCESS')
                            FROM transactions GROUP BY 1 ORDER BY 1"""))
        rep.table("Correlation: share of each merchant's volume in its dominant region / category",
                  ["avg_dominant_region_pct", "avg_dominant_category_pct"],
                  q(cur, """WITH mr AS (SELECT merchant_id, max(c) * 100.0 / sum(c) s FROM
                                   (SELECT merchant_id, region, count(*) c FROM transactions GROUP BY 1, 2) x
                                   GROUP BY 1),
                                 mc AS (SELECT merchant_id, max(c) * 100.0 / sum(c) s FROM
                                   (SELECT merchant_id, category, count(*) c FROM transactions GROUP BY 1, 2) x
                                   GROUP BY 1)
                            SELECT round((SELECT avg(s) FROM mr), 1), round((SELECT avg(s) FROM mc), 1)"""))
        rep.table("Merchant/category combinations",
                  ["possible_pairs", "observed_pairs", "pairs_under_10_rows"],
                  q(cur, """WITH p AS (SELECT merchant_id, category, count(*) c FROM transactions GROUP BY 1, 2)
                            SELECT (SELECT count(DISTINCT merchant_id) FROM transactions) * 8,
                                   count(*), count(*) FILTER (WHERE c < 10) FROM p"""))
        rep.table("Hour-of-day profile (UTC, top 5 and bottom 3)", ["hour", "count"],
                  q(cur, """(SELECT extract(hour FROM transaction_date)::int h, count(*) n
                             FROM transactions GROUP BY 1 ORDER BY 2 DESC LIMIT 5)
                            UNION ALL
                            (SELECT extract(hour FROM transaction_date)::int, count(*)
                             FROM transactions GROUP BY 1 ORDER BY 2 ASC LIMIT 3)"""))
        rep.table("Storage", ["heap", "indexes (PK only)", "total", "total_bytes"],
                  q(cur, """SELECT pg_size_pretty(pg_relation_size('transactions')),
                                   pg_size_pretty(pg_indexes_size('transactions')),
                                   pg_size_pretty(pg_total_relation_size('transactions')),
                                   pg_total_relation_size('transactions')"""))
    conn.close()
    return finish(rep, args, version)


def finish(rep: Report, args, version: str) -> int:
    status = "FAILED" if rep.failures else "PASSED"
    print(f"\nValidation {status}" + (f": {rep.failures}" if rep.failures else ""))
    if not args.no_report:
        REPORT_FILE.parent.mkdir(exist_ok=True)
        header = [f"# Dataset validation report", "",
                  f"PostgreSQL {version}. Generated by `scripts/validate.py`. Result: **{status}**.",
                  "", "| Result | Check | Detail |", "|---|---|---|"]
        REPORT_FILE.write_text("\n".join(header + rep.lines) + "\n")
        print(f"report written to {REPORT_FILE.relative_to(ROOT)}")
    return 1 if rep.failures else 0


if __name__ == "__main__":
    sys.exit(main())
