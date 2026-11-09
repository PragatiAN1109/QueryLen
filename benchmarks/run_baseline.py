#!/usr/bin/env python3
"""Run each baseline query once and save its results and EXPLAIN ANALYZE plan.

Each run goes into a new timestamped folder (earlier runs are never overwritten):
  benchmarks/results/baseline/<run_id>/   manifest.json + result rows per query
  benchmarks/plans/baseline/<run_id>/     raw EXPLAIN JSON per query

One execution per query, so the timings are only indicative.
Exit codes: 0 ok, 1 query failure, 3 database unreachable.
"""
from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import platform
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import psycopg  # noqa: E402

from db import connect  # noqa: E402  (also puts data/ on sys.path)

import generate_transactions as gen  # noqa: E402

SQL_DIR = ROOT / "sql" / "baseline"
RESULTS_DIR = ROOT / "benchmarks" / "results" / "baseline"
PLANS_DIR = ROOT / "benchmarks" / "plans" / "baseline"

QUERIES = [
    "q1_category_revenue",
    "q2_merchant_revenue",
    "q3_failure_rate",
    "q4_merchant_ranking",
    "q5_narrow",
    "q5_broad",
]
SETTINGS = ["server_version", "shared_buffers", "work_mem", "effective_cache_size",
            "max_parallel_workers_per_gather", "random_page_cost", "jit", "jit_above_cost",
            "TimeZone"]
SCAN_NODES = {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"}
INDEX_NODES = {"Bitmap Index Scan"}  # reads an index only; its rows are not table rows
PARAM_RE = re.compile(r"^--\s*@param\s+(\w+)\s*=\s*(.+?)\s*(?:\(.*\))?$")


def load_query(name: str) -> tuple[str, dict]:
    return load_sql_file(SQL_DIR / f"{name}.sql")


def load_sql_file(path: Path) -> tuple[str, dict]:
    """Return (SQL without trailing semicolon, {param: value} from @param headers)."""
    text = path.read_text()
    params = {}
    for line in text.splitlines():
        m = PARAM_RE.match(line.strip())
        if m:
            params[m.group(1)] = m.group(2)
    return text.strip().rstrip(";"), params


def jsonable(v):
    if isinstance(v, decimal.Decimal):
        return str(v)  # keep exact monetary precision
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    return v


def parallel_divisor(workers: int) -> float:
    """PostgreSQL's planner divisor for per-process row estimates under Gather:
    workers plus the leader's partial share (1 - 0.3 * workers, if positive).
    Plan Rows of a parallel node = estimated total rows / divisor."""
    return workers + max(0.0, 1.0 - 0.3 * workers)


def walk(node: dict, depth: int = 0, gather_workers: int = 0):
    yield node, depth, gather_workers
    if node["Node Type"] in ("Gather", "Gather Merge"):
        gather_workers = node.get("Workers Planned", 0)
    for child in node.get("Plans", []):
        yield from walk(child, depth + 1, gather_workers)


def node_summary(node: dict, gather_workers: int = 0) -> dict:
    loops = node.get("Actual Loops", 1) or 1
    divisor = parallel_divisor(gather_workers) if gather_workers else 1.0
    children_ms = sum(c.get("Actual Total Time", 0) for c in node.get("Plans", []))
    s = {
        "node_type": node["Node Type"],
        "strategy": node.get("Strategy"),
        "relation": node.get("Relation Name"),
        "parallel_aware": node.get("Parallel Aware"),
        "plan_rows_per_loop": node.get("Plan Rows"),
        "actual_rows_per_loop": node.get("Actual Rows"),
        "actual_loops": loops,
        # Estimated vs actual on the same (whole-query) basis. Actual per-loop
        # values are rounded averages in PG17, so totals can be off by +-loops.
        "plan_rows_total_est": round(node.get("Plan Rows", 0) * divisor),
        "actual_rows_total": round(node.get("Actual Rows", 0) * loops),
        "rows_removed_by_filter_total": round(node.get("Rows Removed by Filter", 0) * loops),
        "total_cost": node.get("Total Cost"),
        "actual_total_ms_per_loop": node.get("Actual Total Time"),
        # Approximate time spent in this node itself (per-loop average): its total
        # time minus its children's. Parallel children run concurrently, so treat
        # as indicative only.
        "approx_self_ms": round(max(node.get("Actual Total Time", 0) - children_ms, 0), 3),
        "shared_hit_blocks": node.get("Shared Hit Blocks"),
        "shared_read_blocks": node.get("Shared Read Blocks"),
        "temp_read_blocks": node.get("Temp Read Blocks"),
        "temp_written_blocks": node.get("Temp Written Blocks"),
    }
    for key in ("Filter", "Index Name", "Index Cond", "Workers Planned", "Workers Launched",
                "Sort Key", "Sort Method", "Sort Space Used", "Sort Space Type", "Group Key",
                "Peak Memory Usage", "Disk Usage", "HashAgg Batches", "Heap Fetches",
                "Exact Heap Blocks", "Lossy Heap Blocks", "Rows Removed by Index Recheck",
                "Recheck Cond"):
        if key in node:
            s[key.lower().replace(" ", "_")] = node[key]
    return s


def plan_metrics(explain: list) -> dict:
    top = explain[0]
    root = top["Plan"]
    nodes = [node_summary(n, gw) | {"depth": d} for n, d, gw in walk(root)]
    scans = [n for n in nodes if n["node_type"] in SCAN_NODES]
    dominant = max(nodes, key=lambda n: n["approx_self_ms"])
    return {
        "planning_ms": top["Planning Time"],
        "execution_ms": top["Execution Time"],
        "jit": top.get("JIT"),
        "rows_returned": root["Actual Rows"],
        # rows_examined_by_scans = rows output by table-scan nodes + rows those
        # nodes discarded with their Filter, summed over all parallel processes.
        # It is the number of table rows the scan(s) looked at, not a sum over
        # every operator. Approximate to +-loops (PG17 reports rounded averages).
        "rows_examined_by_scans": sum(n["actual_rows_total"] + n["rows_removed_by_filter_total"]
                                      + round(n.get("rows_removed_by_index_recheck", 0)
                                              * n["actual_loops"])
                                      for n in scans),
        "rows_matching_scan_filter": sum(n["actual_rows_total"] for n in scans),
        "rows_matching_scan_filter_est": sum(n["plan_rows_total_est"] for n in scans),
        "scan_nodes": [{k: n.get(k) for k in ("node_type", "relation", "index_name",
                                              "parallel_aware", "filter")}
                       for n in nodes if n["node_type"] in SCAN_NODES | INDEX_NODES],
        "node_types": [("  " * n["depth"]) + n["node_type"] +
                       (f" ({n['strategy']})" if n.get("strategy") else "") for n in nodes],
        "dominant_node": f"{dominant['node_type']} (~{dominant['approx_self_ms']} ms self)",
        "shared_hit_blocks": root.get("Shared Hit Blocks", 0),
        "shared_read_blocks": root.get("Shared Read Blocks", 0),
        "temp_blocks": (root.get("Temp Read Blocks", 0) or 0) + (root.get("Temp Written Blocks", 0) or 0),
        "planning_buffers": top.get("Planning", {}),
        "workers_launched": max((n.get("workers_launched", 0) for n in nodes), default=0),
        "nodes": nodes,
    }


def main() -> int:
    run_started = dt.datetime.now(dt.timezone.utc)
    run_id = run_started.strftime("%Y%m%dT%H%M%SZ")
    res_dir, plan_dir = RESULTS_DIR / run_id, PLANS_DIR / run_id

    conn = connect()
    failures: list[str] = []
    with conn.cursor() as cur:
        settings = {}
        for s in SETTINGS:
            cur.execute(f"SHOW {s}")  # fixed list, not user input
            settings[s] = cur.fetchone()[0]
        cur.execute("SELECT version()")
        full_version = cur.fetchone()[0]
        cur.execute("""SELECT seed, record_count, start_date, end_date, fingerprint_md5,
                              generator_version FROM dataset_metadata""")
        seed, count, start, end, recorded_fp, gver = cur.fetchone()
        cur.execute(gen.FINGERPRINT_SQL)
        live_fp = cur.fetchone()[0]
        if live_fp != recorded_fp:
            failures.append("dataset fingerprint does not match dataset_metadata")
        cur.execute("""SELECT indexname FROM pg_indexes
                       WHERE schemaname = 'public' AND tablename = 'transactions' ORDER BY 1""")
        indexes = [r[0] for r in cur.fetchall()]
        cur.execute("""SELECT pg_relation_size('transactions') / 8192,
                              pg_size_pretty(pg_total_relation_size('transactions')),
                              last_analyze, last_autoanalyze
                       FROM pg_stat_user_tables WHERE relname = 'transactions'""")
        heap_pages, total_size, last_analyze, last_autoanalyze = cur.fetchone()

        res_dir.mkdir(parents=True, exist_ok=False)   # never overwrite an earlier run
        plan_dir.mkdir(parents=True, exist_ok=False)

        queries = {}
        for name in QUERIES:
            sql, params = load_query(name)
            entry = {"sql_file": f"sql/baseline/{name}.sql",
                     "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                     "params": params}
            try:
                t0 = time.perf_counter()
                cur.execute(sql)
                columns = [d.name for d in cur.description]
                rows = cur.fetchall()
                entry["client_wall_ms"] = round((time.perf_counter() - t0) * 1000, 3)
                entry["row_count"] = len(rows)
                (res_dir / f"{name}.json").write_text(json.dumps(
                    {"query": name, "columns": columns,
                     "rows": [[jsonable(v) for v in r] for r in rows]}, indent=2) + "\n")

                cur.execute("EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) " + sql)
                explain = cur.fetchone()[0]
                (plan_dir / f"{name}.json").write_text(json.dumps(explain, indent=2) + "\n")
                entry["plan_file"] = f"benchmarks/plans/baseline/{run_id}/{name}.json"
                entry["metrics"] = plan_metrics(explain)
                if not rows:
                    failures.append(f"{name}: returned no rows")
            except psycopg.Error as exc:
                failures.append(f"{name}: {exc}")
                entry["error"] = str(exc)
            queries[name] = entry
            m = entry.get("metrics", {})
            print(f"{name:22s} rows={entry.get('row_count', '-'):>3}  "
                  f"plan={m.get('planning_ms', '-')}ms exec={m.get('execution_ms', '-')}ms  "
                  f"scanned={m.get('rows_examined_by_scans', '-')}  workers={m.get('workers_launched', '-')}  "
                  f"dominant={m.get('dominant_node', '-')}")
    conn.close()

    manifest = {
        "run_id": run_id,
        "phase": "baseline",
        "executed_at_utc": run_started.isoformat(),
        "method": "1 plain execution (results, warms cache) then 1 EXPLAIN ANALYZE per query, "
                  "sequential, single connection. Not a repeated-run benchmark.",
        "postgres_version": full_version,
        "settings": settings,
        "dataset": {"seed": seed, "record_count": count, "start_date": str(start),
                    "end_date": str(end), "generator_version": gver,
                    "fingerprint_recorded": recorded_fp, "fingerprint_live": live_fp},
        "table": {"indexes": indexes, "heap_pages": heap_pages, "total_size": total_size,
                  "last_analyze": jsonable(last_analyze),
                  "last_autoanalyze": jsonable(last_autoanalyze)},
        "environment": {"platform": platform.platform(), "machine": platform.machine(),
                        "python": platform.python_version(), "psycopg": psycopg.__version__},
        "queries": queries,
        "failures": failures,
    }
    (res_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=jsonable) + "\n")
    print(f"\nresults: {res_dir.relative_to(ROOT)}\nplans:   {plan_dir.relative_to(ROOT)}")
    if failures:
        print("FAILURES:", *failures, sep="\n  ", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
