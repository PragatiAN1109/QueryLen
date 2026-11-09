#!/usr/bin/env python3
"""Benchmark the three optimizations (BRIN, covering B-tree, summary tables).

For each optimization the batches run in the order A0 B0 A1 B1 A2
(A = no optimization, B = optimization created). Every optimized batch has a
baseline batch right before and after it, which makes timing drift visible.
Each batch does a few untimed warm-ups, then timed rounds in a shuffled order.

Latency is measured in the client with perf_counter_ns() around execute() and
fetchall(). psycopg's auto-prepare is off, so every run is parsed and planned
like an ad-hoc query. Server-side EXPLAIN ANALYZE times are collected separately
and never mixed with the client timings.

Every result is compared exactly with the baseline result; a mismatch fails the run.
The script refuses to start if experiment objects are already present, and it
always drops what it created, so the database ends in its original state.

Exit codes: 0 ok, 1 correctness failure/error, 2 unexpected schema or dataset, 3 DB down.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "benchmarks"))

import psycopg  # noqa: E402

import bench_stats as bs  # noqa: E402
import run_baseline as rb  # noqa: E402
from db import connect  # noqa: E402

import generate_transactions as gen  # noqa: E402

RESULTS_DIR = ROOT / "benchmarks" / "results" / "validated"
PLANS_DIR = ROOT / "benchmarks" / "plans" / "validated"


BASELINE_SQL = ROOT / "sql" / "baseline"
EXP_SQL = ROOT / "sql" / "experiments"
BASELINE_REFERENCE_RUN = "20261109T015439Z"   # saved baseline results in benchmarks/results/baseline/
EXPECTED_FINGERPRINT = "16aae85345e5d7881554301ac5753e4f"
BASELINE_INDEXES = {"transactions_pkey", "dataset_metadata_pkey"}
BASELINE_TABLES = {"transactions", "dataset_metadata"}

# The optimization configurations. Each is created before an optimized batch and dropped after.
CONFIGS: dict[str, dict] = {
    "brin_ppr128": {
        "ddl": ["CREATE INDEX exp_brin_txn_date_ppr128 ON transactions "
                "USING brin (transaction_date) WITH (pages_per_range = 128)"],
        "objects": ["exp_brin_txn_date_ppr128"]},
    "btree_merchant_status_cov": {
        "ddl": ["CREATE INDEX exp_btree_merchant_status_cov ON transactions "
                "(merchant_id, status) INCLUDE (region, amount)"],
        "objects": ["exp_btree_merchant_status_cov"]},
    "preagg_daily": {
        "ddl_file": EXP_SQL / "preagg_daily_build.sql",
        "objects": ["exp_merchant_daily_success", "exp_region_daily_status"]},
    "preagg_monthly": {
        "ddl_file": EXP_SQL / "preagg_monthly_build.sql",
        "objects": ["exp_merchant_monthly_success", "exp_region_monthly_status"]},
}


def schema_objects(cur) -> dict:
    cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY 1")
    indexes = [r[0] for r in cur.fetchall()]
    cur.execute("""SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace
                   AND relkind IN ('r', 'm', 'p', 'v') ORDER BY 1""")
    return {"indexes": indexes, "relations": [r[0] for r in cur.fetchall()]}


def unexpected_objects(cur) -> list[str]:
    objs = schema_objects(cur)
    return ([f"index {i}" for i in objs["indexes"] if i not in BASELINE_INDEXES] +
            [f"relation {r}" for r in objs["relations"] if r not in BASELINE_TABLES])


def cleanup(cur) -> None:
    cur.execute((EXP_SQL / "cleanup.sql").read_text())


def dataset_identity(cur) -> dict:
    cur.execute("SELECT count(*) FROM transactions")
    count = cur.fetchone()[0]
    cur.execute(gen.FINGERPRINT_SQL)
    return {"row_count": count, "fingerprint": cur.fetchone()[0]}


def object_sizes(cur, names: list[str]) -> dict:
    out = {}
    for name in names:
        cur.execute("SELECT pg_total_relation_size(%s::regclass), "
                    "pg_size_pretty(pg_total_relation_size(%s::regclass))", (name, name))
        size, pretty = cur.fetchone()
        entry = {"bytes": size, "pretty": pretty}
        cur.execute("SELECT relkind FROM pg_class WHERE relname = %s", (name,))
        if cur.fetchone()[0] == "r":
            cur.execute(f'SELECT count(*) FROM "{name}"')  # name from the fixed CONFIGS
            entry["rows"] = cur.fetchone()[0]
        out[name] = entry
    return out


def plan_uses(metrics: dict, objects: list[str]) -> list[str]:
    return sorted({n[k] for n in metrics["nodes"] for k in ("index_name", "relation")
                   if n.get(k) in objects})


def results_equal(a: dict, b: dict) -> dict:
    """Exact comparison of two result sets (values as strings, NUMERIC kept exact, same order)."""
    mismatches = [i for i, (x, y) in enumerate(zip(a["rows"], b["rows"]))
                  if [str(v) for v in x] != [str(v) for v in y]]
    equal = len(a["rows"]) == len(b["rows"]) and not mismatches
    return {"equal": equal, "mismatched_row_indexes": mismatches}


def reference_result(workload_file: Path) -> dict | None:
    """Saved baseline result from the original baseline run, for unchanged baseline SQL."""
    if workload_file.parent != BASELINE_SQL:
        return None
    path = ROOT / "benchmarks/results/baseline" / BASELINE_REFERENCE_RUN / f"{workload_file.stem}.json"
    return json.loads(path.read_text()) if path.exists() else None

# Fixed benchmark plan: (experiment, config) -> workloads (baseline SQL, optimized SQL).
B, E = BASELINE_SQL, EXP_SQL
GROUPS: list[dict] = [
    {"experiment": "brin", "config": "brin_ppr128", "workloads": {
        "q1_category_revenue": (B / "q1_category_revenue.sql", B / "q1_category_revenue.sql"),
        "q5_narrow": (B / "q5_narrow.sql", B / "q5_narrow.sql"),
        "q5_broad": (B / "q5_broad.sql", B / "q5_broad.sql")}},
    {"experiment": "btree", "config": "btree_merchant_status_cov", "workloads": {
        "q2_high_volume_1345": (B / "q2_merchant_revenue.sql", B / "q2_merchant_revenue.sql"),
        "q2_low_volume_1427": (E / "q2_merchant_revenue_low_volume.sql",
                               E / "q2_merchant_revenue_low_volume.sql")}},
    {"experiment": "preaggregation", "config": "preagg_daily", "workloads": {
        "q3_failure_rate": (B / "q3_failure_rate.sql", E / "q3_failure_rate_preagg_daily.sql"),
        "q4_merchant_ranking": (B / "q4_merchant_ranking.sql", E / "q4_merchant_ranking_preagg_daily.sql")}},
    {"experiment": "preaggregation", "config": "preagg_monthly", "workloads": {
        "q3_failure_rate": (B / "q3_failure_rate.sql", E / "q3_failure_rate_preagg_monthly.sql"),
        "q4_merchant_ranking": (B / "q4_merchant_ranking.sql", E / "q4_merchant_ranking_preagg_monthly.sql")}},
]


def config_def(experiment: str, config: str) -> dict:
    return CONFIGS[config]


def apply_config(cur, cfg: dict) -> float:
    t0 = time.perf_counter()
    if "ddl_file" in cfg:
        cur.execute(cfg["ddl_file"].read_text())
    else:
        for stmt in cfg["ddl"]:
            cur.execute(stmt)
    return time.perf_counter() - t0


def drop_config(cur, cfg: dict) -> None:
    kind = "TABLE" if "ddl_file" in cfg else "INDEX"
    for obj in cfg["objects"]:  # fixed names from the config, never user input
        cur.execute(f'DROP {kind} IF EXISTS "{obj}"')



def _cmd(args: list[str]) -> str | None:
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=10).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def environment(cur) -> dict:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    settings = {}
    for s in rb.SETTINGS + ["max_worker_processes", "max_parallel_workers", "huge_pages"]:
        cur.execute(f"SHOW {s}")  # fixed list
        settings[s] = cur.fetchone()[0]
    cur.execute("""SELECT c.relpages, c.relallvisible, c.reltuples, s.n_dead_tup, s.last_analyze,
                          s.last_vacuum, s.last_autovacuum
                   FROM pg_class c JOIN pg_stat_user_tables s ON s.relid = c.oid
                   WHERE c.relname = 'transactions'""")
    relpages, allvis, reltuples, dead, last_analyze, last_vac, last_autovac = cur.fetchone()
    cur.execute("""SELECT blks_hit, blks_read FROM pg_stat_database WHERE datname = current_database()""")
    hit, read = cur.fetchone()
    container = _cmd(["docker", "compose", "ps", "-q", "postgres"])
    limits = _cmd(["docker", "inspect", container, "--format",
                   "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}"]) if container else None
    docker_vm = _cmd(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}} {{.OperatingSystem}}"])
    return {
        "captured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": {"platform": platform.platform(), "machine": platform.machine(),
                 "cpu": _cmd(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor(),
                 "logical_cpus": os.cpu_count(),
                 "memory_bytes": int(_cmd(["sysctl", "-n", "hw.memsize"]) or 0) or None,
                 "load_average": os.getloadavg() if hasattr(os, "getloadavg") else None},
        "python": platform.python_version(), "psycopg": psycopg.__version__,
        "docker": {"vm_ncpu_mem_os": docker_vm,
                   "container_limits_nanocpus_memory": limits,
                   "note": "0 0 means no container CPU/memory limit (bounded by the Docker VM)"},
        "postgres": {"version": version, "settings": settings},
        "transactions_table": {"relpages": relpages, "relallvisible": allvis, "reltuples": reltuples,
                               "n_dead_tup": dead, "last_analyze": rb.jsonable(last_analyze),
                               "last_vacuum": rb.jsonable(last_vac),
                               "last_autovacuum": rb.jsonable(last_autovac)},
        "database_buffer_counters_at_start": {"blks_hit": hit, "blks_read": read},
    }



def run_batch(cur, *, group: dict, batch: str, kind: str, sql: dict[str, str], reference: dict,
              args, rng: random.Random, writer, server_rows: list, plan_dir: Path | None,
              mismatches: list) -> dict:
    """Run one batch; return {workload: [latency_ms...]}. kind is 'baseline' or 'optimized'."""
    workloads = list(sql)
    for w in workloads:
        for _ in range(args.warmups):
            cur.execute(sql[w])
            cur.fetchall()
    lat: dict[str, list[float]] = {w: [] for w in workloads}
    for i in range(args.samples):
        order = workloads[:]
        rng.shuffle(order)
        for pos, w in enumerate(order):
            t0 = time.perf_counter_ns()
            cur.execute(sql[w])
            rows = cur.fetchall()
            ms = (time.perf_counter_ns() - t0) / 1e6
            ok = rows == reference[w]
            if not ok:
                mismatches.append({"group": group["config"], "batch": batch, "workload": w,
                                   "sample": i, "got": [[rb.jsonable(v) for v in r] for r in rows]})
            lat[w].append(ms)
            writer.writerow([group["experiment"], group["config"], w, batch, kind, i, pos,
                             f"{ms:.6f}", ok])
    for w in workloads:  # server-side diagnostic, separate from client timings
        for j in range(args.server_samples):
            cur.execute("EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) " + sql[w])
            top = cur.fetchone()[0][0]
            server_rows.append([group["experiment"], group["config"], w, batch, kind, j,
                                top["Planning Time"], top["Execution Time"],
                                top["Plan"].get("Shared Hit Blocks", 0),
                                top["Plan"].get("Shared Read Blocks", 0)])
        if plan_dir is not None:
            cur.execute("EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) " + sql[w])
            explain = cur.fetchone()[0]
            plan_dir.mkdir(parents=True, exist_ok=True)
            (plan_dir / f"{w}__{kind}.json").write_text(json.dumps(explain, indent=2) + "\n")
    return lat


def rel(path: Path) -> str:
    """Path relative to the project if inside it, else absolute (e.g. --output-dir /tmp/x)."""
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path.resolve())


def plan_summary(path: Path, objects: list[str]) -> dict:
    explain = json.loads(path.read_text())
    m = rb.plan_metrics(explain)
    used = plan_uses(m, objects)
    keep = ("node_type", "relation", "index_name", "plan_rows_total_est", "actual_rows_total",
            "rows_removed_by_filter_total", "heap_fetches", "exact_heap_blocks", "lossy_heap_blocks",
            "rows_removed_by_index_recheck", "shared_hit_blocks", "shared_read_blocks",
            "temp_read_blocks", "temp_written_blocks", "sort_method", "workers_launched")
    return {"plan_file": rel(path), "node_types": m["node_types"],
            "planning_ms": m["planning_ms"], "execution_ms": m["execution_ms"],
            "rows_examined_by_scans": m["rows_examined_by_scans"],
            "rows_returned": m["rows_returned"],
            # Root-node buffer counters are cumulative for the whole plan (they include
            # all child nodes and parallel workers); per-node counters below are also
            # inclusive of that node's children, so they must not be summed.
            "root_shared_hit_blocks": m["shared_hit_blocks"],
            "root_shared_read_blocks": m["shared_read_blocks"],
            "temp_blocks": m["temp_blocks"], "workers_launched": m["workers_launched"],
            "uses_optimization_objects": used,
            "scan_nodes": [{k: n.get(k) for k in keep if n.get(k) is not None}
                           for n in m["nodes"] if n["node_type"] in rb.SCAN_NODES | rb.INDEX_NODES],
            "aggregate_and_sort_nodes": [{k: n.get(k) for k in ("node_type", "strategy", "sort_method",
                                                                 "hashagg_batches", "actual_rows_total")
                                          if n.get(k) is not None}
                                         for n in m["nodes"] if n["node_type"] in ("Aggregate", "Sort")]}


def run_group(cur, group: dict, args, rng, writer, server_rows, plan_root: Path,
              mismatches: list) -> dict:
    cfg = config_def(group["experiment"], group["config"])
    base_sql = {w: rb.load_sql_file(p[0])[0] for w, p in group["workloads"].items()}
    opt_sql = {w: rb.load_sql_file(p[1])[0] for w, p in group["workloads"].items()}
    reference = {}
    for w, sql in base_sql.items():
        cur.execute(sql)
        reference[w] = cur.fetchall()
    print(f"\n== {group['experiment']} / {group['config']}  workloads={list(base_sql)}")

    batches: dict[str, dict] = {}
    build_s: list[float] = []
    sizes = None
    schedule = []
    for c in range(args.cycles):
        schedule += [(f"A{c}", "baseline"), (f"B{c}", "optimized")]
    schedule.append((f"A{args.cycles}", "baseline"))

    try:
        for batch, kind in schedule:
            first_of_kind = not any(k == kind for k, _ in [(v["kind"], 0) for v in batches.values()])
            if kind == "optimized":
                build_s.append(apply_config(cur, cfg))
                if sizes is None:
                    sizes = object_sizes(cur, cfg["objects"])
            pdir = plan_root / group["experiment"] / group["config"] if first_of_kind else None
            lat = run_batch(cur, group=group, batch=batch, kind=kind,
                            sql=opt_sql if kind == "optimized" else base_sql, reference=reference,
                            args=args, rng=rng, writer=writer, server_rows=server_rows,
                            plan_dir=pdir, mismatches=mismatches)
            batches[batch] = {"kind": kind, "latencies": lat}
            if kind == "optimized":
                drop_config(cur, cfg)
            print(f"  {batch} {kind:9s} " + "  ".join(
                f"{w}={statistics.median(v):.3f}ms" for w, v in lat.items()))
    finally:
        drop_config(cur, cfg)

    out = {"experiment": group["experiment"], "config": group["config"],
           "ddl": cfg.get("ddl") or [str(cfg["ddl_file"].relative_to(ROOT))],
           "build_seconds": build_s, "objects": sizes, "schedule": [b for b, _ in schedule],
           "workloads": {}}
    for w in base_sql:
        a = {b: v["latencies"][w] for b, v in batches.items() if v["kind"] == "baseline"}
        o = {b: v["latencies"][w] for b, v in batches.items() if v["kind"] == "optimized"}
        a_all = [x for v in a.values() for x in v]
        o_all = [x for v in o.values() for x in v]
        # Temporally matched ratio per cycle: mean of bracketing control medians / B median.
        bracketed = []
        for c in range(args.cycles):
            ctrl = (bs.percentile(a[f"A{c}"], 50) + bs.percentile(a[f"A{c + 1}"], 50)) / 2
            opt = bs.percentile(o[f"B{c}"], 50)
            bracketed.append({"cycle": c, "control_median": ctrl, "optimized_median": opt,
                              "ratio": ctrl / opt if opt else None})
        pdir = plan_root / group["experiment"] / group["config"]
        out["workloads"][w] = {
            "baseline_sql": str(group["workloads"][w][0].relative_to(ROOT)),
            "optimized_sql": str(group["workloads"][w][1].relative_to(ROOT)),
            "baseline": bs.summarize(a_all), "optimized": bs.summarize(o_all),
            "comparison": bs.compare(a_all, o_all),
            "bracketed_cycles": bracketed,
            "baseline_drift": bs.drift([bs.percentile(v, 50) for v in a.values()]),
            "optimized_batch_drift": bs.drift([bs.percentile(v, 50) for v in o.values()]),
            "batch_medians": {b: bs.percentile(v["latencies"][w], 50) for b, v in batches.items()},
            "correct": not any(m["group"] == group["config"] and m["workload"] == w for m in mismatches),
            "baseline_vs_reference_run": (results_equal(
                {"columns": None, "rows": [[rb.jsonable(v) for v in r] for r in reference[w]]},
                {"columns": None, "rows": reference_result(group["workloads"][w][0])["rows"]})["equal"]
                if reference_result(group["workloads"][w][0]) else None),
            "plans": {k: plan_summary(pdir / f"{w}__{k}.json", cfg["objects"])
                      for k in ("baseline", "optimized")},
        }
    return out



COL_BASE, COL_OPT = "#2a78d6", "#eb6834"   # categorical slots 1 and 2 (validated default palette)
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _label(r: dict) -> str:
    names = {"q1_category_revenue": "Q1 category revenue", "q5_narrow": "Q5 narrow (7 d)",
             "q5_broad": "Q5 broad (12 mo)", "q2_high_volume_1345": "Q2 merchant 1345 (high)",
             "q2_low_volume_1427": "Q2 merchant 1427 (low)", "q3_failure_rate": "Q3 failure rate",
             "q4_merchant_ranking": "Q4 merchant ranking"}
    tech = {"brin_ppr128": "BRIN", "btree_merchant_status_cov": "covering B-tree",
            "preagg_daily": "daily summary", "preagg_monthly": "monthly summary"}
    return f"{names.get(r['workload'], r['workload'])}\n{tech.get(r['config'], r['config'])}"


def _style(ax, title: str, xlabel: str):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="bold")
    ax.set_xlabel(xlabel, color=INK2)
    ax.tick_params(colors=INK2, labelsize=9)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.grid(axis="x", color=GRID, linewidth=0.8, which="major")
    ax.set_axisbelow(True)


def make_charts(rows: list[dict], samples: dict, out_dir: Path, run_id: str, args) -> list[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_dir.mkdir(parents=True, exist_ok=True)
    src = (f"Source: QueryLens benchmark run {run_id}; local PostgreSQL 17.6 in Docker on Apple M3; "
           f"client-observed latency, {args.warmups} warm-ups + {args.samples} samples per batch.")
    labels = [_label(r) for r in rows][::-1]
    rr = rows[::-1]
    files = []

    # Median latency per comparison; log scale because values span 0.4-60 ms.
    fig, ax = plt.subplots(figsize=(10, 7.5), facecolor=SURFACE)
    y = list(range(len(rr)))
    h = 0.36
    ax.barh([i + h / 2 + 0.02 for i in y], [r["baseline_p50"] for r in rr], height=h,
            color=COL_BASE, label="Baseline (no optimization)")
    ax.barh([i - h / 2 - 0.02 for i in y], [r["optimized_p50"] for r in rr], height=h,
            color=COL_OPT, label="Optimized")
    for i, r in enumerate(rr):
        ax.text(r["baseline_p50"] * 1.08, i + h / 2 + 0.02, f"{r['baseline_p50']:.2f} ms",
                va="center", fontsize=8, color=INK2)
        ax.text(r["optimized_p50"] * 1.08, i - h / 2 - 0.02, f"{r['optimized_p50']:.3g} ms",
                va="center", fontsize=8, color=INK2)
    ax.set_xscale("log")
    ax.set_yticks(y, labels)
    ax.set_xlim(right=max(r["baseline_p50"] for r in rr) * 3)
    _style(ax, "Median query latency, baseline vs optimized (log scale)", "Median client-observed latency (ms)")
    ax.legend(loc="lower right", bbox_to_anchor=(1.0, 1.04), ncol=2, frameon=False, fontsize=9)
    fig.text(0.01, 0.01, src, fontsize=7, color=INK2)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    p = out_dir / "latency_p50.png"
    fig.savefig(p, dpi=150)
    plt.close(fig)
    files.append(p.name)

    return files



def parse_args(argv):
    p = argparse.ArgumentParser(description="Benchmark the BRIN, covering B-tree and summary-table optimizations.")
    p.add_argument("--experiment", choices=["all", "brin", "btree", "preaggregation"], default="all")
    p.add_argument("--samples", type=int, default=30, help="timed rounds per batch (default 30)")
    p.add_argument("--warmups", type=int, default=5, help="untimed runs per workload per batch (default 5)")
    p.add_argument("--cycles", type=int, default=2, help="A/B/A cycles per configuration (default 2)")
    p.add_argument("--server-samples", type=int, default=5,
                   help="EXPLAIN ANALYZE (TIMING OFF) runs per workload per batch (default 5)")
    p.add_argument("--seed", type=int, default=20261109, help="seed for workload order shuffling")
    p.add_argument("--smoke", action="store_true", help="quick check: 3 samples, 1 warm-up, 1 cycle, 1 server sample")
    p.add_argument("--charts-only", metavar="RUN_ID", default=None,
                   help="rebuild charts of an existing run from its saved data (no measurement)")
    p.add_argument("--output-dir", type=Path, default=None,
                   help="base directory for results/plans (default benchmarks/{results,plans}/validated)")
    a = p.parse_args(argv)
    if a.smoke:
        a.samples, a.warmups, a.cycles, a.server_samples = 3, 1, 1, 1
    for name in ("samples", "warmups", "cycles", "server_samples"):
        lo = 0 if name in ("warmups", "server_samples") else 1
        if not lo <= getattr(a, name) <= 10_000:
            p.error(f"--{name.replace('_', '-')} out of range")
    return a


def rebuild_charts(run_id: str, res_base: Path) -> int:
    """Regenerate charts from a run's summary.json + samples.csv (same measured data)."""
    if not run_id.replace("-", "").replace("~", "").replace("T", "").replace("Z", "").replace("smoke", "").isdigit():
        print("ERROR: invalid run id", file=sys.stderr)
        return 2
    res_dir = res_base / run_id
    summary = json.loads((res_dir / "summary.json").read_text())
    rows = summary["comparisons"]
    groups = {g["config"]: g for g in summary["groups"]}
    for r in rows:  # derive storage of the objects the optimized plan used (older runs lack it)
        g = groups[r["config"]]
        used = g["workloads"][r["workload"]]["plans"]["optimized"]["uses_optimization_objects"]
        r.setdefault("used_storage_bytes", sum(g["objects"][o]["bytes"] for o in used)
                     if used else r["storage_bytes"])
    samples: dict = {}
    with open(res_dir / "samples.csv") as fh:
        for s in csv.DictReader(fh):
            samples.setdefault((s["config"], s["workload"]), {"baseline": [], "optimized": []})[
                s["kind"]].append(float(s["latency_ms"]))
    p = summary["parameters"]
    args = argparse.Namespace(samples=p["samples"], warmups=p["warmups"])
    files = make_charts(rows, samples, res_dir / "charts", run_id, args)
    print("rebuilt:", *[rel(res_dir / "charts" / f) for f in files], sep="\n  ")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.charts_only:
        return rebuild_charts(args.charts_only, args.output_dir / "results" if args.output_dir else RESULTS_DIR)
    groups = [g for g in GROUPS if args.experiment in ("all", g["experiment"])]
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ("-smoke" if args.smoke else "")
    res_base = args.output_dir / "results" if args.output_dir else RESULTS_DIR
    plan_base = args.output_dir / "plans" if args.output_dir else PLANS_DIR
    n = 1
    while (res_base / run_id).exists() or (plan_base / run_id).exists():  # never reuse a run dir
        n += 1
        run_id = f"{run_id.split('~')[0]}~{n}"
    res_dir, plan_dir = res_base / run_id, plan_base / run_id

    with connect() as conn:
        conn.prepare_threshold = None  # never auto-prepare: every run is parsed+planned
        with conn.cursor() as cur:
            bad = unexpected_objects(cur)
            if bad:
                print(f"ERROR: non-baseline objects present: {bad}; run "
                      "sql/experiments/cleanup.sql if they are exp_ leftovers.",
                      file=sys.stderr)
                return 2
            identity = dataset_identity(cur)
            if identity != {"row_count": 1_000_000, "fingerprint": EXPECTED_FINGERPRINT}:
                print(f"ERROR: unexpected dataset {identity}", file=sys.stderr)
                return 2
            res_dir.mkdir(parents=True, exist_ok=False)  # unique; never overwrite
            plan_dir.mkdir(parents=True, exist_ok=False)
            env = environment(cur)
            cur.execute("""SELECT pg_relation_size('transactions'), pg_indexes_size('transactions'),
                                  pg_total_relation_size('transactions')""")
            heap, idx, total = cur.fetchone()
            env["transactions_table"].update({"heap_bytes": heap, "index_bytes": idx, "total_bytes": total})
            (res_dir / "environment.json").write_text(json.dumps(env, indent=2, default=str) + "\n")

            rng = random.Random(args.seed)
            mismatches: list = []
            server_rows: list = []
            results, error = [], None
            started = time.perf_counter()
            with open(res_dir / "samples.csv", "w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["experiment", "config", "workload", "batch", "kind", "round",
                                 "order_position", "latency_ms", "result_ok"])
                try:
                    for g in groups:
                        results.append(run_group(cur, g, args, rng, writer, server_rows, plan_dir, mismatches))
                except Exception as exc:  # keep partial evidence, then restore
                    error = repr(exc)
                    print(f"ERROR during benchmark: {error}", file=sys.stderr)
                finally:
                    cleanup(cur)
            elapsed = time.perf_counter() - started
            with open(res_dir / "server_execution.csv", "w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["experiment", "config", "workload", "batch", "kind", "run",
                            "planning_ms", "execution_ms", "root_shared_hit_blocks", "root_shared_read_blocks"])
                w.writerows(server_rows)
            restored = not unexpected_objects(cur)
            identity_after = dataset_identity(cur)

    # comparisons + server-side summaries
    comp_rows, sample_map = [], {}
    import collections
    srv = collections.defaultdict(list)
    for r in server_rows:
        srv[(r[1], r[2], r[4])].append(r[7])
    for g in results:
        for w, info in g["workloads"].items():
            storage = sum(o["bytes"] for o in g["objects"].values())
            used = info["plans"]["optimized"]["uses_optimization_objects"]
            row = {"experiment": g["experiment"], "config": g["config"], "workload": w,
                   "baseline_n": info["baseline"]["n"], "optimized_n": info["optimized"]["n"],
                   "baseline_p50": info["baseline"]["median"], "optimized_p50": info["optimized"]["median"],
                   "baseline_p95": info["baseline"]["p95"], "optimized_p95": info["optimized"]["p95"],
                   "baseline_p99_approx": info["baseline"]["p99"], "optimized_p99_approx": info["optimized"]["p99"],
                   "baseline_mean": info["baseline"]["mean"], "optimized_mean": info["optimized"]["mean"],
                   "baseline_stdev": info["baseline"]["stdev"], "optimized_stdev": info["optimized"]["stdev"],
                   "baseline_iqr": info["baseline"]["iqr"], "optimized_iqr": info["optimized"]["iqr"],
                   "speedup": info["comparison"]["speedup_ratio"],
                   "median_change_pct": info["comparison"]["median_change_pct"],
                   "bracketed_ratio_min": min(c["ratio"] for c in info["bracketed_cycles"]),
                   "bracketed_ratio_max": max(c["ratio"] for c in info["bracketed_cycles"]),
                   "baseline_drift_pct": info["baseline_drift"]["spread_pct"],
                   "drift_flagged": info["baseline_drift"]["flagged"],
                   "server_baseline_exec_p50": bs.percentile(srv[(g["config"], w, "baseline")], 50)
                   if srv[(g["config"], w, "baseline")] else None,
                   "server_optimized_exec_p50": bs.percentile(srv[(g["config"], w, "optimized")], 50)
                   if srv[(g["config"], w, "optimized")] else None,
                   "optimizer_used_object": bool(info["plans"]["optimized"]["uses_optimization_objects"]),
                   "storage_bytes": storage,
                   "used_storage_bytes": sum(g["objects"][o]["bytes"] for o in used) if used else storage,
                   "correct": info["correct"]}
            info["server_side_execution_ms"] = {
                k: bs.summarize(srv[(g["config"], w, k)]) for k in ("baseline", "optimized")
                if srv[(g["config"], w, k)]}
            comp_rows.append(row)
    # latency samples for charts, from the CSV just written
    with open(res_dir / "samples.csv") as fh:
        for s in csv.DictReader(fh):
            sample_map.setdefault((s["config"], s["workload"]), {"baseline": [], "optimized": []})[
                s["kind"]].append(float(s["latency_ms"]))
    if comp_rows:
        with open(res_dir / "comparisons.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(comp_rows[0]))
            w.writeheader()
            for r in comp_rows:
                w.writerow({k: (f"{v:.6f}" if isinstance(v, float) else v) for k, v in r.items()})
    charts = []
    if comp_rows and not args.smoke:
        charts = make_charts(comp_rows, sample_map, res_dir / "charts", run_id, args)

    failures = ([f"result mismatch: {m['group']}/{m['workload']} {m['batch']}#{m['sample']}"
                 for m in mismatches] + ([error] if error else []) +
                ([] if restored else ["baseline schema not restored"]) +
                ([] if identity_after == identity else ["dataset changed"]))
    if mismatches:
        (res_dir / "mismatches.json").write_text(json.dumps(mismatches, indent=2, default=str) + "\n")
    summary = {
        "run_id": run_id, "phase": "benchmark", "smoke": args.smoke,
        "parameters": {"samples": args.samples, "warmups": args.warmups, "cycles": args.cycles,
                       "server_samples": args.server_samples, "seed": args.seed,
                       "experiment": args.experiment},
        "percentile_method": "linear interpolation between closest ranks (Hyndman-Fan type 7)",
        "drift_threshold_pct": bs.DRIFT_THRESHOLD_PCT,
        "timing": {"primary": "client-observed: perf_counter_ns around execute()+fetchall(), "
                              "autocommit, auto-prepare disabled",
                   "diagnostic": "server-side EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) Execution Time; "
                                 "not mixed with client timings"},
        "elapsed_seconds": round(elapsed, 1),
        "dataset_before": identity, "dataset_after": identity_after,
        "baseline_restored": restored, "groups": results, "comparisons": comp_rows,
        "charts": [f"charts/{c}" for c in charts], "failures": failures,
    }
    (res_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=rb.jsonable) + "\n")
    print(f"\nelapsed {elapsed:.0f}s  results: {res_dir}\nplans: {plan_dir}")
    print(f"baseline restored: {restored}  dataset unchanged: {identity_after == identity}")
    for r in comp_rows:
        print(f"  {r['config']:26s} {r['workload']:22s} p50 {r['baseline_p50']:8.3f} -> "
              f"{r['optimized_p50']:8.3f} ms  ({r['speedup']:.1f}x)  drift {r['baseline_drift_pct']:.1f}%"
              f"{' FLAG' if r['drift_flagged'] else ''}  used={r['optimizer_used_object']}  ok={r['correct']}")
    if failures:
        print("FAILURES:", *failures, sep="\n  ", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
