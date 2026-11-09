#!/usr/bin/env python3
"""Generate synthetic e-commerce transactions, reproducibly from a seed.

The data is skewed on purpose (a few big merchants, merchants tied to a home
region and category, category-specific amounts, growth over time). Rows are
produced one day at a time, so memory stays small.

Rows come out in timestamp order with sequential ids, like an append-only table.
That physical time order is what makes the BRIN experiment work.

Usage (standalone, writes a CSV file):
    python data/generate_transactions.py --count 1000000 --seed 42 \
        --output-dir data/output

The loader (scripts/load_data.py) imports this module and streams rows
directly into PostgreSQL COPY without writing an intermediate file.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from random import Random
from typing import Iterator, NamedTuple

GENERATOR_VERSION = "1.0.0"

# Value lists must match the CHECK constraints in sql/schema.sql.

# category -> (global frequency weight, lognormal mu, lognormal sigma)
# exp(mu) is the median amount; sigma controls the right tail.
CATEGORIES: dict[str, tuple[float, float, float]] = {
    "Groceries":   (0.22, 3.50, 0.50),   # median ~$33, narrow
    "Clothing":    (0.20, 3.90, 0.60),   # median ~$49
    "Electronics": (0.16, 5.20, 0.85),   # median ~$181, long tail
    "Home":        (0.12, 4.40, 0.80),   # median ~$81
    "Beauty":      (0.10, 3.40, 0.55),   # median ~$30
    "Sports":      (0.08, 4.20, 0.75),   # median ~$67
    "Books":       (0.07, 2.90, 0.45),   # median ~$18, narrow
    "Automotive":  (0.05, 4.80, 1.00),   # median ~$122, widest tail
}
REGIONS: dict[str, float] = {
    "NORTH_AMERICA": 0.38,
    "EUROPE": 0.27,
    "ASIA_PACIFIC": 0.22,
    "LATIN_AMERICA": 0.08,
    "MIDDLE_EAST_AFRICA": 0.05,
}
PAYMENT_METHODS: dict[str, float] = {
    "CREDIT_CARD": 0.45,
    "DEBIT_CARD": 0.25,
    "DIGITAL_WALLET": 0.22,
    "BANK_TRANSFER": 0.08,
}
STATUSES: dict[str, float] = {
    "SUCCESS": 0.90,
    "FAILED": 0.05,
    "REFUNDED": 0.03,
    "PENDING": 0.02,
}
# Relative activity by hour of day (UTC): quiet overnight, lunch and evening peaks.
HOUR_WEIGHTS: list[float] = [
    0.30, 0.20, 0.15, 0.12, 0.12, 0.20,  # 00-05
    0.40, 0.70, 0.90, 1.00, 1.10, 1.30,  # 06-11
    1.50, 1.40, 1.10, 1.00, 1.00, 1.20,  # 12-17
    1.50, 1.80, 1.90, 1.60, 1.00, 0.60,  # 18-23
]
# Monday .. Sunday multipliers.
WEEKDAY_FACTORS: list[float] = [0.90, 0.95, 0.97, 1.00, 1.15, 1.25, 1.10]

# Merchant behaviour
MERCHANT_ID_BASE = 1001
MERCHANT_ZIPF_EXPONENT = 1.05
MERCHANT_ZIPF_OFFSET = 5          # softens the head so one merchant isn't ~20% of volume
HOME_REGION_PROBABILITY = 0.80
PRIMARY_CATEGORY_PROBABILITY = 0.60
SECONDARY_CATEGORY_PROBABILITY = 0.25   # remaining 0.15 -> any category (global weights)

MIN_AMOUNT = 0.50
MAX_AMOUNT = 25_000.00

MAX_RECORDS = 50_000_000
MAX_MERCHANTS = 1_000_000

DEFAULT_COUNT = 1_000_000
DEFAULT_SEED = 42
DEFAULT_START = dt.date(2025, 11, 1)
DEFAULT_END = dt.date(2026, 11, 1)   # exclusive -> exactly 12 months
DEFAULT_MERCHANTS = 1_000
DEFAULT_BATCH_SIZE = 50_000

COLUMNS = (
    "transaction_id", "merchant_id", "category", "region",
    "payment_method", "status", "amount", "transaction_date",
)


class Transaction(NamedTuple):
    transaction_id: int
    merchant_id: int
    category: str
    region: str
    payment_method: str
    status: str
    amount: str                 # fixed 2-decimal string; exact for NUMERIC(12,2)
    transaction_date: dt.datetime  # timezone-aware, UTC


class MerchantProfile(NamedTuple):
    merchant_id: int
    home_region: str
    primary_category: str
    secondary_category: str


@dataclass(frozen=True)
class GeneratorConfig:
    count: int = DEFAULT_COUNT
    seed: int = DEFAULT_SEED
    start_date: dt.date = DEFAULT_START
    end_date: dt.date = DEFAULT_END     # exclusive
    num_merchants: int = DEFAULT_MERCHANTS

    def __post_init__(self) -> None:
        if not isinstance(self.count, int) or isinstance(self.count, bool) or self.count < 1:
            raise ValueError("count must be a positive integer")
        if self.count > MAX_RECORDS:
            raise ValueError(f"count must be <= {MAX_RECORDS}")
        if not isinstance(self.seed, int) or isinstance(self.seed, bool):
            raise ValueError("seed must be an integer")
        if not (isinstance(self.start_date, dt.date) and isinstance(self.end_date, dt.date)):
            raise ValueError("start_date and end_date must be dates")
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        if not isinstance(self.num_merchants, int) or not 1 <= self.num_merchants <= MAX_MERCHANTS:
            raise ValueError(f"num_merchants must be between 1 and {MAX_MERCHANTS}")

    @property
    def start_ts(self) -> dt.datetime:
        return dt.datetime.combine(self.start_date, dt.time(), dt.timezone.utc)

    @property
    def end_ts(self) -> dt.datetime:
        return dt.datetime.combine(self.end_date, dt.time(), dt.timezone.utc)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["start_date"] = self.start_date.isoformat()
        d["end_date"] = self.end_date.isoformat()
        return d


def _cumulative(weights: list[float]) -> list[float]:
    total, out = 0.0, []
    for w in weights:
        total += w
        out.append(total)
    return out


def build_merchants(cfg: GeneratorConfig) -> tuple[list[int], list[float], dict[int, MerchantProfile]]:
    """Return (merchant ids in popularity order, cumulative weights, profiles).

    Popularity follows a shifted Zipf law over rank. Ids are shuffled so the
    busiest merchant is not simply the smallest id.
    """
    rng = Random(f"merchants:{cfg.seed}")
    ids = list(range(MERCHANT_ID_BASE, MERCHANT_ID_BASE + cfg.num_merchants))
    rng.shuffle(ids)
    weights = [1.0 / (rank + MERCHANT_ZIPF_OFFSET) ** MERCHANT_ZIPF_EXPONENT
               for rank in range(1, cfg.num_merchants + 1)]

    region_names, region_w = list(REGIONS), list(REGIONS.values())
    cat_names = list(CATEGORIES)
    cat_w = [CATEGORIES[c][0] for c in cat_names]

    profiles: dict[int, MerchantProfile] = {}
    for mid in ids:
        home = rng.choices(region_names, weights=region_w)[0]
        primary = rng.choices(cat_names, weights=cat_w)[0]
        others = [c for c in cat_names if c != primary]
        secondary = rng.choices(others, weights=[CATEGORIES[c][0] for c in others])[0]
        profiles[mid] = MerchantProfile(mid, home, primary, secondary)
    return ids, _cumulative(weights), profiles


def daily_counts(cfg: GeneratorConfig) -> list[tuple[dt.date, int]]:
    """Allocate exactly cfg.count rows across days (largest-remainder method).

    Day weight = growth trend x weekday factor x holiday-season boost x noise.
    """
    rng = Random(f"days:{cfg.seed}")
    n_days = (cfg.end_date - cfg.start_date).days
    days = [cfg.start_date + dt.timedelta(days=i) for i in range(n_days)]
    weights = []
    for i, d in enumerate(days):
        trend = 0.75 + 0.50 * (i / max(n_days - 1, 1))      # ~1.7x growth start -> end
        weekday = WEEKDAY_FACTORS[d.weekday()]
        holiday = 1.35 if (d.month == 11 and d.day >= 20) or d.month == 12 else 1.0
        noise = rng.uniform(0.92, 1.08)
        weights.append(trend * weekday * holiday * noise)
    total = sum(weights)
    exact = [cfg.count * w / total for w in weights]
    counts = [int(x) for x in exact]
    remainder = cfg.count - sum(counts)
    # Deterministic tie-break: larger fractional part first, then earlier day.
    order = sorted(range(n_days), key=lambda i: (-(exact[i] - counts[i]), i))
    for i in order[:remainder]:
        counts[i] += 1
    return list(zip(days, counts))


def iter_transactions(cfg: GeneratorConfig) -> Iterator[Transaction]:
    """Lazily yield exactly cfg.count transactions in timestamp order."""
    rng = Random(f"rows:{cfg.seed}")
    merchant_ids, merchant_cum, profiles = build_merchants(cfg)

    hours = list(range(24))
    hour_cum = _cumulative(HOUR_WEIGHTS)
    region_names, region_cum = list(REGIONS), _cumulative(list(REGIONS.values()))
    cat_names = list(CATEGORIES)
    cat_cum = _cumulative([CATEGORIES[c][0] for c in cat_names])
    pm_names, pm_cum = list(PAYMENT_METHODS), _cumulative(list(PAYMENT_METHODS.values()))
    st_names, st_cum = list(STATUSES), _cumulative(list(STATUSES.values()))

    utc = dt.timezone.utc
    next_id = 1
    for day, n in daily_counts(cfg):
        midnight = dt.datetime(day.year, day.month, day.day, tzinfo=utc)
        rows = []
        for _ in range(n):
            hour = rng.choices(hours, cum_weights=hour_cum)[0]
            ts = midnight + dt.timedelta(seconds=hour * 3600 + rng.randrange(3600))

            mid = rng.choices(merchant_ids, cum_weights=merchant_cum)[0]
            prof = profiles[mid]

            region = (prof.home_region if rng.random() < HOME_REGION_PROBABILITY
                      else rng.choices(region_names, cum_weights=region_cum)[0])

            r = rng.random()
            if r < PRIMARY_CATEGORY_PROBABILITY:
                category = prof.primary_category
            elif r < PRIMARY_CATEGORY_PROBABILITY + SECONDARY_CATEGORY_PROBABILITY:
                category = prof.secondary_category
            else:
                category = rng.choices(cat_names, cum_weights=cat_cum)[0]

            _, mu, sigma = CATEGORIES[category]
            amount = min(max(rng.lognormvariate(mu, sigma), MIN_AMOUNT), MAX_AMOUNT)

            payment = rng.choices(pm_names, cum_weights=pm_cum)[0]
            status = rng.choices(st_names, cum_weights=st_cum)[0]
            rows.append((ts, mid, category, region, payment, status, f"{amount:.2f}"))

        rows.sort(key=lambda row: row[0])  # stable -> deterministic for equal timestamps
        for ts, mid, category, region, payment, status, amount in rows:
            yield Transaction(next_id, mid, category, region, payment, status, amount, ts)
            next_id += 1


# Output formats shared by the CSV writer, the loader and validation.

def to_copy_line(t: Transaction) -> str:
    """Tab-separated line for PostgreSQL COPY text format.

    Values come from fixed vocabularies and numbers, so no escaping is needed.
    """
    return (f"{t.transaction_id}\t{t.merchant_id}\t{t.category}\t{t.region}\t"
            f"{t.payment_method}\t{t.status}\t{t.amount}\t"
            f"{t.transaction_date:%Y-%m-%d %H:%M:%S}+00\n")


def fingerprint_line(t: Transaction) -> str:
    """Canonical representation; matches FINGERPRINT_SQL row expression exactly."""
    return (f"{t.transaction_id}|{t.merchant_id}|{t.category}|{t.region}|"
            f"{t.payment_method}|{t.status}|{t.amount}|"
            f"{t.transaction_date:%Y-%m-%dT%H:%M:%S}")


# MD5 over all canonical rows ordered by transaction_id, joined with '\n'.
# Explicit ORDER BY: never relies on physical/unspecified row order.
FINGERPRINT_SQL = """
SELECT md5(string_agg(
    transaction_id::text || '|' || merchant_id::text || '|' || category || '|' ||
    region || '|' || payment_method || '|' || status || '|' || amount::text || '|' ||
    to_char(transaction_date AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS'),
    E'\\n' ORDER BY transaction_id))
FROM transactions
"""


class Fingerprinter:
    """Incremental MD5 matching FINGERPRINT_SQL."""

    def __init__(self) -> None:
        self._md5 = hashlib.md5()
        self._first = True

    def update(self, t: Transaction) -> None:
        if not self._first:
            self._md5.update(b"\n")
        self._md5.update(fingerprint_line(t).encode())
        self._first = False

    def hexdigest(self) -> str:
        return self._md5.hexdigest()


def dataset_fingerprint(cfg: GeneratorConfig) -> str:
    fp = Fingerprinter()
    for t in iter_transactions(cfg):
        fp.update(t)
    return fp.hexdigest()



def _positive_int(value: str) -> int:
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an integer: {value!r}")
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1: {value!r}")
    return n


def _iso_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}")


def add_generator_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=_positive_int, default=DEFAULT_COUNT,
                        help=f"number of transactions (default {DEFAULT_COUNT})")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help=f"random seed (default {DEFAULT_SEED})")
    parser.add_argument("--start-date", type=_iso_date, default=DEFAULT_START,
                        help=f"first day, inclusive, UTC (default {DEFAULT_START})")
    parser.add_argument("--end-date", type=_iso_date, default=DEFAULT_END,
                        help=f"last day, exclusive, UTC (default {DEFAULT_END})")
    parser.add_argument("--merchants", type=_positive_int, default=DEFAULT_MERCHANTS,
                        help=f"number of merchants (default {DEFAULT_MERCHANTS})")
    parser.add_argument("--batch-size", type=_positive_int, default=DEFAULT_BATCH_SIZE,
                        help=f"rows buffered per write (default {DEFAULT_BATCH_SIZE})")


def config_from_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> GeneratorConfig:
    try:
        return GeneratorConfig(count=args.count, seed=args.seed, start_date=args.start_date,
                               end_date=args.end_date, num_merchants=args.merchants)
    except ValueError as exc:
        parser.error(str(exc))  # exits with status 2
        raise  # unreachable


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_generator_args(parser)
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"),
                        help="directory for the CSV file (default data/output)")
    args = parser.parse_args(argv)
    cfg = config_from_args(parser, args)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / f"transactions_seed{cfg.seed}_n{cfg.count}.csv"
    tmp = out.with_suffix(".csv.partial")

    fp = Fingerprinter()
    written = 0
    started = time.perf_counter()
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        fh.write(",".join(COLUMNS) + "\n")
        buf: list[str] = []
        for t in iter_transactions(cfg):
            fp.update(t)
            buf.append(to_copy_line(t).replace("\t", ","))
            if len(buf) >= args.batch_size:
                fh.write("".join(buf))
                written += len(buf)
                buf.clear()
        fh.write("".join(buf))
        written += len(buf)
    tmp.replace(out)  # atomic: no half-written file under the final name
    elapsed = time.perf_counter() - started

    print(f"generated {written} records in {elapsed:.2f}s -> {out}")
    print(f"config: {cfg.to_dict()}")
    print(f"fingerprint_md5: {fp.hexdigest()}")
    return 0 if written == cfg.count else 1


if __name__ == "__main__":
    sys.exit(main())
