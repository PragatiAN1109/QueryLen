-- pgAdmin walkthrough: Q5 narrow before and after a BRIN index
--
-- Run each statement on its own in pgAdmin's Query Tool: highlight it, then use the
-- button named in its step. Nothing here runs automatically.
-- Before you start, open Explain Options (the dropdown next to Explain Analyze) and
-- enable Buffers, Costs, Timing and Summary.

-- ============================================================================
-- Step 1: Original query
-- Highlight only the SELECT below and click Explain Analyze (not Execute).
-- Expect a Parallel Seq Scan that reads every page of the table.
-- Copied unchanged from sql/baseline/q5_narrow.sql.
-- ============================================================================
SELECT payment_method,
       COUNT(*)              AS transaction_count,
       SUM(amount)           AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE transaction_date >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
  AND transaction_date <  TIMESTAMPTZ '2026-10-08 00:00:00+00'
  AND region = 'EUROPE'
  AND status = 'SUCCESS'
GROUP BY payment_method
ORDER BY total_revenue DESC, payment_method;

-- ============================================================================
-- Step 2: Create the BRIN index
-- Highlight this statement and click Execute Query (not Explain Analyze).
-- Same definition the benchmark runner uses (benchmarks/run_benchmarks.py).
-- ============================================================================
CREATE INDEX exp_brin_txn_date_ppr128 ON transactions USING brin (transaction_date) WITH (pages_per_range = 128);

-- ============================================================================
-- Step 3: Optimized query
-- Highlight the same SELECT and click Explain Analyze again.
-- Expect a Bitmap Index Scan on exp_brin_txn_date_ppr128 feeding a Bitmap Heap Scan.
-- ============================================================================
SELECT payment_method,
       COUNT(*)              AS transaction_count,
       SUM(amount)           AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE transaction_date >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
  AND transaction_date <  TIMESTAMPTZ '2026-10-08 00:00:00+00'
  AND region = 'EUROPE'
  AND status = 'SUCCESS'
GROUP BY payment_method
ORDER BY total_revenue DESC, payment_method;

-- ============================================================================
-- Step 4: Cleanup
-- Highlight this statement and click Execute Query. It drops only the walkthrough
-- index; the primary key and all tables are untouched.
-- ============================================================================
DROP INDEX IF EXISTS exp_brin_txn_date_ppr128;
