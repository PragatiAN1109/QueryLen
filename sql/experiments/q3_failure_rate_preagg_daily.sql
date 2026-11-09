-- Q3 answered from exp_region_daily_status. Same semantics as sql/baseline/q3_failure_rate.sql:
-- the window [2026-05-01, 2026-11-01) UTC is day-aligned, so whole days are exact.
-- Percent computed from exact summed integer counts (never averaged percentages).
-- @param start_day = 2026-05-01
-- @param end_day = 2026-11-01   (exclusive)
-- @param min_transactions = 1000
SELECT region,
       SUM(txn_count)::bigint                                  AS transaction_count,
       (SUM(txn_count) FILTER (WHERE status = 'FAILED'))::bigint AS failed_count,
       ROUND(100.0 * (SUM(txn_count) FILTER (WHERE status = 'FAILED'))::bigint
                   / SUM(txn_count)::bigint, 3)                AS failure_pct
FROM exp_region_daily_status
WHERE day >= DATE '2026-05-01'
  AND day <  DATE '2026-11-01'
GROUP BY region
HAVING SUM(txn_count) >= 1000
ORDER BY failure_pct DESC, region;
