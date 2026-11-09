-- Q3 answered from exp_region_monthly_status. Exact only because the Q3 window
-- [2026-05-01, 2026-11-01) is month-aligned.
-- @param start_month = 2026-05-01
-- @param end_month = 2026-11-01   (exclusive)
-- @param min_transactions = 1000
SELECT region,
       SUM(txn_count)::bigint                                  AS transaction_count,
       (SUM(txn_count) FILTER (WHERE status = 'FAILED'))::bigint AS failed_count,
       ROUND(100.0 * (SUM(txn_count) FILTER (WHERE status = 'FAILED'))::bigint
                   / SUM(txn_count)::bigint, 3)                AS failure_pct
FROM exp_region_monthly_status
WHERE month >= DATE '2026-05-01'
  AND month <  DATE '2026-11-01'
GROUP BY region
HAVING SUM(txn_count) >= 1000
ORDER BY failure_pct DESC, region;
