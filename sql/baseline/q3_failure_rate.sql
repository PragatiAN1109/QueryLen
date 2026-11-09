-- Q3: Regional failure-rate investigation
-- Question: which regions have elevated failure rates over the last six months?
-- Tests: conditional aggregation (FILTER) over a broad time window, HAVING, computed sort key.
-- @param start_ts = 2026-05-01 00:00:00+00
-- @param end_ts = 2026-11-01 00:00:00+00   (exclusive)
-- @param min_transactions = 1000
SELECT region,
       COUNT(*)                                    AS transaction_count,
       COUNT(*) FILTER (WHERE status = 'FAILED')   AS failed_count,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'FAILED') / COUNT(*), 3)
                                                   AS failure_pct
FROM transactions
WHERE transaction_date >= TIMESTAMPTZ '2026-05-01 00:00:00+00'
  AND transaction_date <  TIMESTAMPTZ '2026-11-01 00:00:00+00'
GROUP BY region
HAVING COUNT(*) >= 1000
ORDER BY failure_pct DESC, region;
