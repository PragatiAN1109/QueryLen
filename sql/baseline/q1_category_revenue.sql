-- Q1: Monthly revenue by category
-- Question: which product categories generated the highest revenue in October 2026?
-- Tests: date-range filter, low-cardinality status predicate, small GROUP BY, sort + LIMIT.
-- @param start_ts = 2026-10-01 00:00:00+00
-- @param end_ts = 2026-11-01 00:00:00+00   (exclusive)
-- @param status = SUCCESS
-- @param limit = 5
SELECT category,
       SUM(amount) AS total_revenue,
       COUNT(*)    AS transaction_count
FROM transactions
WHERE transaction_date >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
  AND transaction_date <  TIMESTAMPTZ '2026-11-01 00:00:00+00'
  AND status = 'SUCCESS'
GROUP BY category
ORDER BY total_revenue DESC, category
LIMIT 5;
