-- Q5 (narrow variant): selective vs broad filter comparison
-- Question: successful European revenue by payment method for ONE WEEK.
-- Identical to q5_broad.sql except for the date range.
-- @param start_ts = 2026-10-01 00:00:00+00
-- @param end_ts = 2026-10-08 00:00:00+00   (exclusive, 7 days)
-- @param region = EUROPE
-- @param status = SUCCESS
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
