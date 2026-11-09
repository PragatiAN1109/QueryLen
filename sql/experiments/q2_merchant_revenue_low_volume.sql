-- Q2 for a low-volume merchant. Identical to sql/baseline/q2_merchant_revenue.sql
-- except for the merchant id.
-- Merchant 1427 is the lowest-volume merchant in the seed-42 dataset (139 rows):
-- SELECT merchant_id FROM transactions GROUP BY 1 ORDER BY count(*), 1 LIMIT 1
-- @param merchant_id = 1427
-- @param status = SUCCESS
SELECT region,
       SUM(amount)           AS total_revenue,
       COUNT(*)              AS transaction_count,
       ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE merchant_id = 1427
  AND status = 'SUCCESS'
GROUP BY region
ORDER BY total_revenue DESC, region;
