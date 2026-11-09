-- Q2: Merchant-specific revenue by region
-- Question: what are volume and revenue for one merchant across regions?
-- Tests: a comparatively selective equality predicate on merchant_id (no index on it).
-- Merchant 1345 is the highest-volume merchant in the seed-42 dataset
-- (SELECT merchant_id FROM transactions GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 1);
-- (run that query to confirm it still holds).
-- @param merchant_id = 1345
-- @param status = SUCCESS
SELECT region,
       SUM(amount)           AS total_revenue,
       COUNT(*)              AS transaction_count,
       ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE merchant_id = 1345
  AND status = 'SUCCESS'
GROUP BY region
ORDER BY total_revenue DESC, region;
