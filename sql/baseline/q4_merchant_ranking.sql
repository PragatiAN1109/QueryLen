-- Q4: High-value merchant ranking
-- Question: which merchants generate the most successful payment volume?
-- Tests: full-table scan, high-cardinality GROUP BY (1,000 groups), HAVING, top-N sort.
-- @param status = SUCCESS
-- @param min_transactions = 500
-- @param limit = 20
SELECT merchant_id,
       COUNT(*)    AS transaction_count,
       SUM(amount) AS total_revenue
FROM transactions
WHERE status = 'SUCCESS'
GROUP BY merchant_id
HAVING COUNT(*) >= 500
ORDER BY total_revenue DESC, merchant_id
LIMIT 20;
