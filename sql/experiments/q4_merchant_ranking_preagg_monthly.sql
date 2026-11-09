-- Q4 answered from exp_merchant_monthly_success (rows already filtered to SUCCESS).
-- @param status = SUCCESS   (applied when the summary was built)
-- @param min_transactions = 500
-- @param limit = 20
SELECT merchant_id,
       SUM(txn_count)::bigint AS transaction_count,
       SUM(revenue)           AS total_revenue
FROM exp_merchant_monthly_success
GROUP BY merchant_id
HAVING SUM(txn_count) >= 500
ORDER BY total_revenue DESC, merchant_id
LIMIT 20;
