-- Build monthly summary tables (UTC calendar months).
-- Answers Q4 (no date filter) and Q3 (window is whole months) exactly; cannot answer
-- windows that start or end mid-month. Rerunnable; reads transactions only.
BEGIN;

DROP TABLE IF EXISTS exp_merchant_monthly_success;
CREATE TABLE exp_merchant_monthly_success AS
SELECT date_trunc('month', transaction_date AT TIME ZONE 'UTC')::date AS month,
       merchant_id,
       COUNT(*)::bigint AS txn_count,
       SUM(amount)      AS revenue
FROM transactions
WHERE status = 'SUCCESS'
GROUP BY 1, 2;
ALTER TABLE exp_merchant_monthly_success ADD PRIMARY KEY (month, merchant_id);

DROP TABLE IF EXISTS exp_region_monthly_status;
CREATE TABLE exp_region_monthly_status AS
SELECT date_trunc('month', transaction_date AT TIME ZONE 'UTC')::date AS month,
       region,
       status,
       COUNT(*)::bigint AS txn_count,
       SUM(amount)      AS revenue
FROM transactions
GROUP BY 1, 2, 3;
ALTER TABLE exp_region_monthly_status ADD PRIMARY KEY (month, region, status);

COMMIT;

ANALYZE exp_merchant_monthly_success;
ANALYZE exp_region_monthly_status;
