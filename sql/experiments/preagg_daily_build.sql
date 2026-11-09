-- Build daily summary tables from transactions.
-- Rerunnable: drops and rebuilds both tables in one transaction. Reads transactions only.
-- Day boundaries are UTC calendar days, matching the UTC-midnight windows of Q3.
BEGIN;

DROP TABLE IF EXISTS exp_merchant_daily_success;
CREATE TABLE exp_merchant_daily_success AS
SELECT (transaction_date AT TIME ZONE 'UTC')::date AS day,
       merchant_id,
       COUNT(*)::bigint AS txn_count,
       SUM(amount)      AS revenue        -- NUMERIC, exact
FROM transactions
WHERE status = 'SUCCESS'
GROUP BY 1, 2;
ALTER TABLE exp_merchant_daily_success ADD PRIMARY KEY (day, merchant_id);

-- All statuses kept so total and failed counts come from exact integer counts.
DROP TABLE IF EXISTS exp_region_daily_status;
CREATE TABLE exp_region_daily_status AS
SELECT (transaction_date AT TIME ZONE 'UTC')::date AS day,
       region,
       status,
       COUNT(*)::bigint AS txn_count,
       SUM(amount)      AS revenue
FROM transactions
GROUP BY 1, 2, 3;
ALTER TABLE exp_region_daily_status ADD PRIMARY KEY (day, region, status);

COMMIT;

ANALYZE exp_merchant_daily_success;
ANALYZE exp_region_daily_status;
