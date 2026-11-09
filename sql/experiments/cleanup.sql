-- Remove every experiment object and restore the baseline schema (primary keys only).
-- Only touches the exp_ objects created by benchmarks/run_benchmarks.py.
DROP INDEX IF EXISTS exp_brin_txn_date_ppr128;
DROP INDEX IF EXISTS exp_btree_merchant_status_cov;
DROP TABLE IF EXISTS exp_merchant_daily_success;
DROP TABLE IF EXISTS exp_region_daily_status;
DROP TABLE IF EXISTS exp_merchant_monthly_success;
DROP TABLE IF EXISTS exp_region_monthly_status;
