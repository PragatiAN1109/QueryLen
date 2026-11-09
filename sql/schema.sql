-- Schema for the transactions table and a small metadata table.
-- Kept minimal on purpose: the only index on transactions is the primary key, so the
-- experiments start from plain sequential scans. Safe to run repeatedly.

SET client_min_messages = warning;

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   BIGINT        NOT NULL,
    merchant_id      INTEGER       NOT NULL,
    category         VARCHAR(50)   NOT NULL,
    region           VARCHAR(20)   NOT NULL,
    payment_method   VARCHAR(30)   NOT NULL,
    status           VARCHAR(20)   NOT NULL,
    amount           NUMERIC(12,2) NOT NULL,
    transaction_date TIMESTAMPTZ   NOT NULL,

    CONSTRAINT transactions_pkey PRIMARY KEY (transaction_id),
    CONSTRAINT transactions_id_positive       CHECK (transaction_id > 0),
    CONSTRAINT transactions_merchant_positive CHECK (merchant_id > 0),
    CONSTRAINT transactions_amount_nonneg     CHECK (amount >= 0),
    CONSTRAINT transactions_category_valid CHECK (category IN (
        'Electronics', 'Clothing', 'Groceries', 'Home',
        'Beauty', 'Sports', 'Automotive', 'Books')),
    CONSTRAINT transactions_region_valid CHECK (region IN (
        'NORTH_AMERICA', 'EUROPE', 'ASIA_PACIFIC',
        'LATIN_AMERICA', 'MIDDLE_EAST_AFRICA')),
    CONSTRAINT transactions_payment_method_valid CHECK (payment_method IN (
        'CREDIT_CARD', 'DEBIT_CARD', 'BANK_TRANSFER', 'DIGITAL_WALLET')),
    CONSTRAINT transactions_status_valid CHECK (status IN (
        'SUCCESS', 'FAILED', 'REFUNDED', 'PENDING'))
);

COMMENT ON TABLE transactions IS
    'Synthetic e-commerce transactions';


CREATE TABLE IF NOT EXISTS dataset_metadata (
    id                SMALLINT    NOT NULL DEFAULT 1,
    generator_version TEXT        NOT NULL,
    seed              BIGINT      NOT NULL,
    record_count      BIGINT      NOT NULL,
    start_date        DATE        NOT NULL,
    end_date          DATE        NOT NULL,
    num_merchants     INTEGER     NOT NULL,
    fingerprint_md5   TEXT        NOT NULL,
    generation_seconds NUMERIC(10,3),
    load_seconds      NUMERIC(10,3),
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT dataset_metadata_pkey PRIMARY KEY (id),
    CONSTRAINT dataset_metadata_single_row CHECK (id = 1)
);
