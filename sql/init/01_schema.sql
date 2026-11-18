CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- STAGING Layer
CREATE TABLE IF NOT EXISTS stg.currency_rates_raw (
    rate_date date NOT NULL,
    payload jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_rates_raw_date
    ON stg.currency_rates_raw (rate_date);

-- CORE Layer
CREATE TABLE IF NOT EXISTS core.currency_rates (
    rate_date date NOT NULL,
    char_code varchar(3) NOT NULL,
    name text NOT NULL,
    nominal integer NOT NULL,
    rate numeric(18, 6) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, char_code)
);

-- MART Layer
CREATE TABLE IF NOT EXISTS mart.currency_rates_daily (
    rate_date date NOT NULL,
    char_code varchar(3) NOT NULL,
    rate_per_unit numeric(18, 6) NOT NULL,
    prev_rate numeric(18, 6),
    change_pct numeric(8, 4),
    avg_7d numeric(18, 6),
    PRIMARY KEY (rate_date, char_code)
);
