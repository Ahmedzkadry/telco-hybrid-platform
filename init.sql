CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id   INTEGER PRIMARY KEY,
    age           INTEGER,
    region        TEXT,
    plan_type     TEXT,
    contract_type TEXT,
    tenure_months INTEGER,
    monthly_fee   NUMERIC(10,2),
    autopay       INTEGER,
    churned       INTEGER
);

CREATE TABLE IF NOT EXISTS raw.daily_activity (
    customer_id    INTEGER NOT NULL,
    activity_date  DATE NOT NULL,
    events_count   INTEGER,
    usage_minutes  DOUBLE PRECISION,
    data_gb        DOUBLE PRECISION,
    support_calls  INTEGER,
    late_payments  INTEGER,
    PRIMARY KEY (customer_id, activity_date)
);

CREATE TABLE IF NOT EXISTS raw.live_events (
    event_id       TEXT PRIMARY KEY,
    customer_id    INTEGER NOT NULL,
    event_type     TEXT NOT NULL,
    usage_minutes  DOUBLE PRECISION,
    data_gb        DOUBLE PRECISION,
    support_call   INTEGER,
    late_payment   INTEGER,
    created_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS serving.predictions (
    customer_id       INTEGER PRIMARY KEY,
    churn_probability DOUBLE PRECISION NOT NULL,
    risk_level        TEXT NOT NULL,
    model_version     TEXT NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
