import argparse
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.model_service import model_version, predict_proba, risk_level

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "telco"),
    "user": os.getenv("POSTGRES_USER", "telco"),
    "password": os.getenv("POSTGRES_PASSWORD", "telco123"),
}

# Gold row + recent live-event aggregates, one row per customer
SQL = """
SELECT
    g.customer_id, g.region, g.plan_type, g.contract_type,
    g.age, g.tenure_months, g.monthly_fee, g.autopay,
    g.avg_daily_minutes, g.avg_daily_data_gb,
    g.support_calls_30d + COALESCE(l.live_support, 0)  AS support_calls_30d,
    g.late_payments_30d + COALESCE(l.live_late, 0)     AS late_payments_30d,
    CASE WHEN l.customer_id IS NOT NULL THEN 0
         ELSE g.days_since_last_activity END           AS days_since_last_activity
FROM analytics.customer_churn_features g
{join} JOIN (
    SELECT customer_id,
           SUM(support_call) AS live_support,
           SUM(late_payment) AS live_late
    FROM raw.live_events
    GROUP BY customer_id
) l ON l.customer_id = g.customer_id
"""

UPSERT = """
INSERT INTO serving.predictions
    (customer_id, churn_probability, risk_level, model_version, updated_at)
VALUES %s
ON CONFLICT (customer_id) DO UPDATE SET
    churn_probability = EXCLUDED.churn_probability,
    risk_level        = EXCLUDED.risk_level,
    model_version     = EXCLUDED.model_version,
    updated_at        = EXCLUDED.updated_at
"""

NUMERIC = [
    "age",
    "tenure_months",
    "monthly_fee",
    "autopay",
    "avg_daily_minutes",
    "avg_daily_data_gb",
    "support_calls_30d",
    "late_payments_30d",
    "days_since_last_activity",
]


def main(only_live: bool):
    # LEFT JOIN = score everyone, INNER JOIN = only customers with live events
    sql = SQL.format(join="INNER" if only_live else "LEFT")
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)

    if df.empty:
        print("No customers to score (no live events yet?)")
        return
    df[NUMERIC] = df[NUMERIC].astype(float)
    print(f"Scoring {len(df):,} customers (only_live={only_live})")

    df["churn_probability"] = predict_proba(df)
    df["risk_level"] = df["churn_probability"].map(risk_level)
    version = model_version()

    rows = [
        (int(cid), float(p), r, version)
        for cid, p, r in zip(df["customer_id"], df["churn_probability"], df["risk_level"])
    ]
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(
            cur,
            UPSERT,
            rows,
            template="(%s, %s, %s, %s, now())",
            page_size=5000,
        )
    print(f"Stored {len(rows):,} predictions, model_version={version}")
    print(df["risk_level"].value_counts().to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only-live", action="store_true", help="score only customers that have live events"
    )
    main(parser.parse_args().only_live)
