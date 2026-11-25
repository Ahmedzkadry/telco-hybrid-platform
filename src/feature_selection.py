import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
from sklearn.feature_selection import f_classif

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts" / "feature_selection.json"

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
TARGET = "churned"
K = 6  # how many numeric features to keep

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "telco"),
    "user": os.getenv("POSTGRES_USER", "telco"),
    "password": os.getenv("POSTGRES_PASSWORD", "telco123"),
}


def load_gold() -> pd.DataFrame:
    cols = NUMERIC + [TARGET]
    sql = f"SELECT {', '.join(cols)} FROM analytics.customer_churn_features"
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols).astype(float)


def main():
    df = load_gold()
    X, y = df[NUMERIC], df[TARGET].astype(int)
    print(f"Loaded {len(df):,} rows, churn rate {y.mean():.1%}")

    # Filter method: ANOVA F-score of each feature against the target
    f_scores, _ = f_classif(X, y)
    scores = dict(zip(NUMERIC, f_scores))
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    selected = [name for name, _ in ranked[:K]]

    result = {
        "method": "filter (ANOVA F-test)",
        "k": K,
        "f_scores": {name: round(float(score), 2) for name, score in ranked},
        "final_selected": selected,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print("\nFeature scores (higher = stronger link with churn):")
    for name, score in ranked:
        mark = "KEEP" if name in selected else "drop"
        print(f"  {mark}  {name:<26} {score:,.1f}")
    print(f"\nSelected: {selected}")
    print(f"Saved to {OUT}")


if __name__ == "__main__":
    main()
