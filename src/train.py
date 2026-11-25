import json
import os
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"

CATEGORICAL = ["region", "plan_type", "contract_type"]
ALL_NUMERIC = [
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
ALL_FEATURES = CATEGORICAL + ALL_NUMERIC  # the 12 inputs of the API contract
TARGET = "churned"

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "telco"),
    "user": os.getenv("POSTGRES_USER", "telco"),
    "password": os.getenv("POSTGRES_PASSWORD", "telco123"),
}


def load_gold() -> pd.DataFrame:
    cols = ALL_FEATURES + [TARGET]
    sql = f"SELECT {', '.join(cols)} FROM analytics.customer_churn_features"
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df[ALL_NUMERIC + [TARGET]] = df[ALL_NUMERIC + [TARGET]].astype(float)
    return df


def main():
    selected = json.loads((ARTIFACTS / "feature_selection.json").read_text())["final_selected"]
    print(f"Numeric features used: {selected}")

    df = load_gold()
    X, y = df[ALL_FEATURES], df[TARGET].astype(int)

    # 1) split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # 2) preprocessing + classifier in ONE pipeline (fitted on training data only)
    preprocess = ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
            ("num", StandardScaler(), selected),
        ],
        remainder="drop",  # features that were not selected are ignored
    )
    model = Pipeline(
        [
            ("preprocess", preprocess),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )
    model.fit(X_train, y_train)

    # 3) evaluate on data the model has never seen
    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    metrics = {
        "model_version": datetime.now().strftime("v%Y%m%d-%H%M%S"),
        "model": "LogisticRegression (class_weight=balanced)",
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "numeric_features": selected,
        "categorical_features": CATEGORICAL,
        "accuracy": round(accuracy_score(y_test, pred), 4),
        "precision": round(precision_score(y_test, pred), 4),
        "recall": round(recall_score(y_test, pred), 4),
        "f1": round(f1_score(y_test, pred), 4),
        "roc_auc": round(roc_auc_score(y_test, proba), 4),
    }

    # 4) save
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, ARTIFACTS / "churn_model.joblib")
    (ARTIFACTS / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("\nMetrics:")
    for k in ["accuracy", "precision", "recall", "f1", "roc_auc"]:
        print(f"  {k:<10} {metrics[k]}")
    print(f"\nModel version: {metrics['model_version']}")
    print(f"Saved: {ARTIFACTS / 'churn_model.joblib'}")
    print(f"Saved: {ARTIFACTS / 'metrics.json'}")


if __name__ == "__main__":
    main()
