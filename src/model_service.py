import json
from functools import lru_cache
from pathlib import Path

import joblib
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = ROOT / "artifacts" / "churn_model.joblib"
METRICS_PATH = ROOT / "artifacts" / "metrics.json"

# the 12 inputs of the API contract
FEATURES = [
    "region",
    "plan_type",
    "contract_type",
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


@lru_cache(maxsize=1)
def load_model():
    """Load the saved fitted pipeline. No training happens here."""
    return joblib.load(MODEL_PATH)


def model_version() -> str:
    return json.loads(METRICS_PATH.read_text())["model_version"]


def risk_level(probability: float) -> str:
    if probability < 0.30:
        return "Low"
    if probability < 0.70:
        return "Medium"
    return "High"


def predict_proba(df: pd.DataFrame):
    return load_model().predict_proba(df[FEATURES])[:, 1]


def predict_one(features: dict) -> dict:
    probability = float(predict_proba(pd.DataFrame([features]))[0])
    return {
        "prediction": int(probability >= 0.5),
        "churn_probability": round(probability, 4),
        "risk_level": risk_level(probability),
    }
