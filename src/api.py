import os
from typing import Literal

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from src import model_service

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "telco"),
    "user": os.getenv("POSTGRES_USER", "telco"),
    "password": os.getenv("POSTGRES_PASSWORD", "telco123"),
}

app = FastAPI(title="Telecom Churn API")


class PredictRequest(BaseModel):
    """Exactly the 12 model features (no churned, no customer_id)."""

    region: Literal["Cairo", "Alex", "Delta", "UpperEgypt"]
    plan_type: Literal["Basic", "Plus", "Premium"]
    contract_type: Literal["Monthly", "Annual"]
    age: int = Field(ge=18, le=100)
    tenure_months: int = Field(ge=0)
    monthly_fee: float = Field(ge=0)
    autopay: Literal[0, 1]
    avg_daily_minutes: float = Field(ge=0)
    avg_daily_data_gb: float = Field(ge=0)
    support_calls_30d: int = Field(ge=0)
    late_payments_30d: int = Field(ge=0)
    days_since_last_activity: int = Field(ge=0)


class PredictResponse(BaseModel):
    prediction: int
    churn_probability: float
    risk_level: str


def query(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


@app.get("/health")
def health():
    try:
        model_service.load_model()
        model_loaded = True
    except Exception:
        model_loaded = False
    return {"status": "ok", "model_loaded": model_loaded}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    return model_service.predict_one(request.model_dump())


@app.get("/predictions/recent")
def recent_predictions(limit: int = Query(20, ge=1, le=500)):
    return query(
        "SELECT customer_id, churn_probability, risk_level, model_version, updated_at "
        "FROM serving.predictions ORDER BY updated_at DESC, customer_id LIMIT %s",
        (limit,),
    )


@app.get("/customers/{customer_id}/risk")
def customer_risk(customer_id: int):
    rows = query(
        "SELECT customer_id, churn_probability, risk_level, model_version, updated_at "
        "FROM serving.predictions WHERE customer_id = %s",
        (customer_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No stored score for this customer")
    return rows[0]


@app.get("/analytics/summary")
def analytics_summary():
    by_risk = query(
        "SELECT risk_level, count(*) AS customers FROM serving.predictions GROUP BY risk_level"
    )
    totals = query(
        "SELECT count(*) AS total_customers, avg(churn_probability) AS avg_probability "
        "FROM serving.predictions"
    )[0]
    return {
        "total_customers": totals["total_customers"],
        "avg_probability": float(totals["avg_probability"] or 0),
        "by_risk": {r["risk_level"]: r["customers"] for r in by_risk},
    }
