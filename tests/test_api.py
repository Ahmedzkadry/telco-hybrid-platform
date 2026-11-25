from fastapi.testclient import TestClient

from src import api, model_service

client = TestClient(api.app)

VALID = {
    "region": "Cairo",
    "plan_type": "Basic",
    "contract_type": "Monthly",
    "age": 35,
    "tenure_months": 12,
    "monthly_fee": 200,
    "autopay": 1,
    "avg_daily_minutes": 50,
    "avg_daily_data_gb": 1.2,
    "support_calls_30d": 1,
    "late_payments_30d": 0,
    "days_since_last_activity": 2,
}


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_predict_returns_contract_shape(monkeypatch):
    fake = {"prediction": 1, "churn_probability": 0.72, "risk_level": "High"}
    monkeypatch.setattr(model_service, "predict_one", lambda features: fake)
    response = client.post("/predict", json=VALID)
    assert response.status_code == 200
    assert response.json() == fake


def test_predict_rejects_invalid_region():
    response = client.post("/predict", json={**VALID, "region": "Mars"})
    assert response.status_code == 422


def test_predict_rejects_missing_field():
    incomplete = {k: v for k, v in VALID.items() if k != "age"}
    response = client.post("/predict", json=incomplete)
    assert response.status_code == 422
