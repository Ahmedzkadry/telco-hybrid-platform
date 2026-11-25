import os

import pandas as pd
import requests
import streamlit as st

API = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Telecom Churn Dashboard", layout="wide")
st.title("Telecom Churn Dashboard")

# ---------- risk insights (stored scores) ----------
st.header("Risk insights")
try:
    summary = requests.get(f"{API}/analytics/summary", timeout=10).json()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Scored customers", f"{summary['total_customers']:,}")
    col2.metric("Avg churn probability", f"{summary['avg_probability']:.1%}")
    by_risk = summary["by_risk"]
    col3.metric("High risk", f"{by_risk.get('High', 0):,}")
    col4.metric("Medium risk", f"{by_risk.get('Medium', 0):,}")

    chart = pd.Series(by_risk).reindex(["Low", "Medium", "High"]).fillna(0)
    st.bar_chart(chart)

    st.subheader("Recent predictions")
    recent = requests.get(f"{API}/predictions/recent", params={"limit": 20}, timeout=10).json()
    st.dataframe(pd.DataFrame(recent), use_container_width=True)
except Exception as exc:
    st.error(f"Could not reach the API at {API}: {exc}")

# ---------- single customer lookup ----------
st.header("Customer lookup")
customer_id = st.number_input("Customer ID", min_value=1, value=1, step=1)
if st.button("Get stored risk"):
    r = requests.get(f"{API}/customers/{int(customer_id)}/risk", timeout=10)
    if r.status_code == 200:
        st.json(r.json())
    else:
        st.warning("No stored score for this customer")

# ---------- on-demand prediction form ----------
st.header("New prediction")
with st.form("predict_form"):
    c1, c2, c3 = st.columns(3)
    region = c1.selectbox("Region", ["Cairo", "Alex", "Delta", "UpperEgypt"])
    plan_type = c2.selectbox("Plan", ["Basic", "Plus", "Premium"])
    contract_type = c3.selectbox("Contract", ["Monthly", "Annual"])
    age = c1.number_input("Age", 18, 100, 35)
    tenure = c2.number_input("Tenure (months)", 0, 200, 12)
    fee = c3.number_input("Monthly fee", 0.0, 2000.0, 200.0)
    autopay = c1.selectbox("Autopay", [1, 0])
    minutes = c2.number_input("Avg daily minutes", 0.0, 1000.0, 50.0)
    data_gb = c3.number_input("Avg daily data (GB)", 0.0, 100.0, 1.2)
    support = c1.number_input("Support calls (30d)", 0, 100, 1)
    late = c2.number_input("Late payments (30d)", 0, 100, 0)
    inactive = c3.number_input("Days since last activity", 0, 365, 2)
    submitted = st.form_submit_button("Predict")

if submitted:
    payload = {
        "region": region,
        "plan_type": plan_type,
        "contract_type": contract_type,
        "age": int(age),
        "tenure_months": int(tenure),
        "monthly_fee": float(fee),
        "autopay": int(autopay),
        "avg_daily_minutes": float(minutes),
        "avg_daily_data_gb": float(data_gb),
        "support_calls_30d": int(support),
        "late_payments_30d": int(late),
        "days_since_last_activity": int(inactive),
    }
    r = requests.post(f"{API}/predict", json=payload, timeout=10)
    if r.status_code == 200:
        result = r.json()
        st.metric("Churn probability", f"{result['churn_probability']:.1%}")
        st.write(f"Risk level: **{result['risk_level']}**")
    else:
        st.error(r.text)
