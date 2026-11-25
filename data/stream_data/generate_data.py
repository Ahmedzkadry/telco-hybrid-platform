"""Generate a synthetic telecom churn dataset (no real customer information).

Batch data (once):   python data/stream_data/generate_data.py --customers 120000 --days 12 --events-per-day 85000
Streaming file:      python data/stream_data/generate_data.py --date 2026-11-21 --events-per-day 5000
"""
import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data" / "batch_data"     # historical files: profiles + 12 daily CSVs
STREAM = ROOT / "data" / "stream_data"    # new-day files that the Kafka producer reads
BASE_DATE = date(2026, 11, 20)


def generate_day(day, customer_ids, rng, events_per_day, folder=None):
    """One CSV per day makes Airflow re-runs and backfills easy to teach."""
    path = (folder or SOURCE) / f"activity_{day.isoformat()}.csv"
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    picks = rng.choice(customer_ids, size=events_per_day, replace=True)
    minutes = rng.gamma(2.5, 26, events_per_day).round(2)
    data = rng.gamma(2.1, 0.9, events_per_day).round(3)
    support = rng.binomial(1, 0.045, events_per_day)
    late = rng.binomial(1, 0.018, events_per_day)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow([
            "event_id", "customer_id", "activity_date", "usage_minutes", "data_gb",
            "support_call", "late_payment"
        ])
        writer.writerows(zip(
            (f"{day:%Y%m%d}-{i:07d}" for i in range(events_per_day)),
            picks, (day.isoformat() for _ in range(events_per_day)), minutes, data,
            support, late,
        ))
    print(f"Generated {path.name}: {events_per_day:,} events")
    return path


def make_dataset(customers=120000, days=12, events_per_day=85000):
    SOURCE.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    ids = np.arange(1, customers + 1, dtype=np.int32)
    age = rng.integers(18, 76, customers)
    region = rng.choice(["Cairo", "Alex", "Delta", "UpperEgypt"], customers)
    plan = rng.choice(["Basic", "Plus", "Premium"], customers, p=[0.5, 0.35, 0.15])
    contract = rng.choice(["Monthly", "Annual"], customers, p=[0.6, 0.4])
    tenure = rng.integers(1, 73, customers)
    fee = np.where(plan == "Basic", 130, np.where(plan == "Plus", 245, 440))
    fee = fee + rng.integers(0, 50, customers)
    autopay = rng.binomial(1, 0.63, customers)

    # Follow customers through the observed window; target is simulated after the window.
    support_totals = np.zeros(customers, dtype=np.int32)
    late_totals = np.zeros(customers, dtype=np.int32)
    usage_totals = np.zeros(customers, dtype=np.float64)
    active_totals = np.zeros(customers, dtype=np.int32)
    for offset in range(days):
        day = BASE_DATE - timedelta(days=days - offset - 1)
        path = generate_day(day, ids, rng, events_per_day, folder=SOURCE)
        # Re-read the generated file to keep labels reproducible after regeneration.
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            for record in reader:
                idx = int(record["customer_id"]) - 1
                support_totals[idx] += int(record["support_call"])
                late_totals[idx] += int(record["late_payment"])
                usage_totals[idx] += float(record["usage_minutes"])
                active_totals[idx] += 1

    avg_usage = usage_totals / np.maximum(active_totals, 1)
    # Synthetic probability; churn is NOT a field that leaks into the features.
    score = (
        -1.35
        + 0.55 * (contract == "Monthly")
        + 0.25 * (plan == "Basic")
        + 0.40 * (autopay == 0)
        + 0.18 * (support_totals >= 2)
        + 0.25 * (late_totals >= 1)
        - 0.014 * tenure
        - 0.007 * avg_usage
    )
    probability = 1 / (1 + np.exp(-score))
    churn = rng.binomial(1, probability)
    with (SOURCE / "customer_profiles.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow([
            "customer_id", "age", "region", "plan_type", "contract_type",
            "tenure_months", "monthly_fee", "autopay", "churned",
        ])
        writer.writerows(zip(ids, age, region, plan, contract, tenure, fee, autopay, churn))
    print(f"Customers: {customers:,}; activity: {days * events_per_day:,} events")
    print(f"Simulated churn rate: {churn.mean():.1%}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=120000)
    parser.add_argument("--days", type=int, default=12)
    parser.add_argument("--events-per-day", type=int, default=85000)
    parser.add_argument("--date", help="Generate one new day into data/stream_data")
    args = parser.parse_args()
    if args.customers <= 0 or args.days <= 0 or args.events_per_day <= 0:
        parser.error("Customers, days and events-per-day must all be positive")

    if args.date:
        day = date.fromisoformat(args.date)
        # deterministic per day, so retrying creates identical data
        rng = np.random.default_rng(int(day.strftime("%Y%m%d")))
        generate_day(day, np.arange(1, args.customers + 1), rng,
                     args.events_per_day, folder=STREAM)
    else:
        make_dataset(args.customers, args.days, args.events_per_day)


if __name__ == "__main__":
    main()
    