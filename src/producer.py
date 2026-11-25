import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
STREAM_DIR = Path(os.getenv("STREAM_DIR", ROOT / "data" / "stream_data"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "telco.activity.raw"


def to_event(row: dict) -> dict:
    """Turn a CSV activity row into the live event JSON from the assignment."""
    if int(row["support_call"]) == 1:
        event_type = "support"
    elif int(row["late_payment"]) == 1:
        event_type = "late_payment"
    else:
        event_type = "usage"
    is_usage = event_type == "usage"
    return {
        "event_id": row["event_id"],
        "customer_id": int(row["customer_id"]),
        "event_type": event_type,
        "usage_minutes": float(row["usage_minutes"]) if is_usage else 0.0,
        "data_gb": float(row["data_gb"]) if is_usage else 0.0,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="CSV path (default: newest in data/stream_data)")
    parser.add_argument("--limit", type=int, default=100, help="how many events to send")
    parser.add_argument("--delay", type=float, default=0.2, help="seconds between events")
    args = parser.parse_args()

    path = Path(args.file) if args.file else sorted(STREAM_DIR.glob("activity_*.csv"))[-1]
    print(f"Reading {path.name}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )

    sent = 0
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if sent >= args.limit:
                break
            event = to_event(row)
            producer.send(TOPIC, key=event["customer_id"], value=event)
            sent += 1
            if sent % 20 == 0:
                print(f"sent {sent} events")
            time.sleep(args.delay)
    producer.flush()
    print(f"Done: {sent} events sent from {path.name}")
