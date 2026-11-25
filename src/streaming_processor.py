import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "telco.activity.raw"
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/opt/airflow/project/data/checkpoints/live_events")
PG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "telco"),
    "user": os.getenv("POSTGRES_USER", "telco"),
    "password": os.getenv("POSTGRES_PASSWORD", "telco123"),
}

SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("customer_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("usage_minutes", DoubleType()),
        StructField("data_gb", DoubleType()),
        StructField("created_at", StringType()),
    ]
)

INSERT_SQL = """
INSERT INTO raw.live_events
(event_id, customer_id, event_type, usage_minutes, data_gb, support_call, late_payment, created_at)
VALUES %s
ON CONFLICT (event_id) DO NOTHING
"""


def write_batch(batch_df, batch_id):
    """Insert one micro-batch into PostgreSQL (duplicates are ignored)."""
    rows = [tuple(r) for r in batch_df.collect()]
    if not rows:
        return
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    print(f"batch {batch_id}: stored {len(rows)} events")


def main():
    spark = (
        SparkSession.builder.appName("telco-streaming")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .dropna(subset=["event_id", "customer_id", "event_type"])
        .filter(F.col("event_type").isin("usage", "support", "late_payment"))
        .withColumn("support_call", F.when(F.col("event_type") == "support", 1).otherwise(0))
        .withColumn("late_payment", F.when(F.col("event_type") == "late_payment", 1).otherwise(0))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .select(
            "event_id",
            "customer_id",
            "event_type",
            "usage_minutes",
            "data_gb",
            "support_call",
            "late_payment",
            "created_at",
        )
    )

    query = (
        events.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
