import os
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = Path(os.getenv("SOURCE_DIR", ROOT / "data" / "batch_data"))
BRONZE_DIR = Path(os.getenv("BRONZE_DIR", ROOT / "data" / "bronze"))
SILVER_DIR = Path(os.getenv("SILVER_DIR", ROOT / "data" / "silver"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", ROOT / "dbt_telco"))

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "telco"),
    "user": os.getenv("POSTGRES_USER", "telco"),
    "password": os.getenv("POSTGRES_PASSWORD", "telco123"),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
JDBC_PROPS = {
    "user": PG["user"],
    "password": PG["password"],
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
}

PROFILE_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType()),
        StructField("age", IntegerType()),
        StructField("region", StringType()),
        StructField("plan_type", StringType()),
        StructField("contract_type", StringType()),
        StructField("tenure_months", IntegerType()),
        StructField("monthly_fee", DoubleType()),
        StructField("autopay", IntegerType()),
        StructField("churned", IntegerType()),
    ]
)

ACTIVITY_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("customer_id", IntegerType()),
        StructField("activity_date", StringType()),
        StructField("usage_minutes", DoubleType()),
        StructField("data_gb", DoubleType()),
        StructField("support_call", IntegerType()),
        StructField("late_payment", IntegerType()),
    ]
)


@contextmanager
def spark_session():
    """Each Airflow task is its own process, so open and close Spark per call."""
    spark = (
        SparkSession.builder.appName("telco-etl")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        yield spark
    finally:
        spark.stop()


# 1) Bronze: unchanged copy of the original files
def extract_to_bronze():
    profile = SOURCE_DIR / "customer_profiles.csv"
    files = sorted(SOURCE_DIR.glob("activity_*.csv"))
    if not profile.exists() or not files:
        raise FileNotFoundError(f"Source files not found in {SOURCE_DIR}")

    (BRONZE_DIR / "activity").mkdir(parents=True, exist_ok=True)
    shutil.copyfile(profile, BRONZE_DIR / "customer_profiles.csv")
    for f in files:
        shutil.copyfile(f, BRONZE_DIR / "activity" / f.name)
    print(f"Bronze: copied profiles + {len(files)} activity files")


# 2) Silver: clean + group per customer per day
def transform_to_silver():
    with spark_session() as spark:
        customers = (
            spark.read.csv(
                str(BRONZE_DIR / "customer_profiles.csv"), header=True, schema=PROFILE_SCHEMA
            )
            .dropna(subset=["customer_id"])
            .dropDuplicates(["customer_id"])
            .withColumn("monthly_fee", F.col("monthly_fee").cast("decimal(10,2)"))
        )
        customers.write.mode("overwrite").parquet(str(SILVER_DIR / "customers"))

        activity = (
            spark.read.csv(
                str(BRONZE_DIR / "activity" / "activity_*.csv"), header=True, schema=ACTIVITY_SCHEMA
            )
            .dropna(subset=["event_id", "customer_id", "activity_date"])
            .dropDuplicates(["event_id"])
            .withColumn("activity_date", F.to_date("activity_date"))
            .withColumn("usage_minutes", F.coalesce("usage_minutes", F.lit(0.0)))
            .withColumn("data_gb", F.coalesce("data_gb", F.lit(0.0)))
            .filter((F.col("usage_minutes") >= 0) & (F.col("data_gb") >= 0))
            .withColumn("support_call", F.when(F.col("support_call") == 1, 1).otherwise(0))
            .withColumn("late_payment", F.when(F.col("late_payment") == 1, 1).otherwise(0))
            .join(customers.select("customer_id"), "customer_id", "left_semi")
            .groupBy("customer_id", "activity_date")
            .agg(
                F.count("*").cast("int").alias("events_count"),
                F.sum("usage_minutes").alias("usage_minutes"),
                F.sum("data_gb").alias("data_gb"),
                F.sum("support_call").cast("int").alias("support_calls"),
                F.sum("late_payment").cast("int").alias("late_payments"),
            )
        )
        activity.write.mode("overwrite").parquet(str(SILVER_DIR / "daily_activity"))
        print("Silver: customers + daily_activity saved as Parquet")


# 3) PostgreSQL: load Silver into the raw tables
def load_to_postgres():
    with spark_session() as spark:
        customers = spark.read.parquet(str(SILVER_DIR / "customers"))
        activity = spark.read.parquet(str(SILVER_DIR / "daily_activity"))

        # Empty the tables first so re-running never duplicates rows
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE raw.customers, raw.daily_activity;")

        customers.coalesce(4).write.jdbc(
            JDBC_URL, "raw.customers", mode="append", properties=JDBC_PROPS
        )
        activity.coalesce(4).write.jdbc(
            JDBC_URL, "raw.daily_activity", mode="append", properties=JDBC_PROPS
        )
        print("Postgres: loaded raw.customers and raw.daily_activity")


# 4) Gold (dbt): run models & data quality tests inside PostgreSQL
def run_dbt_gold():
    if not DBT_PROJECT_DIR.exists():
        raise FileNotFoundError(f"dbt project directory not found at {DBT_PROJECT_DIR}")

    # تحديد المسار الفعلي لبرنامج dbt المثبت في البيئة (مثل /home/airflow/.local/bin/dbt)
    dbt_bin = shutil.which("dbt", path="/home/airflow/.local/bin:/usr/local/bin:/usr/bin")
    if not dbt_bin:
        raise RuntimeError("dbt executable not found! Install dbt-postgres in your Dockerfile.")

    res_run = subprocess.run(
        [dbt_bin, "run", "--select", "gold"],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if res_run.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{res_run.stdout}\n{res_run.stderr}")
    print("dbt: Gold models built successfully")

    res_test = subprocess.run(
        [dbt_bin, "test", "--select", "gold"],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if res_test.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{res_test.stdout}\n{res_test.stderr}")
    print("dbt: All Gold tests passed")


# 5) Validate Gold: final check on PostgreSQL Gold table
def validate_gold():
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gold.customer_summary;")
        row_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM gold.customer_summary WHERE total_usage_minutes < 0;")
        invalid_rows = cur.fetchone()[0]

    if row_count == 0:
        raise ValueError("Validation Failed: gold.customer_summary is empty!")
    if invalid_rows > 0:
        raise ValueError(f"Validation Failed: Found {invalid_rows} rows with negative usage!")

    print(f"Validate Gold: Passed ({row_count} customer records verified)")


if __name__ == "__main__":
    extract_to_bronze()
    transform_to_silver()
    load_to_postgres()
    run_dbt_gold()
    validate_gold()
