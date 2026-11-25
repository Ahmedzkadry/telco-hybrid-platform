import hashlib
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

from src import etl


@dag(
    dag_id="telco_data_pipeline",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    params={"demo_retry": False},
    tags=["workshop", "data-engineering", "pyspark", "dbt"],
)
def data_pipeline():

    @task
    def extract_to_bronze():
        context = get_current_context()
        if context["params"]["demo_retry"]:
            # Fail ONLY the first attempt of this DAG run to teach retries.
            run_id = context["ti"].run_id
            marker_id = hashlib.sha256(run_id.encode()).hexdigest()[:12]
            marker = etl.ROOT / "data" / "demo" / f"retry_{marker_id}.txt"
            marker.parent.mkdir(parents=True, exist_ok=True)
            if not marker.exists():
                marker.write_text("first attempt failed", encoding="utf-8")
                raise RuntimeError("Demo failure: Airflow will retry in 10 seconds")
        etl.extract_to_bronze()

    @task
    def transform_to_silver():
        etl.transform_to_silver()

    @task
    def load_to_postgres():
        etl.load_to_postgres()

    @task
    def run_dbt_gold():
        etl.run_dbt_gold()

    @task
    def validate_gold():
        etl.validate_gold()

    (
        extract_to_bronze()
        >> transform_to_silver()
        >> load_to_postgres()
        >> run_dbt_gold()
        >> validate_gold()
    )


data_pipeline()
