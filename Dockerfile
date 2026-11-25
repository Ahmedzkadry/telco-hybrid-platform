FROM apache/airflow:3.0.6-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir pyspark==3.5.3 pandas psycopg2-binary python-dotenv

# dbt in its own venv so it does not conflict with Airflow
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres

RUN pip install --no-cache-dir dbt-core dbt-postgres

RUN pip install --no-cache-dir pyspark==3.5.3 pandas psycopg2-binary python-dotenv kafka-python
