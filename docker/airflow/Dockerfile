FROM apache/airflow:3.4.0-python3.12

# Ingestion deps, installed against Airflow's pinned version so nothing gets downgraded.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# dbt lives in its own virtualenv to avoid dependency clashes with Airflow.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.11.0" "dbt-postgres~=1.11.0" \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt
USER airflow
