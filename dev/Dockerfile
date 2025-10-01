ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

WORKDIR /opt/airflow

COPY requirements.txt .
RUN uv pip install -r requirements.txt
