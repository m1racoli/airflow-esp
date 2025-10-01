from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


@task.stub
def wait_button():
    pass


@task.stub
def set_led():
    pass


@dag(
    schedule=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["edge", "airflow-summit", "rust"],
)
def airflow_summit_demo():
    wait_button() >> set_led()


airflow_summit_demo()
