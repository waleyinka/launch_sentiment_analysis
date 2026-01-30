# pyright: reportMissingImports=false

"""
LaunchSentiment Airflow DAG.

This DAG orchestrates an hourly snapshot workflow:
downloads a Wikimedia pageviews hourly dump
extracts the gzip
transforms and aggregates pageviews for selected companies
loads results into Postgres
Downstream analysis is done externally using SQL tools like pgcli or pgAdmin.

Design goals:
idempotence at each stage
safe retries for network and IO operations
clear task boundaries for observability and debugging
"""

from __future__ import annotations

# Import standard libraries
from datetime import datetime, timedelta
import os
import pendulum

# Import necessary Airflow modules
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from launch_sentiment.include.common.runtime import (
    task_fetch,
    task_extract,
    task_transform,
    task_load,
)


default_args = {
    "owner": "Data Consulting Firm",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="launch_sentiment",
    start_date=pendulum.datetime(2025, 12, 10, 16, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    tags=["capstone", "wikipedia", "sentiment"],
    default_args=default_args
) as dag:

    start = EmptyOperator(task_id="start")

    fetch = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=task_fetch,
        op_kwargs={"logical_date_str": "{{ logical_date }}"},
    )
    
    extract = PythonOperator(
       task_id="extract_pageviews",
       python_callable=task_extract,
       op_kwargs={"logical_date_str": "{{ logical_date }}"},
    )
    
    transform = PythonOperator(
        task_id="transform_pageviews",
        python_callable=task_transform,
        op_kwargs={"logical_date_str": "{{ logical_date }}"},
    )
    
    load = PythonOperator(
        task_id="load_pageviews",
        python_callable=task_load,
        op_kwargs={"logical_date_str": "{{ logical_date }}"},
    )
    
    end = EmptyOperator(task_id="end")

    start >> fetch >> extract >> transform >> load >> end

# EOF