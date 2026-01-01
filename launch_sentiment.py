# pyright: reportMissingImports=false

"""
This DAG orchestrates the ingestion of one hour of Wikipedia pageviews, filters it to five companies (Amazon, Apple,
Facebook, Google, & Microsoft), persists the result, and enables a downstream analytical query.

Target timestamp: 4pm - 5pm data for 10th of December, 2025
Corresponding Wikimedia URL: https://dumps.wikimedia.org/other/pageviews/2025/2025-12/pageviews-20251210-170000.gz
Successful run returns: a cleaned/analysed data showing which of the filtered company had the highest views


"""

# Import standard libraries
import os
from pendulum import datetime

# Import necessary Airflow modules
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Import python functions from src/ folder
from launch_sentiment.include.utils.fetch_pageviews import fetch_data
from launch_sentiment.include.utils.extract_pageviews import extract_data
from launch_sentiment.include.utils.transform_pageviews import transform_data
from launch_sentiment.include.utils.load_pageviews import load_data

from launch_sentiment.include.common.config import WIKIMEDIA_BASE_URL

# -------------------------------
# Constants
# -------------------------------

data_dir = os.environ.get("data_dir", "/opt/airflow/dags/launch_sentiment/data")

target_companies = {
    "Amazon", 
    "Apple", 
    "Facebook", 
    "Google", 
    "Microsoft"
}


# -------------------------------
# Airflow time templates
# -------------------------------

year = "{{ data_interval_end.strftime('%Y') }}"
year_month = "{{ data_interval_end.strftime('%Y-%m') }}"
hour_end = "{{ data_interval_end.strftime('%Y%m%d-%H0000') }}"
hour_timestamp = "{{ data_interval_end.isoformat() }}"


# -------------------------------
# Derived paths
# -------------------------------

wikimedia_url = f"{WIKIMEDIA_BASE_URL}/{year}/{year_month}/pageviews-{hour_end}.gz"

raw_gzip_path = f"{data_dir}/raw/pageviews-{hour_end}.gz"
extracted_text_path = f"{data_dir}/extracted/pageviews-{hour_end}.txt"
transformed_csv_path = f"{data_dir}/transformed/pageviews-{hour_end}.csv"


# -------------------------------
# DAG definition
# -------------------------------

default_args = {
    "owner": "Data Consulting Firm",
    "retries": 3,
}

with DAG(
    dag_id="launch_sentiment",
    start_date=datetime(2025, 12, 10, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    tags=["capstone", "wikipedia", "sentiment"],
    default_args=default_args
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=fetch_data,
        op_kwargs={
            "url": wikimedia_url,
            "output_path": raw_gzip_path,
        },
    )
    
    extract_pageviews = PythonOperator(
       task_id="extract_pageviews",
       python_callable=extract_data,
       op_kwargs={
           "input_gzip_path": raw_gzip_path,
           "output_text_path": extracted_text_path,
       },
       do_xcom_push=False,
    )
    
    transform_pageviews = PythonOperator(
        task_id="transform_pageviews",
        python_callable=transform_data,
        op_kwargs={
            "input_file_path": extracted_text_path,
            "target_hour_timestamp": hour_timestamp,
            "target_companies": target_companies,
            "output_file_path": transformed_csv_path
        },
    )
    
    load_pageviews = PythonOperator(
        task_id="load_pageviews",
        python_callable=load_data,
        op_kwargs={
            "csv_file_path": transformed_csv_path,
            "target_hour_timestamp": hour_timestamp,
            "table_name": "pageviews_hourly",
        }
    )
    
    end = EmptyOperator(task_id="end")

    start >> fetch_pageviews >> extract_pageviews >> transform_pageviews >> load_pageviews >> end

# EOF