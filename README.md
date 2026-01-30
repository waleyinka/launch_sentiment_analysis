# LaunchSentiment

***A Wikipedia Pageviews–Driven Sentiment Pipeline Orchestrated with Apache Airflow***

## Table of Contents

 - [Overview](#overview)
 - [Problem Context](#hypothesis)
 - [High-Level Architecture](#high-level-architecture)
 - [Data Source](#data-source)
 - [DAG Design ](#key-design-decisions)
    - [Tasks Breakdown](#tasks-breakdown)
 - [Idempotence and Reliability](#idempotence-and-reliability)
 - [Observability](#observability)
 - [Analysis](#analysis)
 - [Key Engineering Trade-offs](#)
 - [Future Improvements]
 - [What This Project Demonstrates]
 - [Final Reflection]


## Overview

LaunchSentiment is an Apache Airflow–orchestrated data pipeline designed to explore whether Wikipedia pageview activity can act as a lightweight proxy for market sentiment for companies, using Apple, Facebook, Google, Microsoft, and Amazon as case study.

The core hypothesis is intentionally simple:
***An increase in Wikipedia pageviews for a company may indicate rising public interest and, by extension, positive sentiment that could correlate with stock price movement.***

This project focuses on building a reliable, reproducible, and observable data pipeline rather than predictive accuracy. It demonstrates how raw, large-scale public data can be ingested, transformed, and queried using production-oriented data engineering practices.


## Problem Context

The Wikimedia Foundation publishes hourly pageview dumps dating back to 2015. Each hourly file is approximately 50 MB compressed and over 200 MB uncompressed, making it unsuitable for naive ingestion or manual analysis.

As a first milestone, the pipeline targets a single hourly snapshot in December 2025 and extracts pageview counts for five companies:

 - Amazon
 - Apple
 - Facebook
 - Google
 - Microsoft
 - Tesla
 - SpaceX

The objective is to answer a simple analytical question:

| For a given hour, which of these companies had the highest Wikipedia pageviews?


## High-Level Architecture

![Archiecture](/dags/launch_sentiment/images/architecture.png)

The pipeline follows a classic batch ingestion pattern orchestrated with Airflow:

 1. Download
 2. Extract
 3. Transform
 4. Load
 5. Analyze

Each step is isolated into a dedicated Airflow task, allowing ***retries***, ***observability***, and ***controlled failure handling***.

At a high level:

 - Apache Airflow orchestrates execution, retries, scheduling, and dependencies
 - Python handles extraction and transformation logic
 - PostgreSQL serves as the analytical storage layer
 - SQL is used for downstream analysis

This separation ensures that the pipeline remains debuggable, testable, and easy to extend.



## Data Source

 - Wikimedia hourly pageviews dumps
 - URL pattern:
    `https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/`

Each file contains space-delimited records with:

 - project code
 - page title
 - view count
 - response size


## DAG Design

The DAG is designed to be time-aware and idempotent.

### Tasks Breakdown

 **1. Fetch Pageviews File**
 Downloads the compressed hourly dump for the selected execution timestamp.
 The task validates file availability before proceeding, reducing downstream failures due to upstream latency.

 **2. Extract Data**
 Unzips the gzip file locally and streams the content rather than loading it fully into memory.

 **3. Transform Pageviews**
 Filters records by:

    - English Wikipedia project
    - Page titles matching the five target companies

 Transforms raw rows into a structured format suitable for relational storage.

 Basic validation checks are applied, including:

    - non-null page titles
    - non-negative view counts

 **4. Load to Database**
 Loads transformed data into PostgreSQL using an idempotent write strategy to prevent duplicate inserts on retries.

 **5. Analyze**
 Executes a SQL query to identify the company with the highest pageviews for the selected hour.

![DAG Run](/dags/launch_sentiment/images/pageviews-20251210-150000.png)


## Idempotence and Reliability

Several design decisions were made to ensure production-style robustness:

 - **Idempotent loads**
 Records are keyed by execution hour and page name, allowing safe re-runs.
 
 - **Retries and failure handling**
 Each task is configured with retries and retry delays appropriate for network-bound workloads.
 
 - **Explicit task boundaries**
 Download, extract, transform, and load are separated to avoid opaque failure states.
 
 - **Deterministic execution**
 The DAG processes a fixed execution timestamp, making backfills and replays predictable.


## Observability

Airflow provides:

 - task-level logs
 - execution timelines
 - retry visibility

Each task logs row counts and key milestones, making it easy to trace data volume changes across stages.


## Project Setup

### Prerequisites

Before running the pipeline, ensure you have the following installed:

 - Python 3.9 or later
 - Docker and Docker Compose
 - PostgreSQL client tools
 - Apache Airflow (local or containerized setup)

Basic familiarity with Airflow concepts such as DAGs, tasks, and execution dates is assumed.

### Installation

Clone the repository:

```bash
git clone https://github.com/your-username/launchsentiment.git
cd launchsentiment
```

Create and activate a Python virtual environment:

```bash
python -m venv venv
source venv/bin/activate
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

If using Docker Compose, build and start the services:

```bash
docker compose up -d
```

This will start Airflow components and the PostgreSQL database.


### Environment Variables

The pipeline relies on environment variables for configuration. These are injected via Airflow connections or a `.env` file.

Required variables include:

 `POSTGRES_HOST`
 `POSTGRES_PORT`
 `POSTGRES_DB`
 `POSTGRES_USER`
 `POSTGRES_PASSWORD`

Optional variables may include:

 `PAGEVIEWS_BASE_URL`
 `LOCAL_DATA_DIR`

These variables allow the pipeline to be environment-agnostic and easily portable between local and containerized setups.


## How to Run the Pipeline

Once Airflow is running, access the Airflow UI:

 `http://localhost:8080`

Enable the `launchsentiment_pageviews` DAG.

Trigger the DAG manually or allow it to run for the configured execution date. The DAG processes a single hourly snapshot based on the execution timestamp.

Upon successful completion, the transformed data will be available in PostgreSQL and ready for analysis.


## Analysis

Once the pipeline completes successfully, the data is available in PostgreSQL and can be queried using standard SQL tools such as pgcli or pgAdmin.

```sql
SELECT
    page_name,
    SUM(view_count) AS total_views
FROM wikipedia_pageviews
WHERE execution_hour = '2025-12-10 16:00:00'
GROUP BY page_name
ORDER BY total_views DESC
LIMIT 1;
```
The query returns the company page with the highest Wikipedia traffic for that hour.

![query](/dags/launch_sentiment/images/top_company.png)


## Future Improvements

 - Extend to multi-hour and daily aggregations
 - Add backfill support across date ranges
 - Persist raw dumps to object storage
 - Introduce dbt for transformations and modeling
 - Enhance validation and data quality checks
 - Integrate alerting via Slack or email


## Reflection

This project treats Airflow as a system for managing uncertainty rather than just a scheduler. The emphasis is on explicit task boundaries, safe retries, and reproducible execution.

While the sentiment hypothesis is intentionally naive, the pipeline structure is designed to scale in both complexity and volume, making it a strong foundation for more advanced analytical or predictive workflows.