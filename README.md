# LaunchSentiment
***Wikipedia Pageviews–Driven Sentiment Pipeline with Apache Airflow***

---

## Table of Contents

 - Project Overview
 - Problem Statement
 - Data Source
 - Key Design Decisions
 - Pipeline Architecture
 - Airflow Time & Scheduling Model
 - Task Breakdown
 - Database Schema
 - Project Structure
 - Environment Setup
 - How to Run the Project
 - Testing
 - Validation & Analysis
 - Failure Handling & Idempotence
 - Scaling & Backfills
 - Key Learnings
 - Future Improvements

---

## Project Overview

LaunchSentiment is an Apache Airflow–orchestrated data pipeline that ingests hourly Wikipedia pageviews data, processes it to extract view counts for selected companies, and loads the results into a PostgreSQL database for analysis.

The project is designed as a production-style capstone, emphasizing:

 - correct time semantics
 - idempotent task design
 - deterministic outputs
 - safe retries and backfills
 - clean separation of orchestration and business logic

---

## Problem Statement

Public attention is often correlated with sentiment.
This project explores a simple hypothesis:

 An increase in a company’s Wikipedia pageviews may indicate increased public interest and potentially positive sentiment.

To validate this hypothesis at a small scale, the pipeline tracks hourly pageviews for the following companies:

 - Amazon
 - Apple
 - Facebook
 - Google
 - Microsoft

---

## Data Source

### Wikimedia Pageviews Dumps

Wikimedia publishes hourly pageviews data in gzip format.

 - Base URL: https://dumps.wikimedia.org/other/pageviews/
 - Example hourly file:
   ```bash
   pageviews-20251210-170000.gz
   ```

### Important Naming Convention

Wikimedia names files using the **end of the hour**.

Example:

 - `pageviews-20251210-170000.gz`
 - Represents pageviews from **16:00–17:00 UTC**

This convention directly informs the Airflow time design.

---

## Key Design Decisions

 - **Airflow data interval–based time handling**
   All paths, URLs, and timestamps are derived from `data_interval_end`.

 - **Idempotent tasks**
   Every task can be safely retried or rerun without corrupting state.

 - **Deterministic artifacts**
   Outputs are stable across runs to prevent flaky behavior.

 - **DDL baked into load step**
   The pipeline owns its storage contract and can run on a fresh database.

--

## Pipeline Architecture

High-level flow:
```bash
fetch_pageviews
        ↓
extract_pageviews
        ↓
transform_pageviews
        ↓
load_pageviews
```

Each Airflow run processes **exactly one hour** of data.

---

## Airflow Time & Scheduling Model

 - **Schedule**: `@hourly`
 - **Timezone**: UTC
 - **Anchor timestamp**: `data_interval_end`

Why `data_interval_end`?

 - Airflow runs represent time intervals
 - Wikimedia filenames use hour-end timestamps
 - This creates a clean one-to-one mapping with no offsets or hacks

---

## Database Schema

Target table: `pageviews_hourly`
```bash
CREATE TABLE IF NOT EXISTS pageviews_hourly (
    company_name TEXT NOT NULL,
    pageviews INTEGER NOT NULL,
    hour_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT pageviews_hourly_uk
        UNIQUE (company_name, hour_timestamp)
);
```

## Project Structure
```bash
launchsentiment/
│
├── include/
│   ├── common/
│   │   ├── config.py
│   │   └── logger_config.py
│   │
│   ├── sql/
│   │   └── analysis.sql
│   │
│   ├── tests/
│   │   └── test_transform_data.py
│   │
│   └── utils/
│       ├── fetch_pageviews.py
│       ├── extract_pageviews.py
│       ├── transform_pageviews.py
│       └── load_pageviews.py
│
├── data/
│   ├── raw/
│   ├── extracted/
│   └── transformed/
│
├── images/
│
├── launch_sentiment_dag.py
├── requirements.txt
└── README.md
```

---

## Environment Setup

### Prerequisites

 - Python 3.10+
 - Apache Airflow
 - PostgreSQL
 - Git

### Environment Variables

Create a `.env` file:
```bash
PG_HOST=host
PG_PORT=port
PG_DB=db_name
PG_USER=user
PG_PASSWORD=password
LOG_LEVEL=INFO
```
Environment variables are loaded via `config.py`.

---

## How to Run the Project

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Airflow
```bash
airflow standalone
```

### 3. Place DAG

Ensure `launch_sentiment.py` is in your Airflow DAGs folder.

### 4. Trigger DAG

Trigger `launch_sentiment` from the Airflow UI.

Each run processes **one hour of Wikimedia data** based on `data_interval_end`.

---

## Validation & Analysis

After a successful run, identify the company with the highest pageviews for a given hour:
```sql
SELECT company_name, pageviews
FROM pageviews_hourly
ORDER BY pageviews DESC
LIMIT 1;
```

---

## Failure Handling & Idempotence

 - Fetch and extract use atomic file writes
 - Transform overwrites deterministic CSVs
 - Load runs inside a transaction
 - Delete + insert guarantees hour-level correctness
 - Safe retries and backfills are supported

---

## Scaling & Backfills

 - Enable catchup=True to process historical hours
 - Each hour produces isolated artifacts
 - Database partitions naturally by hour_timestamp
 - Indexing can be added for scale

---

## Key Learnings

 - Airflow is orchestration, not transformation logic
 - Idempotence is the foundation of reliable pipelines
 - Deterministic outputs prevent flaky behavior
 - Clear contracts between tasks simplify reasoning]

---

## Future Improvements

 - Company title normalization mapping
 - Data retention and cleanup policies
 - Indexes on `hour_timestamp`
 - Metrics and alerts
 - Visualization layer on top of the data