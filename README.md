# 🎬 Spark ETL: Movie Analytics

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10+-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Metabase](https://img.shields.io/badge/Metabase-Latest-509EE3?style=flat-square&logo=metabase&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

> Production-grade, end-to-end data engineering pipeline processing **33.8 million MovieLens ratings** through a PySpark ETL workflow, orchestrated by Apache Airflow, persisted to a PostgreSQL star schema, and surfaced via Metabase dashboards — running weekly on a fully automated schedule with zero cloud cost.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Airflow Orchestration](#airflow-orchestration)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Pipeline Breakdown](#data-pipeline-breakdown)
- [Advanced Analytics](#advanced-analytics)
- [Setup & Installation](#setup--installation)
- [Usage](#usage)
- [Data Quality & Monitoring](#data-quality--monitoring)
- [Results & Insights](#results--insights)
- [Challenges & Solutions](#challenges--solutions)
- [Future Improvements](#future-improvements)
- [Author](#author)

---

## Overview

### The Problem

Raw movie rating data from GroupLens arrives as flat CSV files with no schema enforcement, duplicate records, Unix timestamps, and pipe-separated genre strings. Without a structured pipeline:

- Processing is manual, inconsistent, and error-prone
- Reprocessing unchanged data wastes 45–90 minutes per run
- Results are inaccessible to non-technical stakeholders
- There is no audit trail, retry logic, or failure isolation

### The Solution

A 15-task Airflow DAG that automates the full lifecycle: **download → detect changes → extract → validate → process → load → quality check → log → clean up**. The pipeline uses MD5 hash-based change detection to skip full reloads when the source dataset hasn't changed, cutting execution time from 90 minutes to 2 minutes on unchanged weeks.

### Why It Matters

| Metric | Value |
|--------|-------|
| Ratings processed | 33,832,162 |
| Movies (post-cleaning) | 79,476 |
| Years of coverage | 1995 – 2023 |
| Airflow tasks | 15 |
| Cloud cost | $0 |
| Pipeline schedule | Weekly (Sunday 02:00 UTC) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCE                                  │
│          GroupLens Research — ml-latest.zip (~334 MB)               │
│          http://files.grouplens.org/datasets/movielens/             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  HTTP Download (streaming)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW ORCHESTRATION                      │
│                   DAG: movie_analytics_pipeline                      │
│                   Schedule: 0 2 * * 0  (weekly)                     │
│  ┌──────────┐  ┌───────────────┐  ┌──────────────────────────────┐  │
│  │  Setup   │→ │  Acquisition  │→ │      Spark Processing        │  │
│  │          │  │               │  │  ingest → clean → transform  │  │
│  │ schema   │  │ download      │  │       → load                 │  │
│  │ creation │  │ hash check    │  └──────────────────────────────┘  │
│  └──────────┘  │ extract       │               │                     │
│                │ validate      │               ▼                     │
│                └───────────────┘  ┌──────────────────────────────┐  │
│                                   │  similarity + quality checks │  │
│                                   │  audit logging + cleanup     │  │
│                                   └──────────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  JDBC (PostgreSQL driver 42.7.1)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL 15 DATA WAREHOUSE                      │
│                         (Dockerized)                                 │
│                                                                      │
│   fact_ratings      dim_movies       dim_users                      │
│   33.8M rows        79,476 rows      unique users                   │
│                                                                      │
│   genre_trends      decade_stats     tag_similarity                 │
│   by year           by decade        cosine similarity              │
│                                                                      │
│   pipeline_run_log  (audit trail — append only)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Direct PostgreSQL connection
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         METABASE BI LAYER                            │
│                         (Dockerized)                                 │
│   Genre trends  ·  Decade analysis  ·  Rating distributions        │
│   Top-rated movies  ·  Similarity exploration                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Airflow Orchestration

### DAG: `movie_analytics_pipeline`

```
start
  │
  ▼
ensure_schema          ← CREATE TABLE IF NOT EXISTS (self-bootstrapping)
  │
  ▼
download_dataset       ← Stream ml-latest.zip from GroupLens (timeout: 30m)
  │
  ▼
check_for_updates      ← MD5 hash compare → AirflowSkipException if unchanged
  │                       (skips all downstream tasks, completes in ~2 min)
  ▼
extract_files          ← Unzip → flatten ml-latest/ subdirectory (timeout: 10m)
  │
  ▼
validate_data          ← File presence check + row count threshold (≥1M ratings)
  │
  ▼
┌─────────────────── TaskGroup: spark_jobs ──────────────────────────┐
│  ingest_data → clean_data → transform_data → load_to_postgres      │
│  (30m)          (30m)         (60m)            (2h)                │
└────────────────────────────────────────────────────────────────────┘
  │
  ▼
run_similarity_job     ← Cosine similarity on top 500 movies (timeout: 1h)
  │
  ▼
data_quality_checks    ← Post-load row count validation per table
  │
  ▼
log_pipeline_run       ← Insert to pipeline_run_log (trigger: all_success)
  │
  ▼
cleanup_temp_files     ← Delete ml-latest.zip (trigger: all_done)
  │
  ▼
end
```

### Task Groups

| Group | Tasks | Purpose |
|-------|-------|---------|
| **Setup** | `start`, `ensure_schema` | Self-bootstrapping schema creation |
| **Acquisition** | `download_dataset`, `check_for_updates`, `extract_files`, `validate_data` | Download, change detection, validation |
| **Processing** | `spark_jobs.*` (4 tasks) | PySpark ETL — ingest, clean, transform, load |
| **Analytics** | `run_similarity_job` | Cosine similarity model |
| **Monitoring** | `data_quality_checks`, `log_pipeline_run`, `cleanup_temp_files` | QA, audit, cleanup |

### Key Design Decisions

**Idempotency — Truncate-before-insert**
All pipeline output tables are truncated at the start of `load_to_postgres` before any data is written. Re-running the pipeline any number of times produces identical results. `pipeline_run_log` is the only append-only table.

**Incremental Update Detection — MD5 Hashing**
GroupLens publishes snapshot archives, not delta feeds. Row-level incremental loading is not feasible without comparing the entire dataset. The pipeline computes an MD5 hash of the downloaded zip and compares it against `.last_dataset_hash` from the previous run. If hashes match, `check_for_updates` raises `AirflowSkipException` — all downstream tasks are marked SKIPPED with green status.

**Retry Policy**
```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
}
```
Individual task timeouts are set per task to prevent runaway workers from blocking the queue.

---

## Tech Stack

| Technology | Version | Role | Justification |
|-----------|---------|------|---------------|
| **Python** | 3.11+ | Primary language | Ecosystem depth; PySpark compatibility |
| **PySpark** | 3.5.0 | Distributed processing | Handles 33M+ rows in partitions; scales to cluster without code changes |
| **Apache Airflow** | 2.10+ | Orchestration | DAG-based dependencies, retry logic, XCom, task grouping, web UI |
| **PostgreSQL** | 15 | Data warehouse | ACID compliance, JDBC support, Metabase-compatible |
| **Docker Compose** | v2 | Infrastructure | Reproducible environments; eliminates host dependencies |
| **Metabase** | Latest | BI / Dashboarding | No-code dashboard authoring; direct PostgreSQL connectivity |
| **psycopg2** | 2.9.9 | DB driver | Native PostgreSQL adapter for Python quality checks and audit logging |

> **Why PythonOperator over SparkSubmitOperator?** This pipeline runs Spark in `local[2]` mode within the Airflow container. PythonOperator provides unified logging and zero additional configuration. SparkSubmitOperator is the correct choice for cluster deployments — documented as a future improvement.

---

## Project Structure

```
movie_analytics/
│
├── dags/
│   ├── movie_pipeline_dag.py       # Main 15-task Airflow DAG
│   └── utils/
│       └── dataset_utils.py        # Download, hash detection, DB helpers, cleanup
│
├── jobs/
│   ├── spark_session.py            # SparkSession factory (local[2], JDBC config)
│   ├── ingest.py                   # CSV → Spark DataFrames with explicit schemas
│   ├── clean.py                    # Dedup, null handling, type conversion, filtering
│   ├── transform.py                # Window functions, genre trends, decade stats
│   ├── load.py                     # JDBC writes to PostgreSQL (year-by-year batching)
│   └── similarity.py               # Cosine similarity on genome tag vectors
│
├── data/
│   ├── raw/                        # Extracted CSVs (ratings, movies, tags, genome-*)
│   │   └── .last_dataset_hash      # MD5 of last successfully processed zip
│   └── processed/                  # Spark staging output (cleared each run)
│
├── sql/
│   └── schema.sql                  # Star schema DDL + pipeline_run_log table
│
├── docker-compose.yml              # PostgreSQL + Metabase + Airflow services
├── Dockerfile                      # Custom Airflow image (Java + PySpark 3.5.0)
├── requirements.txt                # Python dependencies
└── README.md
```

---

## Data Pipeline Breakdown

### 1. Ingestion

Reads six CSV files using PySpark's `DataFrameReader` with **explicit schemas** — schema inference on 33M rows is slow and produces type errors on nullable columns.

```
ratings.csv       → 33.8M rows  (userId, movieId, rating, timestamp)
movies.csv        → 86,537 rows (movieId, title, genres)
tags.csv          → 2.3M rows   (userId, movieId, tag, timestamp)
genome-scores.csv → 18.5M rows  (movieId, tagId, relevance)
genome-tags.csv   → 1,128 rows  (tagId, tag)
links.csv         → 86,537 rows (movieId, imdbId, tmdbId)
```

### 2. Cleaning

| Operation | Applied To | Method |
|-----------|-----------|--------|
| Null removal | All required fields | `dropna(subset=[...])` |
| Deduplication | `(userId, movieId)` pairs | `dropDuplicates()` — keeps latest rating |
| Timestamp conversion | `ratings.timestamp` | `from_unixtime()` → `to_date()` |
| Release year extraction | `movies.title` | `regexp_extract(r'\((\d{4})\)')` |
| Genre parsing | `movies.genres` | `split('\\|')` → `ArrayType(StringType)` |
| Domain filtering | `ratings.rating` | Keep 0.5 – 5.0 only |
| Tag noise removal | `tags.tag` | Drop length < 2, lowercase, trim |

### 3. Transformation

Window functions compute analytics over the cleaned ratings:

```python
# Cumulative rating count per movie
Window.partitionBy('movieId').orderBy('rating_date').rowsBetween(UNBOUNDED, CURRENT)

# 30-day rolling average
Window.partitionBy('movieId').orderBy(col('rating_date').cast('long')).rangeBetween(-30*86400, 0)
```

Genre trends are computed by exploding the genre array to one row per `(rating, genre)` pair, then aggregating by `(genre, rating_year)`.

### 4. Data Modeling — Star Schema

```
                    ┌──────────────┐
                    │  dim_movies  │
                    │  movie_id PK │
                    │  title       │
                    │  clean_title │
                    │  genres      │
                    └──────┬───────┘
                           │
┌────────────┐    ┌────────┴─────────┐    ┌───────────────┐
│  dim_users │    │   fact_ratings   │    │  genre_trends │
│  user_id PK│◄───│  user_id FK      │    │  genre        │
└────────────┘    │  movie_id FK     │    │  rating_year  │
                  │  rating          │    │  avg_rating   │
                  │  rating_date     │    └───────────────┘
                  │  rating_year     │
                  └──────────────────┘    ┌───────────────┐
                                          │  decade_stats │
                  ┌──────────────────┐    │  decade       │
                  │  tag_similarity  │    │  avg_rating   │
                  │  movie_id_1      │    └───────────────┘
                  │  movie_id_2      │
                  │  similarity_score│    ┌────────────────────┐
                  └──────────────────┘    │  pipeline_run_log  │
                                          │  run_id (audit)    │
                                          └────────────────────┘
```

### 5. Loading — PostgreSQL via JDBC

`fact_ratings` (33.8M rows) is written **year-by-year in 29 sequential batches** (1995–2023). Writing the full table in a single JDBC call exhausts the JVM on memory-constrained environments. Each annual partition is coalesced to 1 partition before writing.

```python
for year in years:
    mode = 'overwrite' if first_year else 'append'
    fact.filter(col('rating_year') == year).coalesce(1) \
        .write.format('jdbc').option('batchsize', 10000).mode(mode).save()
```

---

## Advanced Analytics

### Tag Genome Cosine Similarity

The genome dataset provides machine-computed relevance scores for **1,128 tags** across 18.5M `(movieId, tagId)` pairs. Each movie is represented as a sparse vector in 1,128-dimensional tag space.

**Why cosine similarity?** It measures angular distance between vectors — invariant to the magnitude of tag relevance scores, which vary across movies with different review counts.

**Implementation:**
1. Filter to top 500 movies by rating count
2. Represent each as a `SparseVector` over 1,128 tag dimensions
3. Compute pairwise cosine similarity across all 500 × 500 pairs
4. Write results to `tag_similarity` table

**Output:** A content-based recommendation foundation — movies that are semantically similar in tag space, independent of genre labels or user collaborative signals.

```sql
-- Example: find movies most similar to movieId 1
SELECT m.clean_title, ts.similarity_score
FROM tag_similarity ts
JOIN dim_movies m ON ts.movie_id_2 = m.movie_id
WHERE ts.movie_id_1 = 1
ORDER BY ts.similarity_score DESC
LIMIT 10;
```

---

## Setup & Installation

### Prerequisites

| Requirement | Version |
|-------------|---------|
| Docker Desktop | Latest (with WSL2 on Windows) |
| Docker Compose | v2+ |
| Git | Any |

> Java and Python are installed **inside the Docker containers** — no host installation required.

### Step 1 — Clone the repository

```bash
git clone https://github.com/gogoharrison/spark-etl-movie-analytics.git
cd spark-etl-movie-analytics
```

### Step 2 — Create the Airflow metadata database

```bash
docker compose up postgres -d
# Wait for healthy status, then:
docker exec -it movie_postgres psql -U movieuser -d moviedb -c "CREATE DATABASE airflowdb;"
```

### Step 3 — Build the custom Airflow image

The custom image installs Java (required for PySpark) and PySpark 3.5.0 on top of the base Airflow image.

```bash
docker compose build
# Takes 10–20 minutes on first build — layers are cached for subsequent runs
```

### Step 4 — Start all services

```bash
docker compose up -d
```

Verify all services are running:

```bash
docker compose ps
# Expected: postgres (healthy), metabase (running), 
#           airflow-webserver (running), airflow-scheduler (running)
```

### Step 5 — Create the Airflow admin user

```bash
docker exec -it movie_airflow_webserver airflow users create \
  --username admin \
  --password admin \
  --firstname YourName \
  --lastname YourSurname \
  --role Admin \
  --email admin@example.com
```

### Step 6 — Access the services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | Set up on first visit |
| PostgreSQL | localhost:5433 | movieuser / movie123 / moviedb |

---

## Usage

### Trigger the pipeline

**Via Airflow UI:**
1. Navigate to http://localhost:8080
2. Find `movie_analytics_pipeline` in the DAG list
3. Toggle the DAG **on** (blue switch)
4. Click **▶ Trigger DAG** to run immediately

**Via CLI:**
```bash
docker exec -it movie_airflow_scheduler \
  airflow dags trigger movie_analytics_pipeline
```

### Monitor execution

The Airflow Graph view shows real-time task status:

- 🟩 **success** — task completed
- 🟨 **running** — task executing
- 🟥 **failed** — task failed (check logs)
- ⬜ **skipped** — dataset unchanged (normal when no updates)

Click any task → **View Log** for full output including Spark row counts and JDBC write progress.

### Test individual tasks

```bash
# Test a specific task without triggering the full DAG
docker exec -it movie_airflow_scheduler \
  airflow tasks test movie_analytics_pipeline <task_id> 2024-01-01

# Examples:
airflow tasks test movie_analytics_pipeline download_dataset 2024-01-01
airflow tasks test movie_analytics_pipeline spark_jobs.clean_data 2024-01-01
```

### Query results directly

```bash
docker exec -it movie_postgres psql -U movieuser -d moviedb
```

```sql
-- Row counts across all tables
SELECT 'fact_ratings' AS tbl, COUNT(*) FROM fact_ratings
UNION ALL SELECT 'dim_movies', COUNT(*) FROM dim_movies
UNION ALL SELECT 'genre_trends', COUNT(*) FROM genre_trends
UNION ALL SELECT 'decade_stats', COUNT(*) FROM decade_stats
UNION ALL SELECT 'tag_similarity', COUNT(*) FROM tag_similarity;

-- Top 10 highest-rated movies (500+ ratings)
SELECT m.clean_title, ROUND(AVG(f.rating), 2) AS avg_rating, COUNT(*) AS total
FROM fact_ratings f JOIN dim_movies m ON f.movie_id = m.movie_id
GROUP BY m.clean_title HAVING COUNT(*) >= 500
ORDER BY avg_rating DESC LIMIT 10;

-- Pipeline run history
SELECT run_date, dataset_hash, status, rows_loaded, duration_secs
FROM pipeline_run_log ORDER BY run_date DESC LIMIT 5;
```

### Connect Metabase to PostgreSQL

On first launch at http://localhost:3000:
- Database type: **PostgreSQL**
- Host: **postgres** (Docker service name, not localhost)
- Port: **5432**
- Database: **moviedb**
- Username: **movieuser** / Password: **movie123**

---

## Data Quality & Monitoring

### Pre-load validation (`validate_data` task)

| Check | Threshold | Failure action |
|-------|-----------|---------------|
| Required files present | All 5 CSVs in `data/raw/` | `FileNotFoundError` → pipeline aborts |
| Ratings row count | ≥ 1,000,000 rows | `ValueError` → pipeline aborts |

### Post-load validation (`data_quality_checks` task)

Row counts for each output table are verified against expected thresholds after loading. Results are pushed via XCom to `log_pipeline_run` for inclusion in the audit log.

### Audit trail (`pipeline_run_log`)

```sql
CREATE TABLE pipeline_run_log (
    run_id        SERIAL PRIMARY KEY,
    run_date      TIMESTAMP DEFAULT NOW(),
    dataset_hash  VARCHAR(64),   -- MD5 of processed zip
    status        VARCHAR(20),   -- 'success' | 'failed' | 'skipped'
    rows_loaded   INTEGER,
    duration_secs INTEGER
);
```

This table is **append-only** — never truncated. It provides a permanent history of every pipeline execution for trend monitoring and incident investigation.

### Recovering from failures

```bash
# In Airflow UI: click the failed task → Clear → Re-run
# Or via CLI:
docker exec -it movie_airflow_scheduler \
  airflow tasks clear movie_analytics_pipeline -t <task_id> -s 2024-01-01
```

Because `load_to_postgres` truncates tables before writing, clearing and re-running the load task is always safe — no partial data survives.

---

## Results & Insights

| Analytical Output | Description |
|-------------------|-------------|
| **Rating volume by genre** | Drama and Comedy account for the highest rating counts by a significant margin |
| **Temporal rating trends** | Rating activity peaks in 2000–2005; 1990s films carry the highest average ratings |
| **Decade analysis** | 1990s films have the highest cumulative average rating across all decades |
| **Content similarity** | Genre and decade are the strongest clustering signals in tag-genome space |
| **Pipeline efficiency** | Hash detection reduces unchanged-week run time from ~90 min to ~2 min |

**Business value:** The pipeline delivers a weekly-refreshed analytical foundation for content recommendation, audience trend analysis, and genre performance tracking — without manual intervention.

---

## Challenges & Solutions

| Challenge | Root Cause | Solution |
|-----------|-----------|----------|
| JVM OOM during `fact_ratings` write | Writing 33M rows in single JDBC call exhausted container heap | Year-by-year batching in 29 sequential writes with `coalesce(1)` |
| WSL2 memory exhaustion on Windows | Docker limited to 3.7GB on 8GB host | `.wslconfig`: `memory=5GB`, `swap=4GB` |
| JDBC connection refused in container | `load.py` hardcoded `127.0.0.1:5433` (host port) | Switched to `postgres:5432` (Docker service name + internal port) |
| `flask_session` import error on init | `apache-airflow` incompatible with `flask-session ≥ 0.6` | Pinned `flask-session==0.5.0` |
| Windows ivy2 path in Docker | `spark_session.py` set ivy cache to `C:\Users\...` path | Changed to `/tmp/.ivy2` (Linux container path) |
| Silent task failures on large files | `pd.read_csv` on 33M rows caused OOM without traceback | Replaced with file line-counting; pandas avoided for large files |
| `SIGALRM` error on Airflow CLI | Unix-only signal used by Airflow's migration prompt | Containerised Airflow on Linux; Windows native install abandoned |

---

## Future Improvements

- **Cloud deployment** — Migrate to Kubernetes with `SparkSubmitOperator`, object storage (S3/GCS) for raw data, and a managed PostgreSQL instance. The pipeline logic requires no changes.
- **True incremental loading** — Replace full-reload strategy with timestamp-watermark incremental loading if GroupLens introduces a delta feed endpoint.
- **dbt transformation layer** — Replace PySpark transform jobs with dbt models for SQL-based transformations with schema tests, documentation, and lineage tracking.
- **Great Expectations** — Add declarative data quality contracts with automated alerting on threshold violations, replacing the current custom check implementation.
- **Collaborative filtering** — Extend the similarity model to incorporate user-based collaborative filtering using the full 33M-row ratings matrix.
- **Real-time streaming** — Add a Kafka ingestion layer for live rating event processing alongside the batch pipeline.
- **Prometheus + Grafana** — Replace the `pipeline_run_log` table with real-time pipeline metrics dashboards.

---

## Author

**Gogo Harrison**  
Data Engineer | Data Scientist | ML Engineer

Specialising in e-commerce analytics, customer behaviour modelling, and production-grade data pipeline development.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat-square&logo=linkedin)](https://linkedin.com/in/gogo-harrison)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat-square&logo=github)](https://github.com/gogoharrison)

---

<div align="center">

**Built with Python · PySpark · Apache Airflow · PostgreSQL · Docker · Metabase**

*Production-grade data engineering — zero cloud cost.*

</div>
