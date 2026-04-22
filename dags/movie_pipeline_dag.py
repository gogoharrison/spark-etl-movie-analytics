# dags/movie_pipeline_dag.py
#
# MovieLens Analytics Pipeline — Production-grade Airflow DAG
# Local machine edition (LocalExecutor, single Postgres instance)
#
# Key design decisions:
#   1. clean() persists Parquet to a processed layer so transform()
#      and load() read from disk — no triple re-cleaning of raw CSVs.
#   2. BranchPythonOperator replaces AirflowSkipException so the
#      skip path is explicit and auditable.
#   3. truncate_tables() is its own task before Spark starts,
#      making the failure boundary clear.
#   4. transform() writes its outputs to Parquet; load() reads them —
#      no silent DataFrame discard.
#   5. task_log_run() handles BOTH the success and the skip branch
#      so every scheduled run is recorded in pipeline_run_log.
#   6. All task IDs used in xcom_pull are extracted to constants so
#      a rename never silently breaks auditing.
#   7. Secrets come from env-vars; no plaintext passwords in code.

import os
import sys
import time
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ── Python-path setup ────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'jobs'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'dags', 'utils'))

# ── Paths ────────────────────────────────────────────────────────────────────
RAW_DIR       = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROCESSED_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed')
ZIP_PATH      = os.path.join(RAW_DIR, 'ml-latest.zip')

# Parquet staging paths — written by clean, read by transform + load
CLEAN_R_PATH  = os.path.join(PROCESSED_DIR, 'clean_ratings.parquet')
CLEAN_M_PATH  = os.path.join(PROCESSED_DIR, 'clean_movies.parquet')

# Analytics Parquet paths — written by transform, read by load
GENRE_TRENDS_PATH  = os.path.join(PROCESSED_DIR, 'genre_trends.parquet')
DECADE_STATS_PATH  = os.path.join(PROCESSED_DIR, 'decade_stats.parquet')
MOVIE_STATS_PATH   = os.path.join(PROCESSED_DIR, 'movie_stats.parquet')

# ── XCom task-ID constants (single source of truth) ─────────────────────────
TASK_VALIDATE    = 'validate_data'
TASK_LOAD        = 'spark_jobs.load_to_postgres'
TASK_QC          = 'data_quality_checks'

# ── Branch destination task IDs ───────────────────────────────────────────────
BRANCH_RUN_PIPELINE = 'download_dataset'
BRANCH_SKIP         = 'log_skipped_run'

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    'owner':                     'harrison',
    'depends_on_past':           False,
    'retries':                   2,
    'retry_delay':               timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'email_on_failure':          False,
    'email_on_retry':            False,
}


# ════════════════════════════════════════════════════════════════════════════
# BRANCH
# ════════════════════════════════════════════════════════════════════════════

def branch_check_updates(**context):
    """
    BranchPythonOperator: decides whether to run the full pipeline
    or skip directly to audit logging.

    Using BranchPythonOperator (not AirflowSkipException) means the
    DAG graph shows two explicit paths — easier to debug in the UI and
    ensures the skipped run is still logged.
    """
    from dataset_utils import should_download_dataset
    if should_download_dataset():
        print('Remote dataset changed or local zip missing — running full pipeline.')
        return BRANCH_RUN_PIPELINE
    print('Remote dataset unchanged — skipping pipeline, logging skip.')
    return BRANCH_SKIP


# ════════════════════════════════════════════════════════════════════════════
# SETUP TASKS
# ════════════════════════════════════════════════════════════════════════════

def task_ensure_schema(**context):
    from dataset_utils import ensure_schema
    ensure_schema()


def task_download(**context):
    from dataset_utils import download_dataset
    os.makedirs(RAW_DIR, exist_ok=True)
    download_dataset()


def task_extract(**context):
    print(f'Extracting {ZIP_PATH} → {RAW_DIR}')
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        z.extractall(RAW_DIR)

    # Flatten the nested ml-latest/ directory
    extracted_dir = os.path.join(RAW_DIR, 'ml-latest')
    if os.path.exists(extracted_dir):
        for fname in os.listdir(extracted_dir):
            os.replace(
                os.path.join(extracted_dir, fname),
                os.path.join(RAW_DIR, fname),
            )
        os.rmdir(extracted_dir)
    print('Extraction complete.')


def task_validate(**context):
    """
    Lightweight pre-Spark validation:
      - Confirms all required CSV files are present.
      - File size sanity check to detect corruption (fast, no full row scan).
      - Estimates row count from file size for downstream QC threshold.
    Pushes ratings_count to XCom for downstream quality checks.
    """
    required = [
        'ratings.csv', 'movies.csv', 'tags.csv',
        'genome-scores.csv', 'genome-tags.csv',
    ]
    missing = [f for f in required if not os.path.exists(os.path.join(RAW_DIR, f))]
    if missing:
        raise FileNotFoundError(f'Missing required files after extraction: {missing}')

    # Fast size check for corruption detection — avoids scanning 33M rows
    file_path = os.path.join(RAW_DIR, 'ratings.csv')
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f'ratings.csv size: {size_mb:.1f} MB')
    if size_mb < 100:
        raise ValueError(f'ratings.csv too small ({size_mb:.1f} MB) — possible corruption')

    # Estimate row count from file size for downstream QC drift detection
    # MovieLens latest: ~900 MB ≈ 33M rows, ~27 bytes per row average
    estimated_rows = int((size_mb * 1024 * 1024) / 27)
    print(f'Validation passed — estimated ratings rows: {estimated_rows:,}')

    context['ti'].xcom_push(key='ratings_count', value=estimated_rows)

# ════════════════════════════════════════════════════════════════════════════
# PRE-LOAD: TRUNCATE (separate from Spark, clear failure boundary)
# ════════════════════════════════════════════════════════════════════════════

def task_truncate_tables(**context):
    """
    Truncate all output tables BEFORE any Spark job starts.

    Keeping this as its own task (not inside task_load) means:
      - If truncate fails, Spark never runs and the DB still has the
        previous run's data intact.
      - If Spark fails mid-load after truncate, the DAG retries cleanly
        from here, re-truncating before another Spark attempt.
    """
    from dataset_utils import truncate_tables
    truncate_tables()


# ════════════════════════════════════════════════════════════════════════════
# SPARK JOBS
# ════════════════════════════════════════════════════════════════════════════

def task_ingest(**context):
    """
    Validates raw CSV schemas with Spark (column types, null counts).
    Acts as a schema-enforcement gate before cleaning begins.
    """
    from spark_session import get_spark
    from ingest import validate
    spark = get_spark('Airflow-Ingest')
    try:
        validate(spark)
    finally:
        spark.stop()


def task_clean(**context):
    """
    Cleans ratings, movies, and tags DataFrames, then writes them to
    Parquet in the processed layer.

    Writing to Parquet here means:
      - transform() and load() read from disk — no triple re-cleaning.
      - Each stage has a clear input/output contract.
      - Parquet files act as a lightweight checkpoint; if load() fails
        you don't need to re-clean to retry.
    """
    from spark_session import get_spark
    from clean import run_cleaning

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    spark = get_spark('Airflow-Clean')
    try:
        clean_r, clean_m, *_ = run_cleaning(spark)
        # Persist to processed layer — downstream tasks read from here
        clean_r.write.mode('overwrite').parquet(CLEAN_R_PATH)
        clean_m.write.mode('overwrite').parquet(CLEAN_M_PATH)
        print(f'Clean data persisted → {PROCESSED_DIR}')
    finally:
        spark.stop()


def task_transform(**context):
    """
    Reads clean Parquet, runs all three analytical computations, and
    writes each result to its own Parquet file.

    Persisting outputs means load() can read them cheaply without
    re-running any window functions or aggregations.
    """
    from spark_session import get_spark
    from transform import compute_movie_stats, compute_genre_trends, compute_decade_stats

    spark = get_spark('Airflow-Transform')
    try:
        clean_r = spark.read.parquet(CLEAN_R_PATH)
        clean_m = spark.read.parquet(CLEAN_M_PATH)

        movie_stats  = compute_movie_stats(clean_r, clean_m)
        genre_trends = compute_genre_trends(clean_r, clean_m)
        decade_stats = compute_decade_stats(clean_m, clean_r)

        movie_stats.write.mode('overwrite').parquet(MOVIE_STATS_PATH)
        genre_trends.write.mode('overwrite').parquet(GENRE_TRENDS_PATH)
        decade_stats.write.mode('overwrite').parquet(DECADE_STATS_PATH)

        print('All transformation outputs persisted to processed layer.')
    finally:
        spark.stop()


def task_load(**context):
    """
    Reads clean + transformed Parquet files and writes to Postgres.

    Deliberately does NOT call run_cleaning() again — clean data already
    lives in the processed layer.  This avoids re-reading and re-parsing
    hundreds of millions of raw CSV rows a second time.

    Pushes load_duration_secs to XCom for the audit log.
    """
    from spark_session import get_spark
    from load import load_dimensions, load_fact_ratings, load_analytics_from_parquet

    start = time.time()
    spark = get_spark('Airflow-Load')
    try:
        clean_r = spark.read.parquet(CLEAN_R_PATH)
        clean_m = spark.read.parquet(CLEAN_M_PATH)

        load_dimensions(clean_m, clean_r)
        load_fact_ratings(clean_r, clean_m)          # ← pass clean_m here
        load_analytics_from_parquet(spark, GENRE_TRENDS_PATH, DECADE_STATS_PATH)

    finally:
        spark.stop()

    duration = int(time.time() - start)
    context['ti'].xcom_push(key='load_duration_secs', value=duration)
    print(f'Load complete in {duration}s')


def task_similarity(**context):
    """
    Computes pairwise cosine similarity for the top-500 most-rated
    movies using genome tag vectors.  Reads top_movies from Postgres
    (populated by task_load) and writes results back.
    """
    from spark_session import get_spark
    from similarity import compute_tag_similarity
    from load import write_table

    spark = get_spark('Airflow-Similarity')
    try:
        sim_df = compute_tag_similarity(spark, top_n_movies=500)
        write_table(sim_df, 'tag_similarity')
    finally:
        spark.stop()


# ════════════════════════════════════════════════════════════════════════════
# QUALITY, AUDIT & CLEANUP
# ════════════════════════════════════════════════════════════════════════════

def task_quality_checks(**context):
    from dataset_utils import run_data_quality_checks
    ti = context['ti']
    ratings_count = ti.xcom_pull(task_ids=TASK_VALIDATE, key='ratings_count') or None
    table_stats   = run_data_quality_checks(expected_ratings=ratings_count)
    ti.xcom_push(key='table_stats', value=table_stats)


def task_log_run(**context):
    """
    Audit log for a SUCCESSFUL full pipeline run.
    Pulls ratings_count, load duration, and table stats from XCom.
    """
    from dataset_utils import save_current_hash, log_pipeline_run, record_table_stats
    ti = context['ti']

    ratings_count = ti.xcom_pull(task_ids=TASK_VALIDATE, key='ratings_count') or 0
    load_duration = ti.xcom_pull(task_ids=TASK_LOAD,     key='load_duration_secs') or 0
    table_stats   = ti.xcom_pull(task_ids=TASK_QC,       key='table_stats') or {}

    dataset_hash = save_current_hash()
    run_id = log_pipeline_run(
        dataset_hash=dataset_hash,
        status='success',
        rows_loaded=ratings_count,
        duration_secs=load_duration,
    )
    record_table_stats(run_id, table_stats)
    print(f'Pipeline run logged — run_id={run_id}')


def task_log_skipped_run(**context):
    """
    Audit log for a SKIPPED run (dataset unchanged).
    Ensures every scheduled execution has a record in pipeline_run_log,
    making gaps or double-runs easy to spot.
    """
    from dataset_utils import log_pipeline_run, save_current_hash
    dataset_hash = save_current_hash()
    run_id = log_pipeline_run(
        dataset_hash=dataset_hash,
        status='skipped',
        rows_loaded=0,
        duration_secs=0,
    )
    print(f'Skipped run logged — run_id={run_id}')


def task_cleanup(**context):
    """
    Deletes the raw zip file if ENABLE_CLEANUP=1.
    Uses trigger_rule='all_done' so it always executes regardless of
    upstream success or failure — no temp files left behind.
    """
    from dataset_utils import cleanup_zip
    cleanup_zip()
    print('Cleanup complete.')


# ════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id='movie_analytics_pipeline',
    default_args=default_args,
    description='End-to-end MovieLens ETL — download, clean, transform, load, similarity',
    schedule_interval='0 2 * * 0',   # Every Sunday at 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['movie', 'etl', 'spark', 'local'],
) as dag:

    # ── Bookend operators ────────────────────────────────────────────────────
    start = EmptyOperator(task_id='start')

    # none_failed_min_one_success: end fires whether the pipeline ran
    # fully or was skipped — as long as nothing actually errored.
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
    )

    # ── Schema + branch ──────────────────────────────────────────────────────
    ensure_schema_task = PythonOperator(
        task_id='ensure_schema',
        python_callable=task_ensure_schema,
    )

    check_updates = BranchPythonOperator(
        task_id='check_remote_updates',
        python_callable=branch_check_updates,
    )

    # ── Skip path ────────────────────────────────────────────────────────────
    log_skipped = PythonOperator(
        task_id=BRANCH_SKIP,
        python_callable=task_log_skipped_run,
    )

    # ── Full pipeline path ───────────────────────────────────────────────────
    download = PythonOperator(
        task_id='download_dataset',
        python_callable=task_download,
        execution_timeout=timedelta(minutes=30),
    )

    extract = PythonOperator(
        task_id='extract_files',
        python_callable=task_extract,
        execution_timeout=timedelta(minutes=10),
    )

    validate = PythonOperator(
        task_id=TASK_VALIDATE,
        python_callable=task_validate,
    )

    truncate = PythonOperator(
        task_id='truncate_tables',
        python_callable=task_truncate_tables,
    )

    with TaskGroup('spark_jobs') as spark_group:
        ingest = PythonOperator(
            task_id='ingest_data',
            python_callable=task_ingest,
            execution_timeout=timedelta(minutes=30),
        )
        clean = PythonOperator(
            task_id='clean_data',
            python_callable=task_clean,
            execution_timeout=timedelta(minutes=30),
        )
        transform = PythonOperator(
            task_id='transform_data',
            python_callable=task_transform,
            execution_timeout=timedelta(minutes=60),
        )
        load = PythonOperator(
            task_id='load_to_postgres',
            python_callable=task_load,
            execution_timeout=timedelta(hours=2),
        )
        ingest >> clean >> transform >> load  # pyright: ignore[reportUnusedExpression]

    similarity = PythonOperator(
        task_id='run_similarity_job',
        python_callable=task_similarity,
        execution_timeout=timedelta(hours=1),
    )

    quality_checks = PythonOperator(
        task_id=TASK_QC,
        python_callable=task_quality_checks,
    )

    log_run = PythonOperator(
        task_id='log_pipeline_run',
        python_callable=task_log_run,
        trigger_rule='all_success',
    )

    cleanup = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=task_cleanup,
        trigger_rule='all_done',
    )

    # ── Wire up the graph ────────────────────────────────────────────────────
    #
    #  start
    #    └─► ensure_schema
    #          └─► check_remote_updates
    #                ├─► log_skipped_run ──────────────────────────────► end
    #                └─► download ► extract ► validate ► truncate
    #                      └─► spark_group (ingest►clean►transform►load)
    #                            └─► similarity ► quality_checks
    #                                  └─► log_pipeline_run ► cleanup ► end

    start >> ensure_schema_task >> check_updates # type: ignore

    # Skip branch
    check_updates >> log_skipped >> end # pyright: ignore[reportUnusedExpression]

    # Full pipeline branch
    (
        check_updates
        >> download
        >> extract
        >> validate
        >> truncate
        >> spark_group
        >> similarity
        >> quality_checks
        >> log_run
        >> cleanup
        >> end
    )  # pyright: ignore[reportUnusedExpression]