# dags/movie_pipeline_dag.py
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'jobs'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'dags', 'utils'))

RAW_DIR  = os.path.join(PROJECT_ROOT, 'data', 'raw')
ZIP_PATH = os.path.join(RAW_DIR, 'ml-latest.zip')

default_args = {
    'owner':                     'harrison',
    'depends_on_past':           False,
    'retries':                   2,
    'retry_delay':               timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'email_on_failure':          False,
    'email_on_retry':            False,
}


def task_download(**context):
    from dataset_utils import download_dataset
    os.makedirs(RAW_DIR, exist_ok=True)
    download_dataset()

def task_ensure_schema(**context):
    from dataset_utils import ensure_schema
    ensure_schema()


def task_check_updates(**context):
    from dataset_utils import has_dataset_changed
    if not has_dataset_changed():
        raise AirflowSkipException('Dataset unchanged — skipping pipeline')
    print('Dataset has changed — proceeding with full pipeline')


def task_extract(**context):
    print(f'Extracting {ZIP_PATH} to {RAW_DIR}')
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        z.extractall(RAW_DIR)
    extracted_dir = os.path.join(RAW_DIR, 'ml-latest')
    if os.path.exists(extracted_dir):
        for fname in os.listdir(extracted_dir):
            src = os.path.join(extracted_dir, fname)
            dst = os.path.join(RAW_DIR, fname)
            os.replace(src, dst)
        os.rmdir(extracted_dir)
    print('Extraction complete')


def task_validate(**context):
    required = ['ratings.csv', 'movies.csv', 'tags.csv',
                'genome-scores.csv', 'genome-tags.csv']
    missing  = [f for f in required
                if not os.path.exists(os.path.join(RAW_DIR, f))]
    if missing:
        raise FileNotFoundError(f'Missing files: {missing}')
    with open(os.path.join(RAW_DIR, 'ratings.csv'), 'r') as f:
        ratings_count = sum(1 for _ in f) - 1  # subtract header row
    print(f'Validation passed — ratings rows: {ratings_count:,}')
    if ratings_count < 1_000_000:
        raise ValueError(f'Unexpectedly low row count: {ratings_count:,}')
    context['ti'].xcom_push(key='ratings_count', value=ratings_count)


def task_ingest(**context):
    from spark_session import get_spark
    from ingest import validate
    spark = get_spark('Airflow-Ingest')
    try:
        validate(spark)
    finally:
        spark.stop()


def task_clean(**context):
    from spark_session import get_spark
    from clean import run_cleaning
    spark = get_spark('Airflow-Clean')
    try:
        run_cleaning(spark)
    finally:
        spark.stop()


def task_transform(**context):
    from spark_session import get_spark
    from clean import run_cleaning
    from transform import (compute_movie_stats,
                           compute_genre_trends,
                           compute_decade_stats)
    spark = get_spark('Airflow-Transform')
    try:
        clean_r, clean_m, *_ = run_cleaning(spark)
        compute_movie_stats(clean_r, clean_m)
        compute_genre_trends(clean_r, clean_m)
        compute_decade_stats(clean_m, clean_r)
        print('All transformations complete')
    finally:
        spark.stop()


def task_load(**context):
    from dataset_utils import truncate_tables
    from spark_session import get_spark
    from load import run_all
    start = time.time()
    truncate_tables()
    spark = get_spark('Airflow-Load')
    try:
        run_all(spark)
    finally:
        spark.stop()
    duration = int(time.time() - start)
    context['ti'].xcom_push(key='load_duration_secs', value=duration)
    print(f'Load complete in {duration}s')


def task_similarity(**context):
    from spark_session import get_spark
    from similarity import compute_tag_similarity
    from load import write_table
    spark = get_spark('Airflow-Similarity')
    try:
        sim_df = compute_tag_similarity(spark, top_n_movies=500)
        write_table(sim_df, 'tag_similarity')
    finally:
        spark.stop()


def task_log_run(**context):
    from dataset_utils import save_current_hash, log_pipeline_run, record_table_stats
    ti = context['ti']
    ratings_count = ti.xcom_pull(task_ids='validate_data',
                                  key='ratings_count') or 0
    load_duration = ti.xcom_pull(task_ids='spark_jobs.load_to_postgres',
                                  key='load_duration_secs') or 0
    table_stats = ti.xcom_pull(task_ids='data_quality_checks',
                                key='table_stats') or {}
    dataset_hash  = save_current_hash()
    run_id = log_pipeline_run(
        dataset_hash=dataset_hash,
        status='success',
        rows_loaded=ratings_count,
        duration_secs=load_duration
    )
    record_table_stats(run_id, table_stats)


def task_cleanup(**context):
    from dataset_utils import cleanup_zip
    cleanup_zip()
    print('Cleanup complete')


def task_quality_checks(**context):
    from dataset_utils import run_data_quality_checks
    ti = context['ti']
    ratings_count = ti.xcom_pull(task_ids='validate_data',
                                  key='ratings_count') or None
    table_stats = run_data_quality_checks(expected_ratings=ratings_count)
    ti.xcom_push(key='table_stats', value=table_stats)


with DAG(
    dag_id='movie_analytics_pipeline',
    default_args=default_args,
    description='End-to-end MovieLens ETL — download, transform, load, similarity',
    schedule_interval='0 2 * * 0',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['movie', 'etl', 'spark', 'production'],
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    ensure_schema = PythonOperator(
        task_id='ensure_schema',
        python_callable=task_ensure_schema,
    )

    download = PythonOperator(
        task_id='download_dataset',
        python_callable=task_download,
        execution_timeout=timedelta(minutes=30),
    )

    check_updates = PythonOperator(
        task_id='check_for_updates',
        python_callable=task_check_updates,
    )

    extract = PythonOperator(
        task_id='extract_files',
        python_callable=task_extract,
        execution_timeout=timedelta(minutes=10),
    )

    validate = PythonOperator(
        task_id='validate_data',
        python_callable=task_validate,
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
        ingest >> clean >> transform >> load # pyright: ignore[reportUnusedExpression]

    similarity = PythonOperator(
        task_id='run_similarity_job',
        python_callable=task_similarity,
        execution_timeout=timedelta(hours=1),
    )

    quality_checks = PythonOperator(
        task_id='data_quality_checks',
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

    (
        start
        >> ensure_schema
        >> download
        >> check_updates
        >> extract
        >> validate
        >> spark_group
        >> similarity
        >> quality_checks
        >> log_run
        >> cleanup
        >> end
    ) # pyright: ignore[reportUnusedExpression]
