# jobs/load.py
#
# Writes clean and transformed DataFrames to Postgres via JDBC.
#
# Design contract:
#   - load_dimensions()              accepts clean_m, clean_r DataFrames
#   - load_fact_ratings()            accepts clean_r DataFrame
#   - load_analytics_from_parquet()  reads genre_trends + decade_stats
#                                    from pre-computed Parquet files
#   - write_table()                  reusable JDBC writer (used by similarity too)
#
# run_all() is retained for standalone local testing only.
# The Airflow DAG calls each loader function individually after reading
# from the processed Parquet layer — it never calls run_all().

import os
from pyspark.sql import functions as F
from spark_session import get_spark
from clean import run_cleaning
from transform import compute_genre_trends, compute_decade_stats

# ── Connection config (env-vars with local-machine defaults) ─────────────────
JDBC_URL = os.environ.get('JDBC_URL', 'jdbc:postgresql://localhost:5433/moviedb')
DB_USER  = os.environ.get('MOVIE_DB_USER', 'movieuser')
DB_PASS  = os.environ.get('MOVIE_DB_PASS', 'movie123')
DRIVER   = 'org.postgresql.Driver'


# ── Core JDBC writer ─────────────────────────────────────────────────────────

def write_table(df, table_name, mode='append'):
    """
    Write a Spark DataFrame to a Postgres table via JDBC.

    Args:
        df:         Spark DataFrame to write.
        table_name: Target Postgres table name.
        mode:       Spark write mode — 'overwrite' or 'append'.
                    Note: 'overwrite' via JDBC drops and recreates the
                    table structure; for production use TRUNCATE + append
                    (handled by the dedicated truncate task in the DAG).
    """
    (
        df.write
        .format('jdbc')
        .option('url',      JDBC_URL)
        .option('dbtable',  table_name)
        .option('user',     DB_USER)
        .option('password', DB_PASS)
        .option('driver',   DRIVER)
        .option('batchsize', 10_000)
        .mode(mode)
        .save()
    )
    print(f'  ✓ Written → {table_name}')


# ── Dimension loaders ────────────────────────────────────────────────────────

def load_dimensions(movies_df, ratings_df):
    """Load dim_movies and dim_users from clean DataFrames."""
    dim_movies = movies_df.select(
        F.col('movieId').alias('movie_id'),
        F.col('title'),
        F.col('clean_title'),
        F.col('genres'),
        F.col('release_year'),
    )
    write_table(dim_movies, 'dim_movies')
    print('dim_movies loaded.')

    dim_users = ratings_df.select(
        F.col('userId').alias('user_id')
    ).distinct()
    write_table(dim_users, 'dim_users')
    print('dim_users loaded.')


# ── Fact loader ──────────────────────────────────────────────────────────────

def load_fact_ratings(ratings_df, movies_df):
    """
    Load fact_ratings year by year.
    Anti-join against clean_movies first to drop any ratings whose
    movie_id was filtered out during cleaning (e.g. no-genre movies).
    This prevents FK constraint violations on fact_ratings_movie_id_fkey.
    """
    # Keep only ratings whose movie_id exists in dim_movies
    valid_movie_ids = movies_df.select('movieId')
    filtered = ratings_df.join(valid_movie_ids, on='movieId', how='inner')

    fact = filtered.select(
        F.col('userId').alias('user_id'),
        F.col('movieId').alias('movie_id'),
        F.col('rating'),
        F.col('rating_date'),
        F.col('rating_year'),
    )

    years = [
        row['rating_year']
        for row in (
            fact.select('rating_year')
            .distinct()
            .orderBy('rating_year')
            .collect()
        )
    ]

    if not years:
        raise ValueError('fact_ratings: no years found after filtering.')

    print(f'Writing fact_ratings — {len(years)} year(s): {years[0]}–{years[-1]}')

    for i, year in enumerate(years):
        year_df = fact.filter(F.col('rating_year') == year).coalesce(2)
        write_table(year_df, 'fact_ratings', mode='append')
        print(f'    year {year} written ({i + 1}/{len(years)})')

    print('fact_ratings load complete.')


# ── Analytics loader (reads from Parquet written by transform task) ──────────

def load_analytics_from_parquet(spark, genre_trends_path, decade_stats_path):
    """
    Load pre-computed analytics tables from Parquet.

    Called by the Airflow load task after transform() has already
    computed and persisted these DataFrames. Reading from Parquet is
    significantly faster than recomputing from raw ratings.
    """
    genre_trends = spark.read.parquet(genre_trends_path)
    write_table(genre_trends, 'genre_trends')
    print('genre_trends loaded.')

    decade_stats = spark.read.parquet(decade_stats_path)
    write_table(decade_stats, 'decade_stats')
    print('decade_stats loaded.')


# ── Standalone helper (local testing only — not called by the DAG) ───────────

def load_analytics(ratings_df, movies_df):
    """
    Compute and load analytics inline.
    Used only when running load.py directly (local development / debugging).
    The DAG uses load_analytics_from_parquet() instead.
    """
    trends = compute_genre_trends(ratings_df, movies_df)
    write_table(trends, 'genre_trends')
    print('genre_trends loaded.')

    decades = compute_decade_stats(movies_df, ratings_df)
    write_table(decades, 'decade_stats')
    print('decade_stats loaded.')


def run_all(spark):
    """
    Full load from scratch — for local standalone testing only.
    The Airflow DAG does NOT call this function; it calls each loader
    individually after reading from the processed Parquet layer.
    """
    print('run_all(): re-cleaning raw data (standalone mode)...')
    clean_r, clean_m, *_ = run_cleaning(spark)

    print('Loading dimension tables...')
    load_dimensions(clean_m, clean_r)

    print('Loading fact_ratings year by year...')
    load_fact_ratings(clean_r, clean_m)

    print('Loading analytics tables...')
    load_analytics(clean_r, clean_m)

    print('All tables loaded successfully.')


if __name__ == '__main__':
    spark = get_spark('Load-Standalone')
    run_all(spark)
    spark.stop()