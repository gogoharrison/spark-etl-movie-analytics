# jobs/load.py
import os
from pyspark.sql import functions as F
from spark_session import get_spark
from clean import run_cleaning
from transform import compute_genre_trends, compute_decade_stats

# ── Connection config ────────────────────────────────────────────────
JDBC_URL = os.environ.get(
    'JDBC_URL',
    'jdbc:postgresql://postgres:5432/moviedb'
)
USER     = 'movieuser'
PASSWORD = 'movie123'
DRIVER   = 'org.postgresql.Driver'


def write_table(df, table_name, mode='overwrite', partition_col=None):
    writer = (
        df.write
        .format('jdbc')
        .option('url',      JDBC_URL)
        .option('dbtable',  table_name)
        .option('user',     USER)
        .option('password', PASSWORD)
        .option('driver',   DRIVER)
        .option('batchsize', 10000)
        .mode(mode)
    )
    writer.save()
    print(f'Written to {table_name}')


def load_dimensions(movies_df, ratings_df):
    """Load dim_movies and dim_users."""
    dim_movies = movies_df.select(
        F.col('movieId').alias('movie_id'),
        F.col('title'),
        F.col('clean_title'),
        F.col('genres'),
        F.col('release_year')
    )
    write_table(dim_movies, 'dim_movies')
    print('dim_movies loaded')

    dim_users = ratings_df.select(
        F.col('userId').alias('user_id')
    ).distinct()
    write_table(dim_users, 'dim_users')
    print('dim_users loaded')


def load_fact_ratings(ratings_df):
    """
    Load fact_ratings without window functions to avoid OOM.
    Writes year by year in append mode.
    """
    fact = ratings_df.select(
        F.col('userId').alias('user_id'),
        F.col('movieId').alias('movie_id'),
        F.col('rating'),
        F.col('rating_date'),
        F.col('rating_year')
    )

    years = [
        row['rating_year']
        for row in fact.select('rating_year')
        .distinct()
        .orderBy('rating_year')
        .collect()
    ]

    print(f'Writing fact_ratings for {len(years)} years: '
          f'{years[0]} to {years[-1]}')

    for i, year in enumerate(years):
        mode   = 'overwrite' if i == 0 else 'append'
        year_df = fact.filter(F.col('rating_year') == year).coalesce(1)
        write_table(year_df, 'fact_ratings', mode=mode)
        print(f'  Written year {year} ({i + 1}/{len(years)})')

    print('fact_ratings load complete')


def load_analytics(ratings_df, movies_df):
    """Load pre-aggregated analytics tables."""
    trends = compute_genre_trends(ratings_df, movies_df)
    write_table(trends, 'genre_trends')
    print('genre_trends loaded')

    decades = compute_decade_stats(movies_df, ratings_df)
    write_table(decades, 'decade_stats')
    print('decade_stats loaded')


def run_all(spark):
    print('Loading and cleaning data...')
    clean_r, clean_m, clean_t, genome_scores, genome_tags = run_cleaning(spark)

    print('Loading dimension tables...')
    load_dimensions(clean_m, clean_r)

    print('Loading fact_ratings year by year...')
    load_fact_ratings(clean_r)

    print('Loading analytics tables...')
    load_analytics(clean_r, clean_m)

    print('All tables loaded successfully!')


if __name__ == '__main__':
    spark = get_spark('Load')
    run_all(spark)
    spark.stop()