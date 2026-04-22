# jobs/transform.py
#
# All three compute functions return a Spark DataFrame.
# The DAG's transform task writes each one to Parquet.
# The load task reads those Parquet files — no recomputation needed.

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark
from clean import run_cleaning


def compute_movie_stats(ratings_df, movies_df):
    """
    Enrich each rating row with cumulative and rolling analytics columns.

    Window functions used:
      cumulative_window  — unbounded preceding → current row (running totals)
      rolling_window     — 30-day look-back using rangeBetween on Unix seconds

    Returns the enriched ratings DataFrame (same grain as fact_ratings).
    These columns are written to fact_ratings alongside the base columns.

    Note: rating_date must be a DateType column (produced by clean_ratings).
    """
    cumulative_window = (
        Window
        .partitionBy('movieId')
        .orderBy('rating_date')
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    rolling_window = (
        Window
        .partitionBy('movieId')
        .orderBy(F.col('rating_date').cast('long'))
        .rangeBetween(-30 * 86_400, 0)   # 86 400 = seconds per day
    )

    return (
        ratings_df
        .withColumn('cumulative_rating_count',
                    F.count('rating').over(cumulative_window))
        .withColumn('cumulative_avg_rating',
                    F.round(F.avg('rating').over(cumulative_window), 2))
        .withColumn('rolling_30d_avg',
                    F.round(F.avg('rating').over(rolling_window), 2))
        .withColumn('rolling_30d_count',
                    F.count('rating').over(rolling_window))
    )


def compute_genre_trends(ratings_df, movies_df):
    """
    Aggregate average rating and rating count per genre per year.

    Steps:
      1. Join ratings → movies to get genre_array.
      2. Explode genre_array so each row represents one (rating, genre) pair.
      3. Group by (genre, rating_year) and aggregate.

    Returns a genre_trends DataFrame that maps directly to the genre_trends table.
    """
    joined = ratings_df.join(
        movies_df.select('movieId', 'genre_array'),
        on='movieId',
        how='inner',
    )

    exploded = joined.withColumn('genre', F.explode('genre_array'))

    return (
        exploded
        .groupBy('genre', 'rating_year')
        .agg(
            F.round(F.avg('rating'), 2).alias('avg_rating'),
            F.count('rating').alias('rating_count'),
        )
        .orderBy('genre', 'rating_year')
    )


def compute_decade_stats(movies_df, ratings_df):
    """
    Per-movie aggregate stats grouped by release decade.

    Only movies with 100+ ratings are included (statistically meaningful).
    Decade is derived by flooring release_year to the nearest 10.

    Returns a decade_stats DataFrame that maps directly to the decade_stats table.
    """
    movies_with_decade = movies_df.withColumn(
        'decade',
        (F.floor(F.col('release_year') / 10) * 10).cast('integer'),
    )

    joined = ratings_df.join(
        movies_with_decade.select('movieId', 'clean_title', 'decade'),
        on='movieId',
        how='inner',
    )

    return (
        joined
        .groupBy('movieId', 'clean_title', 'decade')
        .agg(
            F.round(F.avg('rating'), 2).alias('avg_rating'),
            F.count('rating').alias('total_ratings'),
        )
        .filter(F.col('total_ratings') >= 100)
        .withColumn('movie_id', F.col('movieId')) 
        .drop('movieId')        
        .orderBy('decade', F.col('avg_rating').desc())
    )


# ── Standalone execution (local dev / debugging) ─────────────────────────────

if __name__ == '__main__':
    spark = get_spark('Transform-Standalone')
    clean_r, clean_m, *_ = run_cleaning(spark)

    print('Computing movie stats...')
    stats = compute_movie_stats(clean_r, clean_m)
    stats.select(
        'movieId', 'rating_date',
        'cumulative_rating_count', 'cumulative_avg_rating', 'rolling_30d_avg',
    ).show(10)

    print('Computing genre trends...')
    compute_genre_trends(clean_r, clean_m).show(10)

    print('Computing decade stats...')
    compute_decade_stats(clean_m, clean_r).show(10, truncate=False)

    spark.stop()