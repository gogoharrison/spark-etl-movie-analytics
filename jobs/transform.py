# jobs/transform.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark
from clean import run_cleaning
 
 
def compute_movie_stats(ratings_df, movies_df):
    """
    Compute per-movie analytics using window functions.
    Joins with movies to get titles for readability.
    """
    # Window: per movie, ordered by date ascending
    movie_window = (
        Window
        .partitionBy('movieId')        # Separate calculation per movie
        .orderBy('rating_date')         # Oldest to newest within each movie
    )
 
    # Unbounded preceding = from the very first row
    # Current row       = up to and including this row
    cumulative_window = movie_window.rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
 
    # 30-day rolling window: look back 30 days from current row's date
    rolling_window = (
        Window
        .partitionBy('movieId')
        .orderBy(F.col('rating_date').cast('long'))  # Must be numeric for rangeBetween
        .rangeBetween(-30 * 86400, 0)               # 86400 = seconds in a day
    )
 
    movie_stats = (
        ratings_df
        # Cumulative count of ratings per movie
        .withColumn('cumulative_rating_count', F.count('rating').over(cumulative_window))
        # Cumulative running average rating
        .withColumn('cumulative_avg_rating',   F.avg('rating').over(cumulative_window))
        # 30-day rolling average rating
        .withColumn('rolling_30d_avg',         F.avg('rating').over(rolling_window))
        # 30-day rolling count
        .withColumn('rolling_30d_count',       F.count('rating').over(rolling_window))
        # Round to 2 decimal places for readability
        .withColumn('cumulative_avg_rating', F.round('cumulative_avg_rating', 2))
        .withColumn('rolling_30d_avg',       F.round('rolling_30d_avg', 2))
    )
 
    return movie_stats

 
def compute_genre_trends(ratings_df, movies_df):
    """
    Explode genre arrays to get one row per (rating, genre),
    then aggregate by genre and year.
    """
    # Join ratings with movies to get genre info
    joined = ratings_df.join(
        movies_df.select('movieId', 'genre_array', 'release_year'),
        on='movieId', how='inner'
    )
 
    # explode turns ['Action','Comedy'] into two separate rows
    exploded = joined.withColumn('genre', F.explode('genre_array'))
 
    # Aggregate: average rating per genre per year
    genre_trends = (
        exploded
        .groupBy('genre', 'rating_year')
        .agg(
            F.round(F.avg('rating'), 2).alias('avg_rating'),
            F.count('rating').alias('rating_count')
        )
        .orderBy('genre', 'rating_year')
    )
 
    return genre_trends
 
 
def compute_decade_stats(movies_df, ratings_df):
    """Aggregate top-rated movies grouped by decade of release."""
    # Decade: floor release_year to nearest 10
    movies_with_decade = movies_df.withColumn(
        'decade',
        (F.floor(F.col('release_year') / 10) * 10).cast('integer')
    )
 
    joined = ratings_df.join(
        movies_with_decade.select('movieId', 'clean_title', 'decade'),
        on='movieId', how='inner'
    )
 
    decade_stats = (
        joined
        .groupBy('movieId', 'clean_title', 'decade')
        .agg(
            F.round(F.avg('rating'), 2).alias('avg_rating'),
            F.count('rating').alias('total_ratings')
        )
        # Only movies with 100+ ratings (statistically meaningful)
        .filter(F.col('total_ratings') >= 100)
        .orderBy('decade', F.col('avg_rating').desc())
    )
 
    return decade_stats
 
 
if __name__ == '__main__':
    spark = get_spark('Transform')
    clean_r, clean_m, clean_t, genome_scores, genome_tags = run_cleaning(spark)
 
    print('Computing movie stats...')
    stats = compute_movie_stats(clean_r, clean_m)
    stats.select('movieId','rating_date','cumulative_rating_count',
                 'cumulative_avg_rating','rolling_30d_avg').show(10)
 
    print('Computing genre trends...')
    trends = compute_genre_trends(clean_r, clean_m)
    trends.show(10)
 
    print('Computing decade stats...')
    decades = compute_decade_stats(clean_m, clean_r)
    decades.show(10, truncate=False)
 
    spark.stop()
