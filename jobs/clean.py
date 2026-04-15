# jobs/clean.py
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from spark_session import get_spark
from ingest import load_all
 
 
def clean_ratings(df):
    return (
        df
        # Drop rows where any required field is null
        .dropna(subset=['userId', 'movieId', 'rating', 'timestamp'])
        # Remove duplicate (userId, movieId) pairs — keep last rating
        .dropDuplicates(['userId', 'movieId'])
        # Convert Unix timestamp (seconds) to a date column
        .withColumn('rating_date', F.to_date(F.from_unixtime('timestamp')))
        # Extract year for partitioning
        .withColumn('rating_year', F.year('rating_date'))
        # Keep only valid rating range 0.5 to 5.0
        .filter((F.col('rating') >= 0.5) & (F.col('rating') <= 5.0))
    )
 
 
def clean_movies(df):
    return (
        df
        .dropna(subset=['movieId', 'title'])
        .dropDuplicates(['movieId'])
        # Split 'Action|Comedy|Drama' into ['Action','Comedy','Drama']
        .withColumn(
            'genre_array',
            F.split(F.col('genres'), '\\|')   # Split on pipe character
        )
        # Extract release year from title like 'Toy Story (1995)'
        .withColumn(
            'release_year',
            F.regexp_extract(F.col('title'), r'\((\d{4})\)', 1).cast('integer')
        )
        # Clean title: remove the year in parentheses
        .withColumn(
            'clean_title',
            F.regexp_replace(F.col('title'), r'\s*\(\d{4}\)\s*$', '')
        )
        # Exclude movies with no genre info
        .filter(F.col('genres') != '(no genres listed)')
    )
 
 
def clean_tags(df):
    return (
        df
        .dropna(subset=['userId', 'movieId', 'tag'])
        # Trim whitespace and lowercase tags for consistency
        .withColumn('tag', F.trim(F.lower(F.col('tag'))))
        # Remove very short tags (likely noise)
        .filter(F.length(F.col('tag')) >= 2)
        .dropDuplicates(['userId', 'movieId', 'tag'])
    )
 
 
def run_cleaning(spark):
    ratings, movies, tags, genome_scores, genome_tags = load_all(spark)
 
    clean_r = clean_ratings(ratings)
    clean_m = clean_movies(movies)
    clean_t = clean_tags(tags)
 
    print('Cleaned ratings:', clean_r.count())
    print('Cleaned movies: ', clean_m.count())
    print('Cleaned tags:   ', clean_t.count())
 
    print('\nMovies schema after cleaning:')
    clean_m.printSchema()
    clean_m.select('clean_title', 'release_year', 'genre_array').show(5, truncate=False)
 
    return clean_r, clean_m, clean_t, genome_scores, genome_tags
 
 
if __name__ == '__main__':
    spark = get_spark('Cleaning')
    run_cleaning(spark)
    spark.stop()
