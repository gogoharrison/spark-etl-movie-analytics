# jobs/ingest.py
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    FloatType, LongType, StringType
)
from spark_session import get_spark
import os
 
#RAW = 'data/raw'
# jobs/ingest.py  — only change this one line
RAW = os.environ.get('RAW_DATA_PATH', 'data/raw')
 
# ── Schemas ─────────────────────────────────────────────────────────
 
RATINGS_SCHEMA = StructType([
    StructField('userId',    IntegerType(), nullable=False),
    StructField('movieId',   IntegerType(), nullable=False),
    StructField('rating',    FloatType(),   nullable=False),
    StructField('timestamp', LongType(),    nullable=False),
])
 
MOVIES_SCHEMA = StructType([
    StructField('movieId', IntegerType(), nullable=False),
    StructField('title',   StringType(),  nullable=False),
    StructField('genres',  StringType(),  nullable=False),
])
 
TAGS_SCHEMA = StructType([
    StructField('userId',    IntegerType(), nullable=True),
    StructField('movieId',   IntegerType(), nullable=False),
    StructField('tag',       StringType(),  nullable=True),
    StructField('timestamp', LongType(),    nullable=False),
])
 
GENOME_SCORES_SCHEMA = StructType([
    StructField('movieId',   IntegerType(), nullable=False),
    StructField('tagId',     IntegerType(), nullable=False),
    StructField('relevance', FloatType(),   nullable=False),
])
 
GENOME_TAGS_SCHEMA = StructType([
    StructField('tagId', IntegerType(), nullable=False),
    StructField('tag',   StringType(),  nullable=False),
])
 
# ── Load functions ───────────────────────────────────────────────────
 
def load_csv(spark, filename, schema):
    path = os.path.join(RAW, filename)
    return (
        spark.read
        .option('header', 'true')   # First row is column names
        .option('mode', 'DROPMALFORMED')  # Skip bad rows
        .schema(schema)
        .csv(path)
    )
 
 
def load_all(spark):
    ratings       = load_csv(spark, 'ratings.csv',       RATINGS_SCHEMA)
    movies        = load_csv(spark, 'movies.csv',         MOVIES_SCHEMA)
    tags          = load_csv(spark, 'tags.csv',           TAGS_SCHEMA)
    genome_scores = load_csv(spark, 'genome-scores.csv',  GENOME_SCORES_SCHEMA)
    genome_tags   = load_csv(spark, 'genome-tags.csv',    GENOME_TAGS_SCHEMA)
    return ratings, movies, tags, genome_scores, genome_tags
 
 
# ── Validation ───────────────────────────────────────────────────────
 
def validate(spark):
    ratings, movies, tags, genome_scores, genome_tags = load_all(spark)
 
    print('=== Row counts ===')
    print(f'ratings:       {ratings.count():,}')
    print(f'movies:        {movies.count():,}')
    print(f'tags:          {tags.count():,}')
    print(f'genome_scores: {genome_scores.count():,}')
    print(f'genome_tags:   {genome_tags.count():,}')
 
    print('\n=== Ratings sample ===')
    ratings.show(5)
 
    print('\n=== Movies sample ===')
    movies.show(5, truncate=False)
 
    return ratings, movies, tags, genome_scores, genome_tags
 
 
if __name__ == '__main__':
    spark = get_spark('Ingest-Validate')
    validate(spark)
    spark.stop()
