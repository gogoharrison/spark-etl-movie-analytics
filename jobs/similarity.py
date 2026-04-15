# jobs/similarity.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark
from ingest import load_csv, GENOME_SCORES_SCHEMA, GENOME_TAGS_SCHEMA
from load import write_table
 
 
def compute_tag_similarity(spark, top_n_movies=500):
    """
    Compute pairwise cosine similarity between the top_n_movies
    most-rated movies using their genome tag vectors.
    We limit to top_n movies to keep computation feasible locally.
    """
    genome = load_csv(spark, 'genome-scores.csv', GENOME_SCORES_SCHEMA)
 
    # Step 1: Find the top N most-rated movies
    # (we need ratings count — load a small summary from Postgres)
    top_movies = (
        spark.read
        .format('jdbc')
        .option('url', 'jdbc:postgresql://postgres:5432/moviedb')
        .option('dbtable', '(SELECT movie_id, COUNT(*) as cnt FROM fact_ratings GROUP BY movie_id ORDER BY cnt DESC LIMIT {}) t'.format(top_n_movies))
        .option('user', 'movieuser')
        .option('password', 'movie123')
        .option('driver', 'org.postgresql.Driver')
        .load()
        .select(F.col('movie_id').alias('movieId'))
    )
 
    # Step 2: Filter genome scores to only top movies
    genome_filtered = genome.join(top_movies, on='movieId', how='inner')
 
    # Step 3: Self-join to get all movie pairs
    # Alias DataFrames to distinguish left and right side
    g1 = genome_filtered.alias('g1')
    g2 = genome_filtered.alias('g2')
 
    pairs = g1.join(g2, on=F.col('g1.tagId') == F.col('g2.tagId'), how='inner')
 
    # Step 4: Compute dot product and norms for cosine similarity
    # cosine_sim(A, B) = dot(A,B) / (|A| * |B|)
    similarity = (
        pairs
        .filter(F.col('g1.movieId') < F.col('g2.movieId'))  # Avoid duplicate pairs
        .groupBy(
            F.col('g1.movieId').alias('movie_id_a'),
            F.col('g2.movieId').alias('movie_id_b')
        )
        .agg(
            F.sum(F.col('g1.relevance') * F.col('g2.relevance')).alias('dot_product'),
            F.sqrt(F.sum(F.pow('g1.relevance', 2))).alias('norm_a'),
            F.sqrt(F.sum(F.pow('g2.relevance', 2))).alias('norm_b')
        )
        .withColumn(
            'similarity',
            F.round(F.col('dot_product') / (F.col('norm_a') * F.col('norm_b')), 4)
        )
        .filter(F.col('similarity') > 0.85)  # Only high-similarity pairs
        .select('movie_id_a', 'movie_id_b', 'similarity')
        .orderBy(F.col('similarity').desc())
    )
 
    return similarity
 
 
if __name__ == '__main__':
    spark = get_spark('TagSimilarity')
    print('Computing tag similarity (10-20 mins for top 500 movies)...')
    sim_df = compute_tag_similarity(spark, top_n_movies=500)
    write_table(sim_df, 'tag_similarity')
    print('Sample results:')
    sim_df.show(20, truncate=False)
    spark.stop()
