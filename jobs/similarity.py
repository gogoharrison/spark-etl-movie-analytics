# jobs/similarity.py
#
# Computes pairwise cosine similarity between the top-N most-rated movies
# using their MovieLens genome tag vectors.
#
# Algorithm:
#   cosine_sim(A, B) = dot(A, B) / (|A| * |B|)
#
# Scoped to top_n_movies to keep the self-join feasible on a local machine.
# For 500 movies the genome matrix is ~500 × 1128 tags; the self-join
# produces at most 500² / 2 ≈ 125 000 pairs before filtering.

import os
from pyspark.sql import functions as F
from spark_session import get_spark
from ingest import load_csv, GENOME_SCORES_SCHEMA
from load import write_table

# ── Connection config (env-vars with local-machine defaults) ─────────────────
JDBC_URL = os.environ.get('JDBC_URL', 'jdbc:postgresql://localhost:5433/moviedb')
DB_USER  = os.environ.get('MOVIE_DB_USER', 'movieuser')
DB_PASS  = os.environ.get('MOVIE_DB_PASS', 'movie123')
DRIVER   = 'org.postgresql.Driver'


def _read_jdbc(spark, query):
    """Helper: execute a pushdown SQL query via JDBC and return a DataFrame."""
    return (
        spark.read
        .format('jdbc')
        .option('url',      JDBC_URL)
        .option('dbtable',  query)
        .option('user',     DB_USER)
        .option('password', DB_PASS)
        .option('driver',   DRIVER)
        .load()
    )


def compute_tag_similarity(spark, top_n_movies=500):
    """
    Compute pairwise cosine similarity for the top-N most-rated movies.

    Args:
        spark:         Active SparkSession.
        top_n_movies:  Number of movies to consider (ranked by rating count).

    Returns:
        DataFrame with columns: movie_id_a, movie_id_b, similarity (NUMERIC 0-1).
        Only pairs with similarity > 0.85 are returned.

    Steps:
        1. Pull the top-N movie IDs from fact_ratings via JDBC.
        2. Filter genome-scores.csv to those movies.
        3. Self-join on tagId to build (movie_a, movie_b, tag) triples.
        4. Aggregate dot product and L2 norms, compute cosine similarity.
        5. Filter to similarity > 0.85 and return.
    """
    genome = load_csv(spark, 'genome-scores.csv', GENOME_SCORES_SCHEMA)

    # Step 1: top-N movie IDs from Postgres
    # top_n_movies is always an int (never user input) but we validate anyway.
    limit = int(top_n_movies)
    top_movies_query = (
        f'(SELECT movie_id '
        f'FROM (SELECT movie_id, COUNT(*) AS cnt '
        f'      FROM fact_ratings '
        f'      GROUP BY movie_id '
        f'      ORDER BY cnt DESC '
        f'      LIMIT {limit}) ranked) top_movies'
    )
    top_movies = (
        _read_jdbc(spark, top_movies_query)
        .select(F.col('movie_id').alias('movieId'))
    )

    # Step 2: filter genome scores
    genome_filtered = genome.join(top_movies, on='movieId', how='inner')

    # Step 3: self-join on tagId (g1.movieId < g2.movieId avoids duplicate pairs)
    g1 = genome_filtered.alias('g1')
    g2 = genome_filtered.alias('g2')

    pairs = (
        g1.join(g2, on=F.col('g1.tagId') == F.col('g2.tagId'), how='inner')
        .filter(F.col('g1.movieId') < F.col('g2.movieId'))
    )

    # Step 4: cosine similarity per movie pair
    similarity = (
        pairs
        .groupBy(
            F.col('g1.movieId').alias('movie_id_a'),
            F.col('g2.movieId').alias('movie_id_b'),
        )
        .agg(
            F.sum(F.col('g1.relevance') * F.col('g2.relevance')).alias('dot_product'),
            F.sqrt(F.sum(F.pow(F.col('g1.relevance'), 2))).alias('norm_a'),
            F.sqrt(F.sum(F.pow(F.col('g2.relevance'), 2))).alias('norm_b'),
        )
        .withColumn(
            'similarity',
            F.round(
                F.col('dot_product') / (F.col('norm_a') * F.col('norm_b')),
                4,
            ),
        )
        # Step 5: keep only high-similarity pairs
        .filter(F.col('similarity') > 0.85)
        .select('movie_id_a', 'movie_id_b', 'similarity')
        .orderBy(F.col('similarity').desc())
    )

    return similarity


# ── Standalone execution (local dev / debugging) ─────────────────────────────

if __name__ == '__main__':
    spark = get_spark('TagSimilarity-Standalone')
    print('Computing tag similarity for top 500 movies (may take 10–20 min)...')
    sim_df = compute_tag_similarity(spark, top_n_movies=500)
    write_table(sim_df, 'tag_similarity')
    print('Top 20 most-similar pairs:')
    sim_df.show(20, truncate=False)
    spark.stop()