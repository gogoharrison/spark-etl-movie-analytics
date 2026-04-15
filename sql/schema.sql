-- sql/schema.sql
 
-- Dimension: movies
CREATE TABLE IF NOT EXISTS dim_movies (
    movie_id      INTEGER PRIMARY KEY,
    title         VARCHAR(500) NOT NULL,
    clean_title   VARCHAR(500),
    genres        TEXT,                    -- Original pipe-separated string
    release_year  SMALLINT,
    created_at    TIMESTAMP DEFAULT NOW()
);
 
-- Dimension: users (minimal — MovieLens anonymises users)
CREATE TABLE IF NOT EXISTS dim_users (
    user_id     INTEGER PRIMARY KEY,
    created_at  TIMESTAMP DEFAULT NOW()
);
 
-- Fact: ratings with pre-computed analytics columns
CREATE TABLE IF NOT EXISTS fact_ratings (
    user_id                   INTEGER NOT NULL REFERENCES dim_users(user_id),
    movie_id                  INTEGER NOT NULL REFERENCES dim_movies(movie_id),
    rating                    NUMERIC(3,1) NOT NULL,
    rating_date               DATE NOT NULL,
    rating_year               SMALLINT NOT NULL,
    cumulative_rating_count   INTEGER,
    cumulative_avg_rating     NUMERIC(4,2),
    rolling_30d_avg           NUMERIC(4,2),
    rolling_30d_count         INTEGER,
    PRIMARY KEY (user_id, movie_id)
);
 
-- Analytics: genre trends
CREATE TABLE IF NOT EXISTS genre_trends (
    genre         VARCHAR(100) NOT NULL,
    rating_year   SMALLINT NOT NULL,
    avg_rating    NUMERIC(4,2),
    rating_count  INTEGER,
    PRIMARY KEY (genre, rating_year)
);
 
-- Analytics: decade stats
CREATE TABLE IF NOT EXISTS decade_stats (
    movie_id      INTEGER PRIMARY KEY,
    clean_title   VARCHAR(500),
    decade        SMALLINT,
    avg_rating    NUMERIC(4,2),
    total_ratings INTEGER
);
 
-- Tag similarity output (Phase 10)
CREATE TABLE IF NOT EXISTS tag_similarity (
    movie_id_a    INTEGER NOT NULL,
    movie_id_b    INTEGER NOT NULL,
    similarity    NUMERIC(6,4),
    PRIMARY KEY (movie_id_a, movie_id_b)
);

-- Add to sql/schema.sql
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          SERIAL PRIMARY KEY,
    run_date        TIMESTAMP DEFAULT NOW(),
    dataset_hash    VARCHAR(64),
    status          VARCHAR(20),   -- 'success', 'failed', 'skipped'
    rows_loaded     INTEGER,
    duration_secs   INTEGER,
    source_url      VARCHAR(500),
    pipeline_version VARCHAR(40)
);

-- Backfill new columns on existing installs
ALTER TABLE pipeline_run_log
    ADD COLUMN IF NOT EXISTS source_url VARCHAR(500);
ALTER TABLE pipeline_run_log
    ADD COLUMN IF NOT EXISTS pipeline_version VARCHAR(40);

-- Per-run table stats for drift tracking
CREATE TABLE IF NOT EXISTS pipeline_table_stats (
    run_id     INTEGER NOT NULL REFERENCES pipeline_run_log(run_id) ON DELETE CASCADE,
    table_name VARCHAR(100) NOT NULL,
    row_count  INTEGER NOT NULL,
    PRIMARY KEY (run_id, table_name)
);
 
-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fact_movie  ON fact_ratings(movie_id);
CREATE INDEX IF NOT EXISTS idx_fact_year   ON fact_ratings(rating_year);
CREATE INDEX IF NOT EXISTS idx_decade      ON decade_stats(decade, avg_rating DESC);
CREATE INDEX IF NOT EXISTS idx_genre_year  ON genre_trends(genre, rating_year);
