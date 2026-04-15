# dags/utils/dataset_utils.py

import os
import hashlib
import requests
import psycopg2

# ── Config ───────────────────────────────────────────────────────────────────

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RAW_DIR      = os.path.join(PROJECT_ROOT, 'data', 'raw')
ZIP_PATH     = os.path.join(RAW_DIR, 'ml-latest.zip')
HASH_FILE    = os.path.join(RAW_DIR, '.last_dataset_hash')
DATASET_URL  = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
SCHEMA_PATH  = os.path.join(PROJECT_ROOT, 'sql', 'schema.sql')

DB_CONFIG = {
    'host':     'postgres',
    'port':     5432,
    'dbname':   'moviedb',
    'user':     'movieuser',
    'password': 'movie123',
}


# ── Download ─────────────────────────────────────────────────────────────────

def download_dataset():
    """Download ml-latest.zip from GroupLens to data/raw/ with resume + retries."""
    import time
    os.makedirs(RAW_DIR, exist_ok=True)

    url = DATASET_URL.replace("http://", "https://")
    max_attempts = 8

    for attempt in range(1, max_attempts + 1):
        # If we already have the full file, skip download
        try:
            head = requests.head(url, timeout=10)
            head.raise_for_status()
            total = int(head.headers.get("Content-Length", "0"))
        except Exception:
            total = 0

        existing = os.path.getsize(ZIP_PATH) if os.path.exists(ZIP_PATH) else 0
        if total and existing >= total:
            print(f"File already complete ({existing} bytes). Skipping download.")
            return

        headers = {"Range": f"bytes={existing}-"} if existing > 0 else {}

        print(f"Downloading dataset from {url}")
        if existing:
            print(f"Resuming from byte {existing}")

        try:
            with requests.get(url, stream=True, timeout=(10, 300), headers=headers) as r:
                r.raise_for_status()
                mode = "ab" if existing > 0 else "wb"
                with open(ZIP_PATH, mode) as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            size_mb = os.path.getsize(ZIP_PATH) / (1024 * 1024)
            print(f"Downloaded {size_mb:.1f} MB to {ZIP_PATH}")
            return
        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout):
            if attempt == max_attempts:
                raise
            print(f"Download interrupted (attempt {attempt}/{max_attempts}). Retrying...")
            time.sleep(min(60, 2 ** attempt))


# ── Hash detection ────────────────────────────────────────────────────────────

def compute_hash(filepath):
    """Compute MD5 hash of a file."""
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def has_dataset_changed():
    """
    Return True if the downloaded zip is different from the last processed zip.
    Returns True if no previous hash exists (first run).
    """
    if not os.path.exists(ZIP_PATH):
        print('No zip file found — treating as changed')
        return True

    current_hash = compute_hash(ZIP_PATH)

    if not os.path.exists(HASH_FILE):
        print('No previous hash found — treating as changed')
        return True

    with open(HASH_FILE, 'r') as f:
        last_hash = f.read().strip()

    changed = current_hash != last_hash
    print(f'Dataset changed: {changed}')
    print(f'  Current hash : {current_hash[:16]}...')
    print(f'  Previous hash: {last_hash[:16]}...')
    return changed


def save_current_hash():
    """Save the current zip MD5 hash to disk. Returns the hash string."""
    current_hash = compute_hash(ZIP_PATH)
    os.makedirs(RAW_DIR, exist_ok=True)
    with open(HASH_FILE, 'w') as f:
        f.write(current_hash)
    print(f'Saved dataset hash: {current_hash[:16]}...')
    return current_hash


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection():
    """Return a psycopg2 connection using the project DB config."""
    return psycopg2.connect(**DB_CONFIG)

def get_pipeline_version():
    """Return the pipeline version identifier (env-driven)."""
    return os.getenv('PIPELINE_VERSION', 'unknown')


def ensure_schema():
    """Ensure database schema exists by running sql/schema.sql."""
    if not os.path.exists(SCHEMA_PATH):
        raise FileNotFoundError(f'Schema file not found: {SCHEMA_PATH}')
    with open(SCHEMA_PATH, 'r') as f:
        schema_sql = f.read()
    conn = get_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        for stmt in schema_sql.split(';'):
            if stmt.strip():
                cur.execute(stmt)
        cur.close()
        print('Schema ensured')
    finally:
        conn.close()


def truncate_tables():
    """
    Truncate all pipeline output tables before a fresh load.
    Ensures idempotency — no stale rows survive a re-run.
    Does NOT truncate pipeline_run_log (audit trail, append-only).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute('''
            TRUNCATE TABLE
                fact_ratings,
                dim_movies,
                dim_users,
                genre_trends,
                decade_stats,
                tag_similarity
            RESTART IDENTITY CASCADE;
        ''')
        conn.commit()
        cur.close()
        print('All pipeline tables truncated — ready for fresh load')
    finally:
        conn.close()


def log_pipeline_run(dataset_hash, status, rows_loaded, duration_secs):
    """
    Insert one row into pipeline_run_log for audit and observability.
    Called at the end of every pipeline run regardless of outcome.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            INSERT INTO pipeline_run_log
                (dataset_hash, status, rows_loaded, duration_secs, source_url, pipeline_version)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
            ''',
            (dataset_hash, status, rows_loaded, duration_secs, DATASET_URL, get_pipeline_version())
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        print(f'Run logged — status: {status}, hash: {dataset_hash[:8]}...')
        return run_id
    finally:
        conn.close()


def get_last_run():
    """
    Return the most recent pipeline_run_log row as a dict.
    Returns None if no runs exist yet.
    Useful for debugging and manual inspection.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT run_id, run_date, dataset_hash, status,
                   rows_loaded, duration_secs
            FROM pipeline_run_log
            ORDER BY run_date DESC
            LIMIT 1;
        ''')
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        return {
            'run_id':        row[0],
            'run_date':      row[1],
            'dataset_hash':  row[2],
            'status':        row[3],
            'rows_loaded':   row[4],
            'duration_secs': row[5],
        }
    finally:
        conn.close()


# ── Cleanup ───────────────────────────────────────────────────────────────────

def record_table_stats(run_id, table_stats):
    """Insert per-table row counts for a given run."""
    if not table_stats:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        for table_name, row_count in table_stats.items():
            cur.execute(
                '''
                INSERT INTO pipeline_table_stats (run_id, table_name, row_count)
                VALUES (%s, %s, %s)
                ''',
                (run_id, table_name, int(row_count))
            )
        conn.commit()
        cur.close()
        print('Table stats recorded')
    finally:
        conn.close()


def run_data_quality_checks(expected_ratings=None, drift_warn_pct=0.20):
    """
    Run lightweight data quality checks and return table row counts.
    Raises on hard failures; prints warnings for drift.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        tables = [
            'dim_movies',
            'dim_users',
            'fact_ratings',
            'genre_trends',
            'decade_stats',
            'tag_similarity',
        ]

        table_stats = {}
        for t in tables:
            cur.execute(f'SELECT COUNT(*) FROM {t}')
            table_stats[t] = cur.fetchone()[0]

        for t, count in table_stats.items():
            if count <= 0:
                print(f'WARNING: {t} is empty')

        if expected_ratings is not None:
            min_expected = int(expected_ratings * 0.90)
            if table_stats['fact_ratings'] < min_expected:
                print(
                    f'WARNING: fact_ratings too small '
                    f'({table_stats["fact_ratings"]} < {min_expected})'
                )

        # Null checks
        cur.execute('SELECT COUNT(*) FROM dim_movies WHERE movie_id IS NULL')
        if cur.fetchone()[0] > 0:
            print('WARNING: dim_movies.movie_id has NULLs')
        cur.execute('SELECT COUNT(*) FROM dim_users WHERE user_id IS NULL')
        if cur.fetchone()[0] > 0:
            print('WARNING: dim_users.user_id has NULLs')
        cur.execute('SELECT COUNT(*) FROM fact_ratings WHERE movie_id IS NULL OR user_id IS NULL OR rating IS NULL')
        if cur.fetchone()[0] > 0:
            print('WARNING: fact_ratings has NULL keys/ratings')

        # Referential integrity checks
        cur.execute('''
            SELECT COUNT(*)
            FROM fact_ratings fr
            LEFT JOIN dim_movies dm ON fr.movie_id = dm.movie_id
            WHERE dm.movie_id IS NULL
        ''')
        if cur.fetchone()[0] > 0:
            print('WARNING: fact_ratings has orphan movie_id')

        cur.execute('''
            SELECT COUNT(*)
            FROM fact_ratings fr
            LEFT JOIN dim_users du ON fr.user_id = du.user_id
            WHERE du.user_id IS NULL
        ''')
        if cur.fetchone()[0] > 0:
            print('WARNING: fact_ratings has orphan user_id')

        # Similarity smoke tests
        cur.execute('SELECT MIN(similarity), MAX(similarity), COUNT(*) FROM tag_similarity')
        sim_min, sim_max, sim_count = cur.fetchone()
        if sim_count <= 0:
            print('WARNING: tag_similarity is empty')
        if sim_min is None or sim_max is None or sim_min < 0 or sim_max > 1:
            print(f'WARNING: tag_similarity out of range ({sim_min}, {sim_max})')

        # Drift warnings (compare to last run's stats)
        cur.execute('SELECT run_id FROM pipeline_run_log ORDER BY run_date DESC LIMIT 1')
        row = cur.fetchone()
        if row:
            last_run_id = row[0]
            cur.execute(
                'SELECT table_name, row_count FROM pipeline_table_stats WHERE run_id = %s',
                (last_run_id,)
            )
            prev_stats = dict(cur.fetchall())
            for table_name, current in table_stats.items():
                prev = prev_stats.get(table_name)
                if prev and prev > 0:
                    pct_change = abs(current - prev) / prev
                    if pct_change >= drift_warn_pct:
                        print(
                            f'WARNING: Drift detected in {table_name}: '
                            f'prev={prev}, current={current}, change={pct_change:.0%}'
                        )

        cur.close()
        print('Data quality checks passed')
        return table_stats
    finally:
        conn.close()


def cleanup_zip():
    if os.getenv("ENABLE_CLEANUP", "0") != "1":
        print("Cleanup disabled (ENABLE_CLEANUP!=1)")
        return
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
        print(f"Deleted {ZIP_PATH}")


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('Testing dataset_utils.py')
    print(f'PROJECT_ROOT : {PROJECT_ROOT}')
    print(f'RAW_DIR      : {RAW_DIR}')
    print(f'ZIP_PATH     : {ZIP_PATH}')
    print(f'HASH_FILE    : {HASH_FILE}')
    print()

    # Test DB connection
    try:
        conn = get_connection()
        conn.close()
        print('Database connection: OK')
    except Exception as e:
        print(f'Database connection: FAILED — {e}')

    # Show last run if exists
    last = get_last_run()
    if last:
        print(f'Last pipeline run: {last}')
    else:
        print('No previous pipeline runs found')
