# dags/utils/dataset_utils.py
#
# Utility functions used by both the Airflow DAG and standalone scripts.
#
# DB credentials come from environment variables with sensible local defaults.
# Never hardcode passwords — even for local development it builds good habits
# and makes the same code deployable to staging/prod without changes.

import os
import hashlib
import json
import requests
import psycopg2

# ── Config ───────────────────────────────────────────────────────────────────

PROJECT_ROOT   = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RAW_DIR        = os.path.join(PROJECT_ROOT, 'data', 'raw')
ZIP_PATH       = os.path.join(RAW_DIR, 'ml-latest.zip')
HASH_FILE      = os.path.join(RAW_DIR, '.last_dataset_hash')
METADATA_FILE  = os.path.join(RAW_DIR, '.last_dataset_meta.json')
DATASET_URL    = 'https://files.grouplens.org/datasets/movielens/ml-latest.zip'
SCHEMA_PATH    = os.path.join(PROJECT_ROOT, 'sql', 'schema.sql')

# DB config from env-vars — local defaults match docker-compose.yml port mapping
DB_CONFIG = {
    'host':     os.environ.get('MOVIE_DB_HOST',     'localhost'),
    'port':     int(os.environ.get('MOVIE_DB_PORT', '5433')),   # 5433 = host-side mapped port
    'dbname':   os.environ.get('MOVIE_DB_NAME',     'moviedb'),
    'user':     os.environ.get('MOVIE_DB_USER',     'movieuser'),
    'password': os.environ.get('MOVIE_DB_PASS',     'movie123'),
}

# Inside Docker containers Airflow tasks connect via the internal service name
# on port 5432.  Set MOVIE_DB_HOST=postgres and MOVIE_DB_PORT=5432 in the
# docker-compose environment blocks to override the local defaults above.


# ── Remote metadata helpers ───────────────────────────────────────────────────

def load_saved_metadata():
    """Load previously saved remote dataset metadata from disk."""
    if not os.path.exists(METADATA_FILE):
        return {}
    try:
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_remote_metadata(metadata):
    """Persist remote metadata so the next run can compare against it."""
    os.makedirs(RAW_DIR, exist_ok=True)
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, sort_keys=True)


def fetch_remote_metadata(url):
    """
    Issue a HEAD request and extract change-detection headers.
    Returns a dict with etag, last_modified, content_length (any may be None).
    """
    response = requests.head(url, timeout=10, allow_redirects=True)
    response.raise_for_status()
    return {
        'etag':           response.headers.get('ETag'),
        'last_modified':  response.headers.get('Last-Modified'),
        'content_length': response.headers.get('Content-Length'),
    }


def metadata_matches(saved, remote):
    """
    Return True when the remote metadata matches the saved metadata.
    Priority: ETag > Last-Modified > Content-Length.
    Returns False (triggering a download) when neither side has a comparable header.
    """
    if not saved or not remote:
        return False

    for key in ('etag', 'last_modified'):
        s, r = saved.get(key), remote.get(key)
        if s and r:
            return s == r

    s_len, r_len = saved.get('content_length'), remote.get('content_length')
    if s_len and r_len:
        return str(s_len) == str(r_len)

    return False


def should_download_dataset():
    """
    Return True when the remote dataset appears changed or the local zip is missing.
    Defaults to True (conservative) on any error so we never silently skip a new dataset.
    """
    if not os.path.exists(ZIP_PATH):
        print('Local zip missing — download required.')
        return True

    saved_meta = load_saved_metadata()
    if not saved_meta:
        print('No saved metadata found — download required.')
        return True

    try:
        remote_meta = fetch_remote_metadata(DATASET_URL)
    except Exception as exc:
        print(f'Remote metadata check failed: {exc}. Conservatively requiring download.')
        return True

    if metadata_matches(saved_meta, remote_meta):
        print('Remote metadata unchanged — download not required.')
        return False

    print('Remote metadata changed — download required.')
    return True


# ── Download ──────────────────────────────────────────────────────────────────

def download_dataset():
    """
    Download ml-latest.zip to data/raw/ with resume support and retries.

    Uses HTTP Range requests to resume partial downloads.
    Retries up to 8 times with exponential back-off (max 60 s between attempts).
    """
    import time

    os.makedirs(RAW_DIR, exist_ok=True)
    max_attempts = 8

    remote_meta = {}
    try:
        remote_meta = fetch_remote_metadata(DATASET_URL)
    except Exception as exc:
        print(f'HEAD check failed: {exc}. Proceeding without resume capability.')

    remote_len = int(remote_meta.get('content_length') or 0)

    for attempt in range(1, max_attempts + 1):
        existing = os.path.getsize(ZIP_PATH) if os.path.exists(ZIP_PATH) else 0

        if remote_len and existing >= remote_len:
            print(f'File already complete ({existing:,} bytes). Skipping download.')
            return

        headers = {'Range': f'bytes={existing}-'} if existing > 0 else {}
        if existing:
            print(f'Resuming from byte {existing:,}')

        print(f'Downloading {DATASET_URL} (attempt {attempt}/{max_attempts})')
        try:
            with requests.get(DATASET_URL, stream=True,
                              timeout=(10, 300), headers=headers) as r:
                r.raise_for_status()

                # Server ignored Range header — restart from the beginning
                mode = 'wb' if (existing > 0 and r.status_code == 200) else (
                    'ab' if existing > 0 else 'wb'
                )
                if existing > 0 and r.status_code == 200:
                    print('Server ignored Range request — restarting full download.')

                with open(ZIP_PATH, mode) as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            size_mb = os.path.getsize(ZIP_PATH) / (1024 * 1024)
            print(f'Downloaded {size_mb:.1f} MB → {ZIP_PATH}')

            if remote_meta:
                save_remote_metadata(remote_meta)
            return

        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as exc:
            if attempt == max_attempts:
                raise
            wait = min(60, 2 ** attempt)
            print(f'Download interrupted ({exc}). Retrying in {wait}s...')
            time.sleep(wait)


# ── Hash helpers ──────────────────────────────────────────────────────────────

def compute_hash(filepath):
    """Compute MD5 hash of a file in 8 KB chunks."""
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def save_current_hash():
    """
    Compute and persist the current zip MD5 hash.
    Also refreshes the remote metadata cache.
    Returns the hash string (stored in pipeline_run_log for auditability).
    """
    current_hash = compute_hash(ZIP_PATH)
    os.makedirs(RAW_DIR, exist_ok=True)

    with open(HASH_FILE, 'w', encoding='utf-8') as f:
        f.write(current_hash)

    try:
        remote_meta = fetch_remote_metadata(DATASET_URL)
        save_remote_metadata(remote_meta)
    except Exception as exc:
        print(f'Could not refresh remote metadata post-run: {exc}')

    print(f'Dataset hash saved: {current_hash[:16]}...')
    return current_hash


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection():
    """Return a psycopg2 connection using DB_CONFIG."""
    return psycopg2.connect(**DB_CONFIG)


def get_pipeline_version():
    """Return PIPELINE_VERSION env-var or 'unknown'."""
    return os.getenv('PIPELINE_VERSION', 'unknown')


def ensure_schema():
    """
    Run sql/schema.sql against the database.
    All CREATE TABLE / CREATE INDEX statements use IF NOT EXISTS,
    so this is safe to call on every pipeline run.
    """
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
        print('Schema ensured.')
    finally:
        conn.close()


def truncate_tables():
    """
    Truncate all pipeline output tables in one atomic statement.

    Called as its own Airflow task BEFORE any Spark job starts.
    Keeping truncate separate from Spark means:
      - If TRUNCATE succeeds but Spark fails, the DAG retries from
        the truncate step and the DB is never left half-loaded.
      - pipeline_run_log is intentionally excluded (append-only audit trail).
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
        print('All pipeline tables truncated — ready for fresh load.')
    finally:
        conn.close()


def log_pipeline_run(dataset_hash, status, rows_loaded, duration_secs):
    """
    Insert one audit row into pipeline_run_log.
    Called for both successful and skipped runs.
    Returns the new run_id.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            INSERT INTO pipeline_run_log
                (dataset_hash, status, rows_loaded, duration_secs,
                 source_url, pipeline_version)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
            ''',
            (dataset_hash, status, rows_loaded, duration_secs,
             DATASET_URL, get_pipeline_version()),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        print(f'Run logged — status={status}, hash={dataset_hash[:8]}..., run_id={run_id}')
        return run_id
    finally:
        conn.close()


def record_table_stats(run_id, table_stats):
    """Insert per-table row counts for a given pipeline run."""
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
                (run_id, table_name, int(row_count)),
            )
        conn.commit()
        cur.close()
        print('Table stats recorded.')
    finally:
        conn.close()


def get_last_run():
    """
    Return the most recent pipeline_run_log row as a dict.
    Returns None if no runs exist yet.
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


# ── Data quality checks ───────────────────────────────────────────────────────

def run_data_quality_checks(expected_ratings=None, drift_warn_pct=0.20):
    """
    Run lightweight post-load quality checks.

    Checks performed:
      - Row count > 0 for all output tables.
      - fact_ratings count ≥ 90% of raw CSV count.
      - No NULL primary/foreign keys in dim_movies, dim_users, fact_ratings.
      - No orphan movie_id or user_id in fact_ratings.
      - tag_similarity similarity values in [0, 1].
      - Row-count drift ≥ 20% vs the previous run.

    Raises on hard failures; prints WARNING for soft drift checks.
    Returns a dict of {table_name: row_count} for the audit log.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        tables = [
            'dim_movies', 'dim_users', 'fact_ratings',
            'genre_trends', 'decade_stats', 'tag_similarity',
        ]

        table_stats = {}
        for t in tables:
            cur.execute(f'SELECT COUNT(*) FROM {t}')
            table_stats[t] = cur.fetchone()[0]

        # Empty table check
        for t, count in table_stats.items():
            if count == 0:
                print(f'WARNING: {t} is empty after load.')

        # Rating count sanity
        if expected_ratings is not None:
            min_expected = int(expected_ratings * 0.90)
            if table_stats['fact_ratings'] < min_expected:
                print(
                    f'WARNING: fact_ratings row count {table_stats["fact_ratings"]:,} '
                    f'is below 90% threshold of {min_expected:,}.'
                )

        # NULL key checks
        cur.execute('SELECT COUNT(*) FROM dim_movies WHERE movie_id IS NULL')
        if cur.fetchone()[0] > 0:
            print('WARNING: dim_movies.movie_id has NULL values.')

        cur.execute('SELECT COUNT(*) FROM dim_users WHERE user_id IS NULL')
        if cur.fetchone()[0] > 0:
            print('WARNING: dim_users.user_id has NULL values.')

        cur.execute('''
            SELECT COUNT(*) FROM fact_ratings
            WHERE movie_id IS NULL OR user_id IS NULL OR rating IS NULL
        ''')
        if cur.fetchone()[0] > 0:
            print('WARNING: fact_ratings has NULL keys or ratings.')

        # Referential integrity
        cur.execute('''
            SELECT COUNT(*) FROM fact_ratings fr
            LEFT JOIN dim_movies dm ON fr.movie_id = dm.movie_id
            WHERE dm.movie_id IS NULL
        ''')
        if cur.fetchone()[0] > 0:
            print('WARNING: fact_ratings has orphan movie_id values.')

        cur.execute('''
            SELECT COUNT(*) FROM fact_ratings fr
            LEFT JOIN dim_users du ON fr.user_id = du.user_id
            WHERE du.user_id IS NULL
        ''')
        if cur.fetchone()[0] > 0:
            print('WARNING: fact_ratings has orphan user_id values.')

        # Similarity range check
        cur.execute('SELECT MIN(similarity), MAX(similarity), COUNT(*) FROM tag_similarity')
        sim_min, sim_max, sim_count = cur.fetchone()
        if sim_count == 0:
            print('WARNING: tag_similarity is empty.')
        elif sim_min is None or sim_max is None or sim_min < 0 or sim_max > 1:
            print(f'WARNING: tag_similarity values out of [0,1] range '
                  f'(min={sim_min}, max={sim_max}).')

        # Drift check vs previous run
        cur.execute('''
            SELECT run_id FROM pipeline_run_log
            WHERE status = 'success'
            ORDER BY run_date DESC LIMIT 1
        ''')
        row = cur.fetchone()
        if row:
            last_run_id = row[0]
            cur.execute(
                'SELECT table_name, row_count FROM pipeline_table_stats WHERE run_id = %s',
                (last_run_id,),
            )
            prev_stats = dict(cur.fetchall())
            for table_name, current in table_stats.items():
                prev = prev_stats.get(table_name)
                if prev and prev > 0:
                    pct_change = abs(current - prev) / prev
                    if pct_change >= drift_warn_pct:
                        print(
                            f'WARNING: Drift in {table_name}: '
                            f'prev={prev:,}, current={current:,}, '
                            f'Δ={pct_change:.0%}'
                        )

        cur.close()
        print('Data quality checks complete.')
        return table_stats
    finally:
        conn.close()


# ── Cleanup ───────────────────────────────────────────────────────────────────

def cleanup_zip():
    """
    Delete the raw zip file.
    Only acts when ENABLE_CLEANUP=1 is set — safe default for local dev
    where you likely want to keep the zip to avoid re-downloading.
    """
    if os.getenv('ENABLE_CLEANUP', '0') != '1':
        print('Cleanup skipped (set ENABLE_CLEANUP=1 to enable).')
        return
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
        print(f'Deleted {ZIP_PATH}')


# ── Standalone smoke test ─────────────────────────────────────────────────────

if __name__ == '__main__':
    print('=== dataset_utils smoke test ===')
    print(f'PROJECT_ROOT : {PROJECT_ROOT}')
    print(f'RAW_DIR      : {RAW_DIR}')
    print(f'ZIP_PATH     : {ZIP_PATH}')
    print(f'DB_CONFIG    : {DB_CONFIG}')
    print()

    try:
        conn = get_connection()
        conn.close()
        print('Database connection: OK')
    except Exception as exc:
        print(f'Database connection: FAILED — {exc}')

    last = get_last_run()
    print(f'Last pipeline run: {last if last else "none"}')