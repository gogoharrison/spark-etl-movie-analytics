# jobs/spark_session.py
#
# Spark is running in local mode inside Docker on a machine with 8 GB total
# RAM and 5 GB allocated to Docker.  Memory budget breakdown:
#
#   Postgres          ~200 MB
#   Airflow scheduler ~300 MB
#   Airflow webserver ~300 MB
#   Metabase          ~500 MB
#   ─────────────────────────
#   Remaining for Spark  ~3.7 GB (be conservative — leave OS headroom)
#
# In local[*] mode the executor runs IN the same JVM as the driver, so
# spark.executor.memory has no practical effect.  Only spark.driver.memory
# matters.  We set it to 1500m and keep maxResultSize tight.
#
# spark.sql.shuffle.partitions = 2 (default is 200, absurdly high for local).

import os
import sys
import logging
from pyspark.sql import SparkSession


def get_spark(app_name='MovieAnalytics'):

    # Guard: Spark needs Java
    if not os.environ.get('JAVA_HOME'):
        raise EnvironmentError(
            'JAVA_HOME is not set. Spark requires Java 11+. '
            'Install from https://adoptium.net and set JAVA_HOME.'
        )

    # Windows-specific Hadoop shim (no-op inside Docker/Linux)
    if os.name == 'nt':
        os.environ.setdefault('HADOOP_HOME',      r'C:\hadoop')
        os.environ.setdefault('HADOOP_USER_NAME', 'spark')

    # Lock Spark to the active Python interpreter so the driver and worker
    # always use the same binary (critical inside a venv or Docker image).
    os.environ['PYSPARK_PYTHON']        = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Suppress noisy shutdown-hook log lines on Windows
    logging.getLogger('org.apache.spark').setLevel(logging.CRITICAL)

    spark = (
        SparkSession.builder
        .appName(app_name)                                          # pyright: ignore[reportAttributeAccessIssue]
        .master('local[2]')                                         # 2 cores — safe for shared Docker host
        .config('spark.driver.memory',          '1500m')            # ↓ from 2g — fits 5 GB Docker budget
        .config('spark.driver.maxResultSize',   '512m')             # ↓ from 1g — limits collect() blowups
        .config('spark.executor.memory',        '1500m')            # no effect in local mode; documents intent
        .config('spark.sql.shuffle.partitions', '2')                # ↓ from 4 — right-sized for local[2]
        .config('spark.sql.session.timeZone',   'UTC')
        .config('spark.jars.packages',          'org.postgresql:postgresql:42.7.1')
        .config('spark.jars.ivy',               '/tmp/.ivy2')       # writable cache dir inside container
        .config(
            'spark.driver.extraJavaOptions',
            '-Dlog4j.logger.org.apache.spark.SparkEnv=OFF '
            '-Dlog4j.logger.org.apache.spark.util.ShutdownHookManager=OFF',
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')
    return spark


if __name__ == '__main__':
    spark = get_spark('SparkSmokeTest')
    print('Spark version :', spark.version)
    print('Python binary :', os.environ['PYSPARK_PYTHON'])
    print('Spark started successfully.')
    spark.stop()