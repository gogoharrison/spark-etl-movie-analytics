# jobs/spark_session.py
import os
import sys
import logging
from pyspark.sql import SparkSession

def get_spark(app_name='MovieAnalytics'):

    # Guard: Spark needs Java
    if not os.environ.get('JAVA_HOME'):
        raise EnvironmentError(
            "JAVA_HOME is not set. Spark requires Java 11+. "
            "Install from https://adoptium.net and set JAVA_HOME."
        )

    # Windows-specific env setup
    if os.name == 'nt':  # Windows only
        os.environ['HADOOP_HOME']      = r'C:\hadoop'
        os.environ['HADOOP_USER_NAME'] = 'spark'

    # Lock Spark to the active venv's Python (avoids venv/system mismatch)
    os.environ['PYSPARK_PYTHON']        = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

 # Suppress Windows shutdown hook noise
    logging.getLogger('org.apache.spark').setLevel(logging.CRITICAL)

    spark = (
        SparkSession.builder
        .appName(app_name)  # pyright: ignore[reportAttributeAccessIssue]
        .master('local[2]')  # Limit to 2 cores inside Docker
        .config('spark.driver.memory',              '2g')
        .config('spark.executor.memory',            '2g')
        .config('spark.driver.maxResultSize',       '1g')
        .config('spark.sql.shuffle.partitions',     '4')
        .config('spark.sql.session.timeZone',       'UTC')
        .config('spark.jars.packages',              'org.postgresql:postgresql:42.7.1')
        .config('spark.jars.ivy',                   '/tmp/.ivy2')
        .config(
            'spark.driver.extraJavaOptions',
            '-Dlog4j.logger.org.apache.spark.SparkEnv=OFF '
            '-Dlog4j.logger.org.apache.spark.util.ShutdownHookManager=OFF'
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')
    return spark


if __name__ == '__main__':
    spark = get_spark()
    print('Spark version:  ', spark.version)
    print('Python used:    ', os.environ['PYSPARK_PYTHON'])
    print('Spark started successfully!')
    spark.stop()