FROM apache/airflow:2.10.5

USER root

# Install Java (required for PySpark) and procps (for Spark's process utils)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Set JAVA_HOME so spark_session.py's guard passes
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# PySpark + psycopg2 (required by dataset_utils.py for all non-Spark tasks)
# psycopg2-binary ships its own libpq so no system lib dependency needed
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9