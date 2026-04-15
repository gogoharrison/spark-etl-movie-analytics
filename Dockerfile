FROM apache/airflow:2.10.5

USER root

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

USER airflow

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install PySpark matching your local version
RUN pip install pyspark==3.5.0