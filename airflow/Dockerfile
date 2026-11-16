FROM apache/airflow:2.9.2-python3.10

USER root

# Install system dependencies if required
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install project Python packages for ClickHouse, Kafka, and Data processing
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    pyarrow==15.0.2 \
    confluent-kafka==2.3.0 \
    requests==2.31.0 \
    pandas==2.2.1 \
    python-dotenv==1.0.1
