# ==============================================================================
# AgriPulse India — Snowflake Kafka Connect Dockerfile
# ==============================================================================
# Uses Confluent Platform 7.6.0 Kafka Connect runtime and installs
# the official Snowflake Kafka Sink Connector (v2.2.3 or latest).

FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# Install official Snowflake Kafka Connector plugin from Confluent Hub
RUN confluent-hub install --no-prompt snowflakeinc/snowflake-kafka-connector:2.2.3

USER appuser
