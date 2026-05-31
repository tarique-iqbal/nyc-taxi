#!/usr/bin/env bash
# deployments/docker/kafka/kafka-topics.sh
#
# Creates the Kafka topics required by the NYC Taxi ETL pipeline.
#
# Topics:
#   nyc-taxi-trips      - main ingestion topic
#   nyc-taxi-trips-dlq  - dead letter queue
#
# Environment:
#   KAFKA_BOOTSTRAP_SERVERS (optional)
#   Defaults to: kafka:9092

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "Waiting for Kafka broker..."

# Wait until Kafka is accepting connections.
# Uses first broker if multiple are supplied.
PRIMARY_BROKER="$(echo "$BOOTSTRAP" | cut -d',' -f1)"

cub kafka-ready \
    -b "$PRIMARY_BROKER" \
    1 \
    30

echo "Kafka is ready."
echo

declare -A TOPICS

TOPICS["nyc-taxi-trips"]="--partitions 4 --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --config compression.type=lz4 \
    --config max.message.bytes=10485760"

TOPICS["nyc-taxi-trips-dlq"]="--partitions 1 --replication-factor 1 \
    --config retention.ms=1209600000 \
    --config cleanup.policy=delete \
    --config compression.type=lz4 \
    --config max.message.bytes=10485760"

echo "Bootstrap servers: $BOOTSTRAP"
echo "Creating Kafka topics..."

for topic in "${!TOPICS[@]}"; do
    # shellcheck disable=SC2086
    kafka-topics \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --if-not-exists \
        --topic "$topic" \
        ${TOPICS[$topic]}

    echo "  [ready] $topic"
done

echo "Final topic list:"

kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --list |
    sort |
    sed 's/^/  /'
