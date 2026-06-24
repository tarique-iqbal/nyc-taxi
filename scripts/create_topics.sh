#!/usr/bin/env bash
# scripts/create_topics.sh
#
# Waits for the Kafka broker to be ready, then creates the pipeline topics
# by executing deployments/docker/kafka/kafka-topics.sh inside the
# Kafka container. Idempotent -- existing topics are left unchanged.
#
# Run from the repository root:
#   bash scripts/create_topics.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

[[ -f ".env" ]] && { set -a; source .env; set +a; }

KAFKA_CONTAINER="${KAFKA_CONTAINER:-nyc_taxi_kafka}"
INTERNAL_BOOTSTRAP="kafka:29092"
TIMEOUT=60

# Wait for Kafka broker
info "Waiting for Kafka broker..."

elapsed=0
until docker compose exec -T kafka \
        kafka-broker-api-versions --bootstrap-server "$INTERNAL_BOOTSTRAP" \
        >/dev/null 2>&1; do
    if [[ $elapsed -ge $TIMEOUT ]]; then
        die "Kafka did not become ready within ${TIMEOUT}s -- is 'make up' running?"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

success "Kafka broker is ready"

# Copy and execute topic creation script inside the container
info "Creating Kafka topics..."

TOPIC_SCRIPT="deployments/docker/kafka/kafka-topics.sh"
[[ -f "$TOPIC_SCRIPT" ]] || die "Topic script not found: $TOPIC_SCRIPT"

docker compose exec -T kafka bash < "$TOPIC_SCRIPT"

success "Topic creation complete"

# List all topics
info "Current topics on broker:"
docker compose exec -T kafka \
    kafka-topics --bootstrap-server "$INTERNAL_BOOTSTRAP" --list \
    | grep -v "^__" \
    | sed 's/^/  /'
