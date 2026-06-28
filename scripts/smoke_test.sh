#!/usr/bin/env bash
# scripts/smoke_test.sh
#
# Runs a series of checks to verify the pipeline is operational:
#   1. Health endpoint responds with status=ok
#   2. ClickHouse is reachable and schema is applied
#   3. Kafka topics exist
#   4. Zone lookup CSV is loadable
#   5. Domain pipeline processes a sample row without errors
#
# Run from the repository root:
#   bash scripts/smoke_test.sh
#
# Exits 0 if all checks pass, 1 otherwise.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
FAIL=0

pass() {
    echo -e "  ${GREEN}PASS${NC}  $*"
    PASS=$((PASS + 1))
}

fail() {
    echo -e "  ${RED}FAIL${NC}  $*"
    FAIL=$((FAIL + 1))
}

[[ -f ".env" ]] && {
    set -a
    source .env
    set +a
}

HEALTH_PORT="${HEALTH_PORT:-8000}"

echo -e "${BOLD}NYC Taxi ETL -- Smoke Test${NC}"
echo "--------------------------"

# 1. Health endpoint
echo ""
echo -e "${BOLD}[1] Health endpoint${NC}"

if command -v curl >/dev/null 2>&1; then
    response=$(curl -sf --max-time 5 "http://localhost:${HEALTH_PORT}/health" 2>/dev/null || true)

    if [[ -z "$response" ]]; then
        fail "Health server not reachable at :${HEALTH_PORT} (is health_server running?)"
    else
        status=$(echo "$response" | python3 -c \
            "import json,sys; print(json.load(sys.stdin).get('status','unknown'))" \
            2>/dev/null || echo "parse_error")

        if [[ "$status" == "ok" ]]; then
            pass "Health status: ok"
        elif [[ "$status" == "degraded" ]]; then
            fail "Health status: degraded -- $response"
        else
            fail "Health status: $status"
        fi
    fi
else
    fail "curl not found"
fi

# 2. ClickHouse
echo ""
echo -e "${BOLD}[2] ClickHouse${NC}"

if python3 - <<'PYEOF'
import sys
from etl.config.settings import get_settings
from etl.infrastructure.clickhouse.client import ClickHouseClient

settings = get_settings()

try:
    client = ClickHouseClient(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        database=settings.clickhouse.database,
        user=settings.clickhouse.user,
        password=settings.clickhouse.password,
    )

    client.ping()
    print("  \033[0;32mPASS\033[0m  ClickHouse ping OK")

    tables = client.execute("SHOW TABLES FROM taxi")
    table_names = [t[0] for t in tables]

    required = {"trips", "schema_migrations"}
    missing = required - set(table_names)

    if missing:
        print(f"  \033[0;31mFAIL\033[0m  Missing tables: {missing}")
        sys.exit(1)

    print("  \033[0;32mPASS\033[0m  Required tables present")

    row_count = client.execute(
        "SELECT count() FROM taxi.trips FINAL"
    )[0][0]

    print(f"  \033[0;32mPASS\033[0m  trips table accessible ({row_count:,} rows)")

    client.close()

except Exception as exc:
    print(f"  \033[0;31mFAIL\033[0m  ClickHouse error: {exc}")
    sys.exit(1)
PYEOF
then
    pass "ClickHouse checks passed"
else
    fail "ClickHouse checks failed"
fi

# 3. Kafka
echo ""
echo -e "${BOLD}[3] Kafka topics${NC}"

if python3 - <<'PYEOF'
import sys
from confluent_kafka.admin import AdminClient
from etl.config.settings import get_settings

settings = get_settings()

required = {
    settings.kafka.topic,
    settings.kafka.dlq_topic,
}

try:
    admin = AdminClient(
        {"bootstrap.servers": settings.kafka.bootstrap_servers}
    )

    metadata = admin.list_topics(timeout=5)

    existing = set(metadata.topics.keys())

    missing = required - existing

    if missing:
        print(f"  \033[0;31mFAIL\033[0m  Missing topics: {missing}")
        sys.exit(1)

    for topic in sorted(required):
        partitions = len(metadata.topics[topic].partitions)
        print(
            f"  \033[0;32mPASS\033[0m  {topic} ({partitions} partition(s))"
        )

except Exception as exc:
    print(f"  \033[0;31mFAIL\033[0m  Kafka error: {exc}")
    sys.exit(1)
PYEOF
then
    pass "Kafka checks passed"
else
    fail "Kafka checks failed"
fi

# 4. Zone lookup
echo ""
echo -e "${BOLD}[4] Zone lookup${NC}"

if python3 - <<'PYEOF'
import sys

from etl.config.settings import get_settings
from etl.infrastructure.storage.zone_lookup import CsvZoneRepository

settings = get_settings()

path = settings.etl.zone_lookup_path

if not path.exists():
    print(f"  \033[0;31mFAIL\033[0m  Zone CSV not found: {path}")
    sys.exit(1)

try:
    repo = CsvZoneRepository(path=path)
    repo.load()

    zone = repo.get_by_id(132)

    if zone.zone != "JFK Airport":
        print(
            f"  \033[0;31mFAIL\033[0m  Unexpected zone: {zone.zone}"
        )
        sys.exit(1)

    print(
        f"  \033[0;32mPASS\033[0m  Zone lookup OK "
        f"(132 = {zone.zone}, {zone.borough})"
    )

except Exception as exc:
    print(f"  \033[0;31mFAIL\033[0m  Zone lookup error: {exc}")
    sys.exit(1)
PYEOF
then
    pass "Zone lookup checks passed"
else
    fail "Zone lookup checks failed"
fi

# 5. Domain pipeline
echo ""
echo -e "${BOLD}[5] Domain pipeline${NC}"

if python3 - <<'PYEOF'
import sys
from datetime import datetime, timezone

from etl.config.settings import get_settings
from etl.domain.trip.services import TripDomainService
from etl.infrastructure.storage.zone_lookup import CsvZoneRepository

settings = get_settings()

raw_row = {
    "vendor_id": "Creative Mobile Technologies",
    "pickup_datetime": datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
    "dropoff_datetime": datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
    "passenger_count": 2,
    "trip_distance": 3.5,
    "rate_code": "Standard",
    "store_and_fwd_flag": "No",
    "pickup_location_id": 161,
    "dropoff_location_id": 236,
    "payment_type": "Credit card",
    "fare_amount": 12.5,
    "extra": 1.0,
    "mta_tax": 0.5,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 0.3,
    "congestion_surcharge": 2.5,
    "airport_fee": 0.0,
    "total_amount": 19.8,
}

try:
    repo = CsvZoneRepository(path=settings.etl.zone_lookup_path)
    repo.load()

    service = TripDomainService(zone_repository=repo)

    valid_trips, invalid_events = service.process_batch(
        raw_rows=[raw_row],
        batch_id="smoke-test",
        source_file="smoke_test.sh",
    )

    if invalid_events:
        print(
            f"  \033[0;31mFAIL\033[0m  "
            f"{invalid_events[0].error_message}"
        )
        sys.exit(1)

    trip = valid_trips[0]

    print("  \033[0;32mPASS\033[0m  Domain pipeline produced valid Trip")
    print(f"  \033[0;32mPASS\033[0m  trip_id: {trip.trip_id[:16]}...")
    print(
        f"  \033[0;32mPASS\033[0m  "
        f"pickup_zone: {trip.pickup_zone}, "
        f"borough: {trip.pickup_borough}"
    )

except Exception as exc:
    print(f"  \033[0;31mFAIL\033[0m  Domain pipeline error: {exc}")
    sys.exit(1)
PYEOF
then
    pass "Domain pipeline checks passed"
else
    fail "Domain pipeline checks failed"
fi

# Summary
echo ""
echo "----------------------------"

if [[ $FAIL -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}All ${PASS} checks passed.${NC}"
    exit 0
else
    echo -e "${RED}${BOLD}${FAIL} check(s) failed, ${PASS} passed.${NC}"
    echo
    echo "Troubleshooting:"
    echo "  Infrastructure down?  make up"
    echo "  Schema missing?       make schema"
    echo "  Topics missing?       make topics"
    echo "  Data missing?         make data"
    exit 1
fi
