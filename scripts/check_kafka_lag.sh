#!/usr/bin/env bash
# scripts/check_kafka_lag.sh
#
# Reports current Kafka consumer group lag for the ETL pipeline.
# Queries both committed offsets and high-water marks, printing
# per-partition lag and a total.
#
# Run from the repository root:
#   bash scripts/check_kafka_lag.sh
#
# Options:
#   --watch     Re-run every 5 seconds (like watch mode)
#   --alert N   Exit with code 1 if total lag exceeds N (for CI/alerting)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

[[ -f ".env" ]] && { set -a; source .env; set +a; }

WATCH=false
ALERT_THRESHOLD=""

for arg in "$@"; do
    case "$arg" in
        --watch)      WATCH=true ;;
        --alert=*)    ALERT_THRESHOLD="${arg#--alert=}" ;;
    esac
done

_run_lag_check() {
    echo -e "${BOLD}$(date '+%Y-%m-%d %H:%M:%S') -- Kafka Consumer Group Lag${NC}"
    echo "----------------------------------------------"

    python3 - << PYEOF
import sys
from etl.config.settings import get_settings
from etl.infrastructure.monitoring.kafka_lag import KafkaLagMonitor

settings = get_settings()

try:
    monitor = KafkaLagMonitor(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_group_id,
        topic=settings.kafka.topic,
    )
    total = monitor.poll()

    print(f"  Group:   {settings.kafka.consumer_group_id}")
    print(f"  Topic:   {settings.kafka.topic}")
    print()

    for p in monitor.partition_lags():
        lag_label = f"\033[0;31m{p.lag:>8}\033[0m" if p.lag > 0 else f"\033[0;32m{p.lag:>8}\033[0m"
        print(f"  Partition {p.partition}:  offset={p.committed_offset:>10}  high={p.current_offset:>10}  lag={lag_label}")

    print()
    colour = "\033[0;31m" if total > 0 else "\033[0;32m"
    print(f"  Total lag: {colour}{total}\033[0m")

    monitor.close()
    sys.exit(0)

except Exception as exc:
    print(f"  Error: {exc}", file=sys.stderr)
    sys.exit(1)
PYEOF

    local exit_code=$?

    if [[ -n "$ALERT_THRESHOLD" ]]; then
        local total
        total=$(python3 -c "
from etl.config.settings import get_settings
from etl.infrastructure.monitoring.kafka_lag import KafkaLagMonitor
s = get_settings()
m = KafkaLagMonitor(s.kafka.bootstrap_servers, s.kafka.consumer_group_id, s.kafka.topic)
m.poll()
print(m.total_lag())
m.close()
" 2>/dev/null || echo "0")

        if [[ "$total" -gt "$ALERT_THRESHOLD" ]]; then
            warn "Lag $total exceeds alert threshold $ALERT_THRESHOLD"
            return 1
        fi
    fi

    return $exit_code
}

if [[ "$WATCH" == "true" ]]; then
    info "Watch mode -- refreshing every 5 seconds (Ctrl+C to stop)"
    while true; do
        clear
        _run_lag_check || true
        sleep 5
    done
else
    _run_lag_check
fi
