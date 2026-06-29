#!/usr/bin/env bash
# scripts/bootstrap.sh
#
# Full bootstrap: starts infrastructure, creates Kafka topics,
# applies ClickHouse schema, and downloads trip data.
#
# Run from the repository root:
#   bash scripts/bootstrap.sh
#
# Options:
#   --skip-data     Skip Parquet file download (if already present)
#   --skip-up       Skip docker compose up (if already running)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SKIP_DATA=false
SKIP_UP=false

for arg in "$@"; do
    case "$arg" in
        --skip-data) SKIP_DATA=true ;;
        --skip-up)   SKIP_UP=true ;;
    esac
done

# Terminal colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
header()  { echo -e "\n${BOLD}==> $*${NC}"; }

# Preflight
header "Preflight checks"

command -v docker     >/dev/null 2>&1 || die "docker not found"
command -v python3    >/dev/null 2>&1 || die "python3 not found"

[[ -f ".env" ]] || { warn ".env not found -- copying from .env.example"; cp .env.example .env; }
set -a; source .env; set +a

success "Preflight passed"

# Step 1: Start infrastructure
header "Step 1: Docker Compose"

if [[ "$SKIP_UP" == "true" ]]; then
    info "Skipping docker compose up (--skip-up)"
else
    info "Starting infrastructure containers..."
    docker compose up -d
    success "Containers started"
fi

# Step 2: Create Kafka topics
header "Step 2: Kafka topics"
bash scripts/create_topics.sh

# Step 3: Apply ClickHouse schema
header "Step 3: ClickHouse schema"
bash scripts/apply_schema.sh

# Step 4: Download data
header "Step 4: Trip data"

if [[ "$SKIP_DATA" == "true" ]]; then
    info "Skipping data download (--skip-data)"
else
    bash scripts/download_data.sh
fi

# Done
echo ""
echo -e "${GREEN}${BOLD}Bootstrap complete.${NC}"
echo ""
echo "Next steps:"
echo "  Terminal 1 (metrics):  python -m etl.entrypoints.metrics_server"
echo "  Terminal 2 (health):   python -m etl.entrypoints.health_server"
echo "  Terminal 3 (consumer): python -m etl.entrypoints.consumer"
echo "  Terminal 4 (producer): python -m etl.entrypoints.producer"
echo ""
echo "  Dashboards: http://localhost:3000  (Grafana)"
echo "  Kafka UI:   http://localhost:8080"
echo "  Health:     http://localhost:${HEALTH_PORT:-8000}/health"
