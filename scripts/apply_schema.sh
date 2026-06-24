#!/usr/bin/env bash
# scripts/apply_schema.sh
#
# Waits for ClickHouse to be ready, then applies all pending migrations
# via SchemaManager. Safe to run multiple times -- already-applied
# migrations are skipped.
#
# Run from the repository root:
#   bash scripts/apply_schema.sh

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

CH_HOST="${CLICKHOUSE_HOST:-localhost}"
CH_PORT="${CLICKHOUSE_PORT:-9000}"
TIMEOUT=60

# Wait for ClickHouse
info "Waiting for ClickHouse at ${CH_HOST}:${CH_PORT}..."

elapsed=0
until docker compose exec -T clickhouse \
        clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
    if [[ $elapsed -ge $TIMEOUT ]]; then
        die "ClickHouse did not become ready within ${TIMEOUT}s -- is 'make up' running?"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

success "ClickHouse is ready"

# Apply migrations via SchemaManager
info "Applying migrations..."

python3 - << 'PYEOF'
import sys
from etl.config.settings import get_settings
from etl.infrastructure.clickhouse.client import ClickHouseClient
from etl.infrastructure.clickhouse.schema_manager import SchemaManager

settings = get_settings()
client = ClickHouseClient(
    host=settings.clickhouse.host,
    port=settings.clickhouse.port,
    database=settings.clickhouse.database,
    user=settings.clickhouse.user,
    password=settings.clickhouse.password,
)

try:
    client.ping()
    manager = SchemaManager(client)
    manager.apply_all()
    applied = manager.applied_versions()
    print(f"Applied migrations: {applied}")
except Exception as exc:
    print(f"Schema application failed: {exc}", file=sys.stderr)
    sys.exit(1)
finally:
    client.close()
PYEOF

success "Schema and migrations applied"

# Show applied versions
info "Applied migration versions:"
docker compose exec -T clickhouse clickhouse-client \
    --query "SELECT version, applied_at FROM taxi.schema_migrations FINAL ORDER BY version" \
    2>/dev/null | sed 's/^/  /' || warn "Could not query migration table (may not exist yet)"
