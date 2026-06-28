#!/usr/bin/env bash
# scripts/replay_dlq.sh
#
# Replays dead letter records from data/rejected/ through the domain
# pipeline. Records that now pass validation are re-published to the
# main Kafka topic. Records that still fail are re-written to the DLQ
# with an incremented retry_count.
#
# Run from the repository root:
#   bash scripts/replay_dlq.sh
#
# Options:
#   --batch-id UUID   Replay only the file for the given batch ID
#   --dry-run         Parse and report without publishing or re-dead-lettering

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

BATCH_ID=""
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --batch-id=*) BATCH_ID="${arg#--batch-id=}" ;;
        --dry-run)    DRY_RUN=true ;;
    esac
done

REJECTED_DIR="${REJECTED_DIR:-data/rejected}"

# Preflight
[[ -d "$REJECTED_DIR" ]] || die "Rejected dir not found: $REJECTED_DIR"

FILE_COUNT=$(find "$REJECTED_DIR" -name "*.jsonl.gz" | wc -l | tr -d ' ')
if [[ "$FILE_COUNT" -eq 0 ]]; then
    info "No rejected files found in $REJECTED_DIR -- nothing to replay"
    exit 0
fi

info "Found $FILE_COUNT rejected file(s) in $REJECTED_DIR"
[[ -n "$BATCH_ID" ]] && info "Filtering to batch: $BATCH_ID"
[[ "$DRY_RUN" == "true" ]] && warn "DRY RUN -- no records will be published or re-dead-lettered"

# List rejected files
echo ""
echo "  Rejected files:"
find "$REJECTED_DIR" -name "*.jsonl.gz" | sort | while read -r f; do
    count=$(python3 -c "
from etl.utils.compression import read_jsonl_gz
from pathlib import Path
print(len(read_jsonl_gz(Path('$f'))))
" 2>/dev/null || echo "?")
    echo "    $(basename "$f")  ($count records)"
done
echo ""

# Run replay
python3 - << PYEOF
import sys
from pathlib import Path
from etl.config.settings import get_settings
from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplayDlqUseCase, ReplaySource
from etl.domain.trip.services import TripDomainService
from etl.infrastructure.kafka.dead_letter_publisher import KafkaDeadLetterPublisher
from etl.infrastructure.kafka.producer import KafkaEventPublisher
from etl.infrastructure.storage.zone_lookup import CsvZoneRepository

settings = get_settings()
dry_run = "${DRY_RUN}" == "true"
batch_id = "${BATCH_ID}" or None

zone_repo = CsvZoneRepository(path=settings.etl.zone_lookup_path)
zone_repo.load()
print(f"  Zones loaded: {zone_repo.zone_count}")

domain_service = TripDomainService(zone_repository=zone_repo)
publisher = KafkaEventPublisher(
    bootstrap_servers=settings.kafka.bootstrap_servers,
    acks=settings.kafka.producer_acks,
    retries=settings.kafka.producer_retries,
)
dl_service = KafkaDeadLetterPublisher(
    bootstrap_servers=settings.kafka.bootstrap_servers,
    dlq_topic=settings.kafka.dlq_topic,
    rejected_dir=settings.etl.rejected_dir,
)

use_case = ReplayDlqUseCase(
    domain_service=domain_service,
    publisher=publisher,
    dead_letter_service=dl_service,
    topic=settings.kafka.topic,
)

command = ReplayDlqCommand(
    source=ReplaySource.DISK,
    batch_id=batch_id,
    rejected_dir=settings.etl.rejected_dir,
)

if dry_run:
    print("  [DRY RUN] Parsing records only...")
    from etl.utils.compression import iter_jsonl_gz, list_rejected_files
    files = list_rejected_files(settings.etl.rejected_dir)
    if batch_id:
        files = [f for f in files if batch_id in f.name]
    total = sum(1 for f in files for _ in iter_jsonl_gz(f))
    print(f"  Records found: {total}")
    print("  No records published (dry run)")
    sys.exit(0)

result = use_case.handle(command)

print()
print(f"  Total replayed:  {result.total_replayed}")
print(f"  Recovered:       {result.recovered}")
print(f"  Still invalid:   {result.still_invalid}")
print(f"  Recovery rate:   {result.recovery_rate:.1%}")

publisher.flush()
dl_service.flush()
PYEOF

echo ""
success "DLQ replay complete"
