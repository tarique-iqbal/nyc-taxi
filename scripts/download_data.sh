#!/usr/bin/env bash
# scripts/download_data.sh
#
# Downloads the NYC Yellow Taxi trip Parquet file for January 2024.
# Source: NYC Taxi & Limousine Commission open data.
#
# Run from the repository root:
#   bash scripts/download_data.sh
#
# The destination path is read from PARQUET_FILE_PATH in .env
# (default: data/raw/yellow_tripdata_2024-01.parquet).

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

DATA_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
DEST="${PARQUET_FILE_PATH:-data/raw/yellow_tripdata_2024-01.parquet}"

mkdir -p "$(dirname "$DEST")"
mkdir -p data/rejected

# Skip if already present
if [[ -f "$DEST" ]]; then
    SIZE=$(du -sh "$DEST" | cut -f1)
    success "Already exists: $DEST ($SIZE) -- skipping download"
    exit 0
fi

# Download
info "Downloading NYC Yellow Taxi data (Jan 2024)..."
info "Source: $DATA_URL"
info "Dest:   $DEST"

if command -v curl >/dev/null 2>&1; then
    curl -L --progress-bar --retry 3 --retry-delay 5 -o "$DEST" "$DATA_URL" \
        || die "curl download failed"
elif command -v wget >/dev/null 2>&1; then
    wget --show-progress --tries=3 -O "$DEST" "$DATA_URL" \
        || die "wget download failed"
else
    die "Neither curl nor wget found -- install one to download data"
fi

SIZE=$(du -sh "$DEST" | cut -f1)
success "Downloaded: $DEST ($SIZE)"

# Quick validation
info "Validating Parquet file..."
python3 -c "
import pyarrow.parquet as pq
import sys
try:
    meta = pq.read_metadata('$DEST')
    rows = meta.num_rows
    print(f'  Rows: {rows:,}')
    print(f'  Row groups: {meta.num_row_groups}')
    print(f'  Columns: {meta.num_columns}')
except Exception as e:
    print(f'Validation failed: {e}', file=sys.stderr)
    sys.exit(1)
" || die "Parquet validation failed -- file may be corrupt"

success "Parquet file valid"
