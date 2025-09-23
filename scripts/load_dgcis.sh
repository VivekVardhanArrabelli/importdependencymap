#!/usr/bin/env bash
set -euo pipefail

: "${ADMIN_KEY:?Missing ADMIN_KEY}"
FILE_PATH=${1:-data/dgcis_latest.csv}
URL=${2:-http://localhost:8000}

echo "Loading DGCI&S data from $FILE_PATH"
curl -sS -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$URL/admin/etl/dgcis?file_path=$FILE_PATH" |
  python -m json.tool
