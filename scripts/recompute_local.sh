#!/usr/bin/env bash
set -euo pipefail

: "${ADMIN_KEY:?Missing ADMIN_KEY}"
URL=${1:-http://localhost:8000}

echo "Recomputing metrics at $URL"
curl -sS -X POST -H "Authorization: Bearer $ADMIN_KEY" "$URL/admin/recompute" |
  python -m json.tool
