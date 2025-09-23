#!/usr/bin/env bash
set -euo pipefail

: "${ADMIN_KEY:?Missing ADMIN_KEY}"
URL=${1:-http://localhost:8000}

echo "Seeding data at $URL"
curl -sS -X POST -H "Authorization: Bearer $ADMIN_KEY" "$URL/admin/seed" |
  python -m json.tool
