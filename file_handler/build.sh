#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$SERVER_DIR"

docker compose build 
docker compose up -d

echo
echo "Watch indexing:"
echo "  docker compose logs -f indexer"
echo
echo "Count recordings:"
echo "  docker compose exec mongo mongosh boxuploader --eval \"db.recordings.countDocuments()\""
