#!/usr/bin/env bash
# check-no-pro-content.sh — Ensure community repo does not contain pro-only models.
#
# Usage:
#   ./scripts/check-no-pro-content.sh
#
# Exit codes:
#   0  no pro content found
#   1  pro content detected
#
# This script scans the community model directory for known pro-only model
# files. If any are found, it exits with non-zero to prevent accidental
# pro content leakage into the public community repository.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODEL_DIR="$PROJECT_ROOT/foggy_mcp/setup/foggy-models"

# Known pro-only model files (TM and QM)
PRO_ONLY_FILES=(
  "model/OdooMrpProductionModel.tm"
  "model/OdooProjectTaskModel.tm"
  "query/OdooMrpProductionQueryModel.qm"
  "query/OdooProjectTaskQueryModel.qm"
)

echo "=== check-no-pro-content ==="
echo "  Model dir: $MODEL_DIR"
echo ""

FOUND_PRO=false

for f in "${PRO_ONLY_FILES[@]}"; do
  if [[ -f "$MODEL_DIR/$f" ]]; then
    echo "FAIL: Pro-only model found: $f" >&2
    FOUND_PRO=true
  fi
done

# Also scan for any file whose name contains known pro module prefixes
# This catches future pro models that aren't in the explicit list
while IFS= read -r -d '' file; do
  rel="${file#"$MODEL_DIR/"}"
  basename="$(basename "$file")"
  case "$basename" in
    *MrpProduction*|*ProjectTask*|*MrpBom*|*MrpWorkorder*)
      echo "FAIL: Suspected pro-only model found: $rel" >&2
      FOUND_PRO=true
      ;;
  esac
done < <(find "$MODEL_DIR/model" "$MODEL_DIR/query" -type f \( -name "*.tm" -o -name "*.qm" \) -print0 2>/dev/null || true)

if $FOUND_PRO; then
  echo "" >&2
  echo "Pro content detected in community model directory." >&2
  echo "This repository must NOT contain pro-only models." >&2
  echo "Run scripts/sync-community-models.sh to restore community-only content." >&2
  exit 1
fi

echo "OK: No pro content detected."
exit 0
