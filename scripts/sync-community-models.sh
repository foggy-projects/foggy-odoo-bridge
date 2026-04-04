#!/usr/bin/env bash
# sync-community-models.sh — Pull community TM/QM models from foggy-model-registry.
#
# This script is hardcoded to community edition only.
# It NEVER pulls pro bundles — use foggy-odoo-bridge-pro for pro models.
#
# Usage:
#   ./scripts/sync-community-models.sh [OPTIONS]
#
# Options:
#   --registry <path|url>   Registry data path or HTTP URL (default: ../foggy-model-registry/data)
#   --channel  <name>       Channel: stable | beta (default: stable)
#   --dry-run               Show what would change without writing files
#
# Requires: Python 3.10+

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Fixed: community only
EDITION="community"
PACKAGE="foggy.odoo.community"

# Configurable defaults
REGISTRY="${PROJECT_ROOT}/../foggy-model-registry/data"
CHANNEL="stable"
DRY_RUN=false

# Paths
MODEL_DIR="$PROJECT_ROOT/foggy_mcp/setup/foggy-models"
LOCK_FILE="$MODEL_DIR/models.lock.json"
REGISTRY_PULL_SCRIPT="$PROJECT_ROOT/../foggy-model-registry/scripts/pull.py"

# ---------- arg parsing ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry) REGISTRY="$2"; shift 2 ;;
    --channel)  CHANNEL="$2";  shift 2 ;;
    --dry-run)  DRY_RUN=true;  shift ;;
    --edition|--package|--key)
      echo "ERROR: This script only supports community edition." >&2
      echo "  --edition, --package, --key are not allowed." >&2
      exit 1
      ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

echo "=== sync-community-models ==="
echo "  registry : $REGISTRY"
echo "  package  : $PACKAGE"
echo "  channel  : $CHANNEL"
echo "  edition  : $EDITION (hardcoded)"

# ---------- pre-flight ----------
if [[ ! -f "$REGISTRY_PULL_SCRIPT" ]]; then
  echo "ERROR: Registry pull script not found at $REGISTRY_PULL_SCRIPT" >&2
  echo "  Ensure foggy-model-registry is checked out alongside this repo." >&2
  exit 1
fi

# ---------- staging ----------
STAGING_DIR=$(mktemp -d)
trap 'rm -rf "$STAGING_DIR"' EXIT

echo ""
echo "Pulling community bundle to staging..."

PULL_ARGS=(
  --registry "$REGISTRY"
  --package "$PACKAGE"
  --channel "$CHANNEL"
  --edition "$EDITION"
  --output "$STAGING_DIR"
)

python3 "$REGISTRY_PULL_SCRIPT" "${PULL_ARGS[@]}"

# pull.py writes models.lock.json in the output dir
STAGING_LOCK="$STAGING_DIR/models.lock.json"
if [[ ! -f "$STAGING_LOCK" ]]; then
  echo "ERROR: pull.py did not produce models.lock.json" >&2
  exit 1
fi

# ---------- pro content guard ----------
PRO_MODELS=(
  "OdooMrpProductionModel"
  "OdooProjectTaskModel"
  "OdooMrpProductionQueryModel"
  "OdooProjectTaskQueryModel"
)

for model in "${PRO_MODELS[@]}"; do
  if find "$STAGING_DIR" -name "${model}.*" 2>/dev/null | grep -q .; then
    echo "ERROR: Pro model detected in community bundle: $model" >&2
    echo "  The registry community bundle should not contain pro models." >&2
    exit 1
  fi
done

# ---------- diff / apply ----------
echo ""
if $DRY_RUN; then
  echo "[dry-run] Would update model directory: $MODEL_DIR"
  echo "[dry-run] Would update lock file:       $LOCK_FILE"
  echo ""
  echo "Staged lock content:"
  cat "$STAGING_LOCK"
  exit 0
fi

# Clear existing model subdirectories and replace
echo "Syncing model directory..."
rm -rf "$MODEL_DIR/model" "$MODEL_DIR/query"
mkdir -p "$MODEL_DIR/model" "$MODEL_DIR/query"

# Copy model files (not lock file) from staging
find "$STAGING_DIR" -maxdepth 1 -mindepth 1 ! -name "models.lock.json" -exec cp -r {} "$MODEL_DIR/" \;

# Compute content-level checksum (deterministic, based on file contents)
CONTENT_CHECKSUM=$(
  cd "$MODEL_DIR" && \
  find . -type f ! -name "GENERATED.md" ! -name "models.lock.json" | LC_ALL=C sort | \
  while IFS= read -r f; do
    sha256sum "$f"
  done | sha256sum | awk '{print "sha256:" $1}'
)

# Augment lock file with content checksum and write to final location
python3 -c "
import json, sys
lock = json.load(open(sys.argv[1], encoding='utf-8'))
lock['content_checksum'] = sys.argv[2]
with open(sys.argv[3], 'w', encoding='utf-8') as f:
    json.dump(lock, f, indent=2, ensure_ascii=False)
    f.write('\n')
" "$STAGING_LOCK" "$CONTENT_CHECKSUM" "$LOCK_FILE"

# Re-create GENERATED marker
cat > "$MODEL_DIR/GENERATED.md" << 'MARKER'
本目录由 foggy-model-registry community bundle 同步生成，禁止手工修改。
使用 scripts/sync-community-models.sh 更新。
MARKER

echo ""
echo "Sync complete."
echo "  Models: $MODEL_DIR"
echo "  Lock:   $LOCK_FILE"
echo ""
cat "$LOCK_FILE"
