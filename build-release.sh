#!/bin/bash
# Build Odoo Apps / GitHub release assets.
# Usage: ./build-release.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN=python
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=python3
  else
    echo "python or python3 is required" >&2
    exit 1
  fi
fi

VERSION="$("$PYTHON_BIN" - <<'PY'
import ast
from pathlib import Path

text = Path("foggy_mcp/__manifest__.py").read_text(encoding="utf-8")
manifest = ast.literal_eval(text.split("\n", 1)[1])
print(manifest["version"])
PY
)"
RELEASE_DIR="release"
ADDON_ZIP="foggy_mcp-${VERSION}.zip"

echo "=== Building Release Assets ==="
echo "Version: $VERSION"

# 清理
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

# 1. 打包 Odoo 插件
echo "Packing $ADDON_ZIP..."
if command -v zip >/dev/null 2>&1; then
  zip -r "$RELEASE_DIR/$ADDON_ZIP" foggy_mcp/ \
    -x "*/__pycache__/*" \
    -x "*.pyc" \
    -x "*.pyo" \
    -x "*.log" \
    -x "*/.pytest_cache/*" \
    -x "*/node_modules/*" \
    -x "*/.auth/*" \
    -x "*/playwright-output/*" \
    -x "*/playwright-report/*"
else
  "$PYTHON_BIN" - "$RELEASE_DIR/$ADDON_ZIP" <<'PY'
import sys
import zipfile
from pathlib import Path

target = Path(sys.argv[1])
root = Path("foggy_mcp")
blocked_parts = {
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    ".auth",
    "playwright-output",
    "playwright-report",
}
blocked_suffixes = {".pyc", ".pyo", ".log"}

with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        rel = path.as_posix()
        if any(part in blocked_parts for part in path.parts):
            continue
        if path.suffix in blocked_suffixes:
            continue
        zf.write(path, rel)
PY
fi

# 2. 复制 docker-compose.yml
echo "Copying docker-compose.yml..."
cp docker-compose.yml "$RELEASE_DIR/"
cp docker-compose.community-smoke.yml "$RELEASE_DIR/"
cp requirements.txt "$RELEASE_DIR/"

# 4. 复制 SQL 文件（可选，供手动初始化）
echo "Copying SQL files..."
mkdir -p "$RELEASE_DIR/sql"
cp foggy_mcp/setup/sql/refresh_closure_tables.sql "$RELEASE_DIR/sql/"

echo ""
echo "=== Release Assets Ready ==="
ls -lh "$RELEASE_DIR/"

echo ""
echo "Upload these files to GitHub Release:"
echo "  https://github.com/foggy-projects/foggy-data-mcp-bridge/releases/new"
echo ""
echo "Files:"
echo "  - $ADDON_ZIP"
echo "  - docker-compose.yml"
echo "  - docker-compose.community-smoke.yml"
echo "  - requirements.txt"
echo "  - sql/refresh_closure_tables.sql (optional)"
