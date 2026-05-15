#!/usr/bin/env bash
# check-odoo-apps-readiness.sh — lightweight Odoo Apps packaging checks.
#
# This is not a replacement for Odoo's review. It catches local regressions
# that are easy to miss before preparing a Community listing package.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== check-odoo-apps-readiness ==="
echo "  Project: $PROJECT_ROOT"
echo ""

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN=python
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=python3
  else
    echo "FAIL: python or python3 is required" >&2
    exit 1
  fi
fi

"$PYTHON_BIN" - "$PROJECT_ROOT" <<'PY'
import ast
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
module = root / "foggy_mcp"
manifest_path = module / "__manifest__.py"
description_path = module / "static" / "description" / "index.html"
icon_path = module / "static" / "description" / "icon.png"
gitattributes_path = root / ".gitattributes"
requirements_path = root / "requirements.txt"
tool_names_path = module / "services" / "tool_names.py"
embedded_backend_path = module / "services" / "embedded_backend.py"
required_public_images = [
    module / "static" / "description" / "banner.png",
    module / "static" / "description" / "screenshot_settings.png",
    module / "static" / "description" / "screenshot_setup_wizard.png",
    module / "static" / "description" / "screenshot_api_keys.png",
]

errors: list[str] = []


def fail(message: str) -> None:
    errors.append(message)


if not manifest_path.exists():
    fail("Missing foggy_mcp/__manifest__.py")
    manifest = {}
else:
    try:
        manifest = ast.literal_eval(manifest_path.read_text(encoding="utf-8").split("\n", 1)[1])
    except Exception as exc:  # pragma: no cover - diagnostic script
        fail(f"Cannot parse __manifest__.py: {exc}")
        manifest = {}

required_keys = [
    "name",
    "version",
    "category",
    "summary",
    "description",
    "author",
    "website",
    "license",
    "support",
    "depends",
    "data",
    "images",
    "price",
    "currency",
    "installable",
    "application",
]
for key in required_keys:
    if key not in manifest:
        fail(f"Manifest missing required key: {key}")

if manifest.get("installable") is not True:
    fail("Manifest installable must be True")
if manifest.get("application") is not True:
    fail("Manifest application must be True for Apps listing")
if manifest.get("price") != 0:
    fail("Community Apps listing price must be 0")

license_value = manifest.get("license")
if license_value not in {"LGPL-3", "AGPL-3", "OEEL-1", "OPL-1", "Other OSI approved licence"}:
    fail(f"Manifest license value is not a known Odoo value: {license_value!r}")

summary = manifest.get("summary", "")
if len(summary) > 80:
    fail("Manifest summary should stay concise for the Apps card")

depends = set(manifest.get("depends", []))
pro_depends = {"project", "mrp"}
unexpected_depends = depends & pro_depends
if unexpected_depends:
    fail(f"Community manifest depends on Pro-scope modules: {sorted(unexpected_depends)}")

python_deps = set((manifest.get("external_dependencies") or {}).get("python", []))
blocked_python_deps = {"openai", "anthropic"}
blocked_found = python_deps & blocked_python_deps
if blocked_found:
    fail(f"Community external_dependencies include AI provider SDKs: {sorted(blocked_found)}")

expected_import_deps = {"asyncpg", "pydantic", "yaml"}
if python_deps != expected_import_deps:
    fail(f"Manifest Python import dependencies drifted: {sorted(python_deps)}")

if requirements_path.exists():
    req_names = set()
    for raw_line in requirements_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        req_names.add(re.split(r"[<>=!~\[]", line, maxsplit=1)[0].strip().lower())
    expected_pip_deps = {"asyncpg", "pydantic", "pyyaml"}
    if req_names != expected_pip_deps:
        fail(f"requirements.txt dependencies drifted: {sorted(req_names)}")
else:
    fail("Missing requirements.txt for Odoo runtime dependencies")

if tool_names_path.exists():
    tool_names_source = tool_names_path.read_text(encoding="utf-8")
    tool_tree = ast.parse(tool_names_source)
    tool_assignments = {}
    for node in tool_tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant):
                tool_assignments[target.id] = node.value.value
    public_tool_values = {
        value for key, value in tool_assignments.items()
        if key.startswith("TOOL_") and isinstance(value, str)
    }
    expected_public_tools = {
        "dataset__query_model",
        "dataset__list_models",
        "dataset__get_metadata",
        "dataset__describe_model_internal",
    }
    if public_tool_values != expected_public_tools:
        fail(f"Public MCP tool names drifted: {sorted(public_tool_values)}")
    public_name_pattern = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
    for name in public_tool_values:
        if not public_name_pattern.fullmatch(name):
            fail(f"Public MCP tool name is not strict-client safe: {name}")
else:
    fail("Missing services/tool_names.py public MCP tool-name mapping")

if embedded_backend_path.exists():
    embedded_source = embedded_backend_path.read_text(encoding="utf-8")
    try:
        embedded_tree = ast.parse(embedded_source)
        tool_node = next(
            node
            for class_node in embedded_tree.body
            if isinstance(class_node, ast.ClassDef) and class_node.name == "EmbeddedBackend"
            for node in class_node.body
            if isinstance(node, ast.FunctionDef) and node.name == "_build_tool_definitions"
        )
    except StopIteration:
        fail("Cannot locate EmbeddedBackend._build_tool_definitions")
    except SyntaxError as exc:
        fail(f"Cannot parse services/embedded_backend.py: {exc}")
    else:
        tool_block = ast.get_source_segment(embedded_source, tool_node) or ""
        if re.search(r"[\u4e00-\u9fff]", tool_block):
            fail("Public MCP tool metadata must be English-only; CJK text found in tool definitions")
else:
    fail("Missing services/embedded_backend.py")

images = manifest.get("images", [])
for rel_image in images:
    image_path = module / rel_image
    if not image_path.exists():
        fail(f"Manifest image does not exist: {rel_image}")

if not icon_path.exists():
    fail("Missing static/description/icon.png")
if not description_path.exists():
    fail("Missing static/description/index.html")
for image_path in required_public_images:
    if not image_path.exists():
        fail(f"Missing public Apps image: {image_path.relative_to(module)}")
    elif image_path.stat().st_size < 10_000:
        fail(f"Public Apps image looks too small: {image_path.relative_to(module)}")

if description_path.exists():
    html = description_path.read_text(encoding="utf-8")
    lower_html = html.lower()
    if "<script" in lower_html:
        fail("static/description/index.html must not contain JavaScript")

    external_refs = re.findall(r"""(?:href|src)\s*=\s*["']([^"']+)["']""", html, flags=re.I)
    allowed_external_prefixes = ("mailto:", "skype:")
    for ref in external_refs:
        ref_lower = ref.lower()
        if ref_lower.startswith(("http://", "https://")):
            if "youtube.com/" in ref_lower or "youtu.be/" in ref_lower:
                continue
            fail(f"Disallowed external reference in static description: {ref}")
        elif ":" in ref and not ref_lower.startswith(allowed_external_prefixes):
            fail(f"Disallowed non-local reference in static description: {ref}")

    blocked_runtime_terms = [
        "foggy_chat",
        "chat_controller",
        "llm_service",
        "/foggy-mcp/chat",
        "foggy ai",
        "project.task",
        "mrp.production",
        "odoo project",
        "odoo mrp",
    ]
    for term in blocked_runtime_terms:
        if term in lower_html:
            fail(f"Pro/AI runtime term found in static description: {term}")

license_file = root / "LICENSE"
if license_file.exists():
    license_text = license_file.read_text(encoding="utf-8", errors="ignore")
    if "Apache License" in license_text and license_value != "Other OSI approved licence":
        fail("LICENSE is Apache-2.0 but manifest license is not 'Other OSI approved licence'")
else:
    fail("Missing LICENSE file")

if gitattributes_path.exists():
    gitattributes = gitattributes_path.read_text(encoding="utf-8", errors="ignore")
    required_export_ignores = [
        "docs export-ignore",
        "tests export-ignore",
        "scripts export-ignore",
        "docker-compose.community-smoke.yml export-ignore",
        "build-release.sh export-ignore",
    ]
    for line in required_export_ignores:
        if line not in gitattributes:
            fail(f".gitattributes missing release archive exclusion: {line}")
else:
    fail("Missing .gitattributes release archive exclusions")

if errors:
    for message in errors:
        print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)

print("OK: Odoo Apps readiness checks passed.")
PY
