"""Odoo runtime namespace resolution for Foggy model profiles."""
from __future__ import annotations

import ast
import logging
import re
from pathlib import Path
from typing import Any, Optional

_logger = logging.getLogger(__name__)


SUPPORTED_ODOO_MAJOR_VERSIONS = (17,)
LEGACY_NAMESPACE = "odoo"
CONFIG_KEY = "foggy_mcp.namespace"


class OdooNamespaceError(ValueError):
    """Raised when the bridge cannot choose a safe Foggy namespace."""


def resolve_odoo_major_version(env: Any = None) -> Optional[int]:
    """Return the current Odoo major version when it can be detected."""
    release = _get_odoo_release()
    major = _major_from_version_info(getattr(release, "version_info", None))
    if major is not None:
        return major
    return _major_from_version_text(getattr(release, "version", None))


def resolve_bridge_manifest_major_version() -> Optional[int]:
    """Return the addon manifest major version as a fallback signal."""
    manifest_path = Path(__file__).resolve().parents[1] / "__manifest__.py"
    try:
        manifest = ast.literal_eval(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive fallback path
        _logger.debug("Cannot read Foggy Odoo manifest version: %s", exc)
        return None
    if not isinstance(manifest, dict):
        return None
    return _major_from_version_text(manifest.get("version"))


def namespace_for_major_version(major: Optional[int]) -> str:
    """Map a supported Odoo major version to its Foggy namespace."""
    if major in SUPPORTED_ODOO_MAJOR_VERSIONS:
        return "odoo%s" % major
    raise OdooNamespaceError(
        "Unsupported Odoo major version for Foggy namespace: %s" % major
    )


def validate_foggy_namespace(namespace: Any) -> str:
    """Validate an explicit Foggy namespace value."""
    value = str(namespace or "").strip()
    if value == "odoo17":
        return value
    raise OdooNamespaceError(
        "Foggy namespace must be explicit: expected odoo17, got %r" % value
    )


def resolve_foggy_namespace(env: Any = None) -> str:
    """Resolve the runtime Foggy namespace from Odoo, then manifest fallback."""
    major = resolve_odoo_major_version(env)
    if major is None:
        major = resolve_bridge_manifest_major_version()
    return namespace_for_major_version(major)


def resolve_configured_foggy_namespace(env: Any) -> str:
    """Resolve the namespace while treating legacy ``odoo`` as runtime-derived."""
    runtime_namespace = resolve_foggy_namespace(env)
    configured = _read_config_param(env, CONFIG_KEY)
    configured_text = str(configured or "").strip()
    if not configured_text or configured_text == LEGACY_NAMESPACE:
        return runtime_namespace

    explicit_namespace = validate_foggy_namespace(configured_text)
    if explicit_namespace != runtime_namespace:
        raise OdooNamespaceError(
            "Configured Foggy namespace %r does not match runtime namespace %r"
            % (explicit_namespace, runtime_namespace)
        )
    return explicit_namespace


def sync_configured_foggy_namespace(env: Any) -> str:
    """Backfill missing/legacy config parameters to the resolved namespace."""
    namespace = resolve_foggy_namespace(env)
    config = _config_parameter(env)
    if config is None:
        return namespace
    current = str(config.get_param(CONFIG_KEY, "") or "").strip()
    if not current or current == LEGACY_NAMESPACE:
        set_param = getattr(config, "set_param", None)
        if callable(set_param):
            set_param(CONFIG_KEY, namespace)
    elif validate_foggy_namespace(current) != namespace:
        raise OdooNamespaceError(
            "Configured Foggy namespace %r does not match runtime namespace %r"
            % (current, namespace)
        )
    return namespace


def _get_odoo_release() -> Any:
    try:
        import odoo  # type: ignore
    except Exception:
        return None
    return getattr(odoo, "release", None)


def _major_from_version_info(version_info: Any) -> Optional[int]:
    if not version_info:
        return None
    try:
        return int(version_info[0])
    except Exception:
        return None


def _major_from_version_text(version: Any) -> Optional[int]:
    if not version:
        return None
    match = re.match(r"\s*(\d+)", str(version))
    if not match:
        return None
    return int(match.group(1))


def _read_config_param(env: Any, key: str) -> Optional[str]:
    config = _config_parameter(env)
    if config is None:
        return None
    return config.get_param(key, None)


def _config_parameter(env: Any) -> Any:
    try:
        config = env["ir.config_parameter"]
    except Exception:
        return None
    sudo = getattr(config, "sudo", None)
    return sudo() if callable(sudo) else config


__all__ = [
    "CONFIG_KEY",
    "LEGACY_NAMESPACE",
    "OdooNamespaceError",
    "SUPPORTED_ODOO_MAJOR_VERSIONS",
    "namespace_for_major_version",
    "resolve_bridge_manifest_major_version",
    "resolve_configured_foggy_namespace",
    "resolve_foggy_namespace",
    "resolve_odoo_major_version",
    "sync_configured_foggy_namespace",
    "validate_foggy_namespace",
]
