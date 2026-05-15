"""Unit tests for Odoo-version Foggy namespace resolution."""
import importlib.util
import os
import sys
import types

import pytest


_bridge_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), 'foggy_mcp', 'services'
)
_spec = importlib.util.spec_from_file_location(
    'foggy_mcp.services.odoo_namespace',
    os.path.join(_bridge_dir, 'odoo_namespace.py'),
)
_ns = importlib.util.module_from_spec(_spec)
sys.modules['foggy_mcp.services.odoo_namespace'] = _ns
_spec.loader.exec_module(_ns)


class _FakeConfig:
    def __init__(self, value=None):
        self.value = value
        self.writes = []

    def sudo(self):
        return self

    def get_param(self, key, default=None):
        return self.value if self.value is not None else default

    def set_param(self, key, value):
        self.writes.append((key, value))
        self.value = value


class _FakeEnv(dict):
    def __init__(self, config):
        super().__init__({'ir.config_parameter': config})


def _install_fake_release(monkeypatch, version_info=(17, 0), version='17.0'):
    odoo = types.ModuleType('odoo')
    odoo.release = types.SimpleNamespace(version_info=version_info, version=version)
    monkeypatch.setitem(sys.modules, 'odoo', odoo)


def test_namespace_for_odoo17():
    assert _ns.namespace_for_major_version(17) == 'odoo17'


def test_namespace_rejects_legacy_explicit_value():
    with pytest.raises(_ns.OdooNamespaceError):
        _ns.validate_foggy_namespace('odoo')


def test_legacy_config_resolves_to_odoo17(monkeypatch):
    _install_fake_release(monkeypatch)
    env = _FakeEnv(_FakeConfig('odoo'))

    assert _ns.resolve_configured_foggy_namespace(env) == 'odoo17'


def test_sync_backfills_legacy_config(monkeypatch):
    _install_fake_release(monkeypatch)
    config = _FakeConfig('odoo')
    env = _FakeEnv(config)

    assert _ns.sync_configured_foggy_namespace(env) == 'odoo17'
    assert config.writes == [('foggy_mcp.namespace', 'odoo17')]


def test_configured_namespace_must_match_runtime(monkeypatch):
    _install_fake_release(monkeypatch, version_info=(18, 0), version='18.0')
    env = _FakeEnv(_FakeConfig('odoo17'))

    with pytest.raises(_ns.OdooNamespaceError):
        _ns.resolve_configured_foggy_namespace(env)
