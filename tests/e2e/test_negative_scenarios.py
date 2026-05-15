# -*- coding: utf-8 -*-
"""
Negative / error scenario E2E tests.

Tests that the system handles error conditions gracefully:
- Invalid/missing API keys
- Malformed requests
- Non-existent models
- Missing permissions
- Unreachable gateway server

These tests require Odoo running (port 8069) with foggy_mcp installed.

Environment variables:
    ODOO_MCP_URL  - Odoo gateway URL (default: http://localhost:8069)
    ODOO_API_KEY  - Valid API key for positive control tests
"""
import json
import os
import pytest
import requests

ODOO_MCP_URL = os.getenv('ODOO_MCP_URL', 'http://localhost:8069')
ODOO_API_KEY = os.getenv('ODOO_API_KEY', '')


@pytest.fixture(scope='module')
def odoo_url():
    return ODOO_MCP_URL


@pytest.fixture(scope='module')
def valid_session(odoo_url):
    """Session with valid API key (for control tests)."""
    s = requests.Session()
    s.headers.update({'Content-Type': 'application/json'})
    if ODOO_API_KEY:
        s.headers.update({'Authorization': f'Bearer {ODOO_API_KEY}'})
    try:
        r = s.get(f'{odoo_url}/foggy-mcp/health', timeout=5)
        if r.status_code != 200:
            pytest.skip(f'Odoo not reachable at {odoo_url}')
    except requests.ConnectionError:
        pytest.skip(f'Odoo not reachable at {odoo_url}')
    return s


def _rpc(session, url, method, params=None):
    """Send a JSON-RPC request and return the response."""
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': method,
        'params': params or {},
    }
    return session.post(f'{url}/foggy-mcp/rpc', json=payload, timeout=15)


# ── N4: Invalid API Key ──────────────────────────────────────────


class TestInvalidApiKey:
    """N4: Invalid API Key should return authentication error."""

    def test_random_fmcp_key(self, odoo_url):
        """API key with fmcp_ prefix but invalid value."""
        s = requests.Session()
        s.headers.update({
            'Content-Type': 'application/json',
            'Authorization': 'Bearer fmcp_THIS_IS_NOT_A_REAL_KEY_12345',
        })
        r = _rpc(s, odoo_url, 'tools/list')
        body = r.json()
        assert 'error' in body, f'Expected error in response: {body}'
        assert 'Authentication' in body['error'].get('message', '') or \
               body['error'].get('code') == -32000

    def test_wrong_prefix_key(self, odoo_url):
        """API key without fmcp_ prefix."""
        s = requests.Session()
        s.headers.update({
            'Content-Type': 'application/json',
            'Authorization': 'Bearer sk-wrong-prefix-key',
        })
        r = _rpc(s, odoo_url, 'tools/list')
        body = r.json()
        assert 'error' in body, f'Expected error: {body}'

    def test_empty_bearer(self, odoo_url):
        """N5: Empty Authorization header value."""
        s = requests.Session()
        s.headers.update({
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ',
        })
        r = _rpc(s, odoo_url, 'tools/list')
        body = r.json()
        assert 'error' in body

    def test_no_auth_header(self, odoo_url):
        """No Authorization header at all (and no session cookie)."""
        s = requests.Session()
        s.headers.update({'Content-Type': 'application/json'})
        r = _rpc(s, odoo_url, 'tools/list')
        body = r.json()
        assert 'error' in body


# ── N9: Malformed Requests ───────────────────────────────────────


class TestMalformedRequests:
    """N9: Malformed JSON-RPC requests should return proper errors."""

    def test_empty_body(self, odoo_url, valid_session):
        """Send empty POST body."""
        r = valid_session.post(
            f'{odoo_url}/foggy-mcp/rpc',
            data='',
            headers={'Content-Type': 'application/json'},
            timeout=10,
        )
        # Should not crash (500); expect 400 or JSON-RPC error
        assert r.status_code in (200, 400), f'Unexpected status: {r.status_code}'
        if r.status_code == 200:
            body = r.json()
            assert 'error' in body

    def test_invalid_json(self, odoo_url, valid_session):
        """Send invalid JSON."""
        r = valid_session.post(
            f'{odoo_url}/foggy-mcp/rpc',
            data='this is not json',
            headers={'Content-Type': 'application/json'},
            timeout=10,
        )
        assert r.status_code in (200, 400)

    def test_missing_method(self, odoo_url, valid_session):
        """JSON-RPC without method field."""
        r = valid_session.post(
            f'{odoo_url}/foggy-mcp/rpc',
            json={'jsonrpc': '2.0', 'id': 1, 'params': {}},
            timeout=10,
        )
        body = r.json()
        # Should handle gracefully — either error or "method not found"
        assert r.status_code in (200, 400)

    def test_unknown_method(self, odoo_url, valid_session):
        """Call a non-existent JSON-RPC method."""
        r = _rpc(valid_session, odoo_url, 'nonexistent/method')
        body = r.json()
        assert 'error' in body
        assert body['error']['code'] == -32601  # Method not found

    def test_tools_call_missing_tool_name(self, odoo_url, valid_session):
        """tools/call without tool name."""
        r = _rpc(valid_session, odoo_url, 'tools/call', {
            'arguments': {'model': 'OdooSaleOrderQueryModel', 'payload': {'columns': ['name']}}
        })
        body = r.json()
        result = body.get('result', {})
        # Should contain error about missing tool name
        assert 'error' in result or 'error' in body


# ── N10: Non-existent Model ──────────────────────────────────────


class TestNonExistentModel:
    """N10: Querying a non-existent model should return clear error."""

    def test_query_fake_model(self, odoo_url, valid_session):
        """Query a model that doesn't exist."""
        r = _rpc(valid_session, odoo_url, 'tools/call', {
            'name': 'dataset__query_model',
            'arguments': {
                'model': 'NonExistentFakeModel',
                'payload': {'columns': ['name'], 'limit': 5},
            }
        })
        body = r.json()
        result = body.get('result', {})
        # Should get an error — either in result.error or in result.content
        # The engine should not crash
        assert r.status_code == 200, f'Should not return HTTP error: {r.status_code}'
        # Verify it's not returning actual data
        if isinstance(result, dict) and 'content' in result:
            content = result['content']
            if isinstance(content, list) and content:
                text = content[0].get('text', '')
                data = json.loads(text) if text else {}
                assert data.get('code') != 200, 'Should not return success for fake model'


# ── N7: Health endpoint always accessible ────────────────────────


class TestHealthEndpoint:
    """Health endpoint should work without authentication."""

    def test_health_no_auth(self, odoo_url):
        """Health check without any auth headers."""
        r = requests.get(f'{odoo_url}/foggy-mcp/health', timeout=10)
        assert r.status_code == 200
        body = r.json()
        assert body.get('status') in ('ok', 'degraded')
        assert 'checks' in body

    def test_health_returns_model_count(self, odoo_url):
        """Health should report model mapping info."""
        r = requests.get(f'{odoo_url}/foggy-mcp/health', timeout=10)
        body = r.json()
        models = body.get('checks', {}).get('models', {})
        assert models.get('mapped_count', 0) > 0, 'Should have mapped models'


# ── Positive control: valid request works ────────────────────────


class TestPositiveControl:
    """Sanity check: valid requests still work (prevents false negatives)."""

    @pytest.mark.skipif(not ODOO_API_KEY, reason='ODOO_API_KEY not set')
    def test_tools_list_works(self, odoo_url, valid_session):
        """Valid tools/list request returns tools."""
        r = _rpc(valid_session, odoo_url, 'tools/list')
        body = r.json()
        assert 'result' in body
        tools = body['result'].get('tools', [])
        assert len(tools) > 0, 'Should have at least one tool'

    @pytest.mark.skipif(not ODOO_API_KEY, reason='ODOO_API_KEY not set')
    def test_query_model_works(self, odoo_url, valid_session):
        """Valid query returns data."""
        r = _rpc(valid_session, odoo_url, 'tools/call', {
            'name': 'dataset__query_model',
            'arguments': {
                'model': 'OdooResCompanyQueryModel',
                'payload': {'columns': ['name'], 'limit': 5},
            }
        })
        body = r.json()
        assert 'result' in body
        result = body['result']
        # Should have content with actual data
        assert 'content' in result or 'items' in str(result), \
            f'Expected data in result: {str(result)[:200]}'
