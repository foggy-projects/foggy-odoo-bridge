# -*- coding: utf-8 -*-
"""
E2E tests: Metadata & describe_model response validation.

Regression tests for the metadata empty-response bug:
- describe_model_internal was returning {} due to
  SemanticMetadataResponse.models exclude=True
- Fixed by using SemanticQueryService.get_metadata_v3() directly

Default format is markdown (correct field names for own dimensions).
JSON format has a known Python-side bug where own dimensions get $id suffix.

These tests verify that metadata endpoints return complete field info
through the Odoo MCP endpoint (embedded mode).
"""
import json
import os
import pytest
import requests
from .conftest import ODOO_MCP_URL

ODOO_DB = os.getenv('ODOO_DB', 'odoo_demo')
ODOO_LOGIN = os.getenv('ODOO_LOGIN', 'admin')
ODOO_PASSWORD = os.getenv('ODOO_PASSWORD', 'admin')


@pytest.fixture(scope='module')
def authed_session():
    """Session authenticated via Odoo session cookie."""
    s = requests.Session()
    s.headers.update({'Content-Type': 'application/json'})
    r = s.post(f'{ODOO_MCP_URL}/web/session/authenticate', json={
        'jsonrpc': '2.0', 'id': 1,
        'params': {
            'db': ODOO_DB,
            'login': ODOO_LOGIN,
            'password': ODOO_PASSWORD,
        }
    })
    assert r.status_code == 200
    result = r.json().get('result', {})
    if not result.get('uid'):
        pytest.skip('Odoo authentication failed')
    return s


def _call_odoo_mcp(session, tool_name, arguments):
    """Call Odoo MCP tool and return parsed text content (raw string)."""
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'tools/call',
        'params': {'name': tool_name, 'arguments': arguments},
    }
    r = session.post(f'{ODOO_MCP_URL}/foggy-mcp/rpc', json=payload)
    assert r.status_code == 200, f'HTTP {r.status_code}: {r.text[:200]}'
    body = r.json()
    assert 'error' not in body, f'RPC error: {body.get("error")}'
    text = body['result']['content'][0]['text']
    return text


# ═══════════════════════════════════════════════════════════════
#  get_metadata (default: markdown)
# ═══════════════════════════════════════════════════════════════

class TestGetMetadata:
    """Verify dataset__get_metadata returns complete model info."""

    def test_metadata_not_empty(self, authed_session):
        """Regression: get_metadata must NOT return empty string or {}."""
        text = _call_odoo_mcp(authed_session, 'dataset__get_metadata', {})
        assert len(text) > 100, (
            f"get_metadata returned too little content ({len(text)} chars)! "
            "This is a regression — SemanticMetadataResponse.models "
            "exclude=True was not bypassed."
        )

    def test_metadata_contains_all_models(self, authed_session):
        """All 9 Odoo models should be mentioned in metadata."""
        text = _call_odoo_mcp(authed_session, 'dataset__get_metadata', {})
        expected_models = [
            'OdooSaleOrderQueryModel',
            'OdooPurchaseOrderQueryModel',
            'OdooAccountMoveQueryModel',
            'OdooStockPickingQueryModel',
            'OdooHrEmployeeQueryModel',
            'OdooResPartnerQueryModel',
            'OdooResCompanyQueryModel',
            'OdooCrmLeadQueryModel',
        ]
        for m in expected_models:
            assert m in text, f"Model '{m}' not found in metadata output"

    def test_metadata_json_format(self, authed_session):
        """JSON format should return V3 with version, fields, models."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__get_metadata', {'format': 'json'})
        data = json.loads(text)
        assert data.get('version') == 'v3', \
            f"Expected version='v3', got {data.get('version')}"
        assert 'fields' in data, "Missing 'fields' in JSON metadata"
        assert 'models' in data, "Missing 'models' in JSON metadata"
        assert len(data['fields']) >= 50, \
            f"Expected 50+ fields, got {len(data['fields'])}"


# ═══════════════════════════════════════════════════════════════
#  describe_model_internal (default: markdown)
# ═══════════════════════════════════════════════════════════════

class TestDescribeModel:
    """Verify dataset__describe_model_internal returns complete field info."""

    def test_describe_not_empty(self, authed_session):
        """Regression: describe_model must NOT return empty string or {}."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooHrEmployeeQueryModel'},
        )
        assert len(text) > 100, (
            f"describe_model returned too little content ({len(text)} chars)! "
            "This is a regression."
        )

    def test_hr_employee_has_department(self, authed_session):
        """HR Employee must include department JOIN fields."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooHrEmployeeQueryModel'},
        )
        assert 'department$id' in text, \
            "department$id not found in HR Employee describe output"
        assert 'department$caption' in text, \
            "department$caption not found in HR Employee describe output"

    def test_hr_employee_has_job_fields(self, authed_session):
        """HR Employee must include job-related fields."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooHrEmployeeQueryModel'},
        )
        assert 'job$caption' in text or 'jobTitle' in text, \
            "No job-related fields in HR Employee describe output"

    def test_hr_employee_has_contact_or_work_fields(self, authed_session):
        """HR Employee must include work-related fields (email, phone, or location)."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooHrEmployeeQueryModel'},
        )
        # TM/QM may define these as dimensions, properties, or attributes
        has_work_fields = (
            'workEmail' in text or 'work_email' in text or
            'workPhone' in text or 'work_phone' in text or
            'workLocation' in text or 'work_location' in text
        )
        assert has_work_fields, \
            "No work-related fields found in HR Employee describe output"

    def test_hr_employee_fields_not_empty(self, authed_session):
        """HR Employee describe must return meaningful field definitions."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooHrEmployeeQueryModel'},
        )
        # Should have dimension fields (JOIN dimensions like department, job)
        assert 'department$id' in text, \
            "department$id not found — TM/QM may not have loaded correctly"
        # Should have measure fields
        assert 'employeeCount' in text or 'employee_count' in text, \
            "No employee count measure found"

    def test_sale_order_has_partner_fields(self, authed_session):
        """Sale Order model must include partner JOIN fields."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooSaleOrderQueryModel'},
        )
        assert 'partner$id' in text, \
            "partner$id not found in Sale Order describe output"
        assert 'partner$caption' in text, \
            "partner$caption not found in Sale Order describe output"

    def test_describe_includes_measures(self, authed_session):
        """Describe model should include measure section."""
        text = _call_odoo_mcp(
            authed_session, 'dataset__describe_model_internal',
            {'model': 'OdooSaleOrderQueryModel'},
        )
        assert '度量' in text or 'measure' in text.lower(), \
            "No measure section found in Sale Order describe output"
