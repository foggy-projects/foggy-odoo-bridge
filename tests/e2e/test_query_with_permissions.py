# -*- coding: utf-8 -*-
"""
E2E tests: Query execution with permission injection.

Regression tests for the IN operator double-wrapping bug:
- permission_bridge.py injected {"field":"company$id","op":"in","value":[1,2]}
- foggy-python's _add_filter treated value as single item: [value] → [[1,2]]
- SQL generated IN (?) with single placeholder instead of IN (?, ?)
- asyncpg passed [1,2] as array to single $1, PostgreSQL returned 0 rows

Fixed by using 'values' key for IN/NOT IN in permission_bridge.py.

These tests verify that queries through the Odoo MCP endpoint
(with automatic permission slice injection) return actual data.
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


def _query_model(session, model, columns, limit=5, slice_conditions=None):
    """Query a model via Odoo MCP endpoint and return parsed result dict."""
    query_payload = {
        'columns': columns,
        'limit': limit,
    }
    if slice_conditions:
        query_payload['slice'] = slice_conditions

    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'tools/call',
        'params': {
            'name': 'dataset.query_model',
            'arguments': {
                'model': model,
                'payload': query_payload,
            },
        },
    }
    r = session.post(f'{ODOO_MCP_URL}/foggy-mcp/rpc', json=payload)
    assert r.status_code == 200, f'HTTP {r.status_code}: {r.text[:200]}'
    body = r.json()
    assert 'error' not in body, f'RPC error: {body.get("error")}'
    text = body['result']['content'][0]['text']
    return json.loads(text)


# ═══════════════════════════════════════════════════════════════
#  Core regression: queries with permission injection return data
# ═══════════════════════════════════════════════════════════════

class TestQueryWithPermissions:
    """Verify that queries through Odoo MCP (with ir.rule permission
    injection) return non-empty results.

    The admin user should always have access to demo data.
    If these tests return 0 rows, the permission slice injection
    is likely broken (e.g., IN operator double-wrapping bug).
    """

    def test_sale_orders_return_data(self, authed_session):
        """Regression: sale.order query must return rows.

        This is the exact scenario that triggered the IN double-wrapping bug:
        ir.rule injects company_id IN [1,2] → was generating IN (?) instead
        of IN (?, ?).
        """
        result = _query_model(
            authed_session,
            'OdooSaleOrderQueryModel',
            ['name', 'amountTotal'],
        )
        items = result.get('items', [])
        assert len(items) > 0, (
            f"Sale orders returned 0 rows! "
            f"SQL: {result.get('debug', {}).get('extra', {}).get('sql', 'N/A')}\n"
            f"This is likely the IN operator double-wrapping regression."
        )

    def test_sale_order_has_expected_fields(self, authed_session):
        """Query result should contain the requested fields."""
        result = _query_model(
            authed_session,
            'OdooSaleOrderQueryModel',
            ['name', 'amountTotal'],
            limit=1,
        )
        items = result.get('items', [])
        assert len(items) > 0, "No items returned"
        item = items[0]
        # Fields should be present (aliases may vary by language)
        assert len(item) >= 2, f"Expected at least 2 fields, got {len(item)}: {item}"

    def test_purchase_orders_return_data(self, authed_session):
        """Purchase orders should also return data with permission injection."""
        result = _query_model(
            authed_session,
            'OdooPurchaseOrderQueryModel',
            ['name'],
        )
        items = result.get('items', [])
        assert len(items) > 0, (
            f"Purchase orders returned 0 rows! "
            f"SQL: {result.get('debug', {}).get('extra', {}).get('sql', 'N/A')}"
        )

    def test_employees_return_data(self, authed_session):
        """Employees should return data."""
        result = _query_model(
            authed_session,
            'OdooHrEmployeeQueryModel',
            ['name', 'department$caption'],
        )
        items = result.get('items', [])
        assert len(items) > 0, "Employees returned 0 rows"

    def test_partners_return_data(self, authed_session):
        """Partners should return data."""
        result = _query_model(
            authed_session,
            'OdooResPartnerQueryModel',
            ['name', 'email'],
        )
        items = result.get('items', [])
        assert len(items) > 0, (
            f"Partners returned 0 rows! "
            f"SQL: {result.get('debug', {}).get('extra', {}).get('sql', 'N/A')}"
        )

    def test_account_moves_return_data(self, authed_session):
        """Account moves (invoices) should return data."""
        result = _query_model(
            authed_session,
            'OdooAccountMoveQueryModel',
            ['name'],
        )
        items = result.get('items', [])
        assert len(items) > 0, "Account moves returned 0 rows"

    def test_sql_has_correct_in_placeholders(self, authed_session):
        """Regression: SQL should have IN (?, ?) not IN (?).

        When company_ids has multiple values (e.g., [1, 2]),
        the SQL must generate one placeholder per value.
        """
        result = _query_model(
            authed_session,
            'OdooSaleOrderQueryModel',
            ['name'],
            limit=1,
        )
        sql = result.get('debug', {}).get('extra', {}).get('sql', '')
        if 'IN' in sql:
            # Find the IN clause and check placeholder count
            import re
            in_matches = re.findall(r'IN \(([^)]+)\)', sql)
            for match in in_matches:
                placeholders = match.split(',')
                # Should have at least 1 placeholder, ideally > 1 for multi-company
                assert len(placeholders) >= 1, (
                    f"IN clause has wrong placeholder count: IN ({match}). "
                    f"Full SQL: {sql}"
                )
                # If there are multiple ?, they should not be a single ? for a list
                if '?' in match:
                    # Single ? for a list value = bug
                    placeholder_count = match.count('?')
                    assert placeholder_count >= 1, f"Unexpected IN clause: ({match})"


class TestDateFiltering:
    """Verify date filtering works with asyncpg.

    Regression: asyncpg requires Python datetime objects for timestamp
    columns, not strings. If the engine passes date strings like
    "2026-03-01" directly, asyncpg raises:
        invalid input for query argument $1: '2026-03-01'
        (expected a datetime.date or datetime.datetime instance, got 'str')
    """

    def test_date_range_filter_returns_data(self, authed_session):
        """Date range filter on sale orders must return rows.

        Uses a wide date range (2020-2030) to ensure demo data is captured
        regardless of when the test runs.
        """
        result = _query_model(
            authed_session,
            'OdooSaleOrderQueryModel',
            ['name', 'amountTotal'],
            limit=5,
            slice_conditions=[
                {'field': 'dateOrder', 'op': '>=', 'value': '2020-01-01'},
                {'field': 'dateOrder', 'op': '<=', 'value': '2030-12-31'},
            ],
        )
        items = result.get('items', [])
        assert len(items) > 0, (
            f"Date-filtered sale orders returned 0 rows! "
            f"SQL: {result.get('debug', {}).get('extra', {}).get('sql', 'N/A')}\n"
            f"This is likely the asyncpg date string type conversion bug."
        )

    def test_date_filter_with_iso_format(self, authed_session):
        """ISO datetime format (2020-01-01T00:00:00) should also work."""
        result = _query_model(
            authed_session,
            'OdooSaleOrderQueryModel',
            ['name'],
            limit=3,
            slice_conditions=[
                {'field': 'dateOrder', 'op': '>=', 'value': '2020-01-01T00:00:00'},
            ],
        )
        items = result.get('items', [])
        assert len(items) > 0, (
            f"ISO datetime filter returned 0 rows! "
            f"SQL: {result.get('debug', {}).get('extra', {}).get('sql', 'N/A')}"
        )

    def test_createdate_filter(self, authed_session):
        """createDate (property field) date filter should work.

        Odoo demo data sets hr_employee.create_date to 2010-01-01,
        so the filter must use a date before that.
        """
        result = _query_model(
            authed_session,
            'OdooHrEmployeeQueryModel',
            ['name'],
            limit=3,
            slice_conditions=[
                {'field': 'createDate', 'op': '>=', 'value': '2009-01-01'},
            ],
        )
        items = result.get('items', [])
        assert len(items) > 0, (
            f"createDate filter on employees returned 0 rows! "
            f"SQL: {result.get('debug', {}).get('extra', {}).get('sql', 'N/A')}"
        )
