#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
三引擎对比测试 — 全部通过 Odoo MCP 端点 (/foggy-mcp/rpc)

三引擎共享同一入口、同一权限注入层、同一 payload，
记录每个引擎生成的 SQL，对比行数和数据一致性。

用法：
    python tests/e2e/run_comparison_via_odoo.py
"""
import json
import os
import sys
import time

import requests

ODOO = os.getenv('ODOO_MCP_URL', 'http://localhost:8069')
DB = os.getenv('ODOO_DB', 'odoo_demo')
LOGIN = os.getenv('ODOO_LOGIN', 'admin')
PASSWORD = os.getenv('ODOO_PASSWORD', 'admin')

JAVA_URL = 'http://host.docker.internal:7108'
PYTHON_URL = 'http://host.docker.internal:8066'

SCENARIOS = [
    {'id': 'T01', 'name': 'res_partner: name+email+city', 'model': 'OdooResPartnerQueryModel',
     'payload': {'columns': ['name', 'email', 'city'], 'limit': 5}},
    {'id': 'T02', 'name': 'res_company: name', 'model': 'OdooResCompanyQueryModel',
     'payload': {'columns': ['name'], 'limit': 5}},
    {'id': 'T03', 'name': 'hr_employee: name', 'model': 'OdooHrEmployeeQueryModel',
     'payload': {'columns': ['name'], 'limit': 5}},
    {'id': 'T04', 'name': 'crm_lead: name', 'model': 'OdooCrmLeadQueryModel',
     'payload': {'columns': ['name'], 'limit': 5}},
    {'id': 'T05', 'name': 'sale_order: name+amountTotal', 'model': 'OdooSaleOrderQueryModel',
     'payload': {'columns': ['name', 'amountTotal'], 'limit': 5}},
    {'id': 'T06', 'name': 'purchase_order: name', 'model': 'OdooPurchaseOrderQueryModel',
     'payload': {'columns': ['name'], 'limit': 5}},
    {'id': 'T07', 'name': 'account_move: name', 'model': 'OdooAccountMoveQueryModel',
     'payload': {'columns': ['name'], 'limit': 5}},
    {'id': 'T08', 'name': 'stock_picking: name', 'model': 'OdooStockPickingQueryModel',
     'payload': {'columns': ['name'], 'limit': 5}},
    {'id': 'T10', 'name': 'agg: companyCount', 'model': 'OdooResCompanyQueryModel',
     'payload': {'columns': ['companyCount']}},
    {'id': 'T11', 'name': 'agg: employeeCount', 'model': 'OdooHrEmployeeQueryModel',
     'payload': {'columns': ['employeeCount']}},
    {'id': 'T15', 'name': 'join: dept+employeeCount', 'model': 'OdooHrEmployeeQueryModel',
     'payload': {'columns': ['department$caption', 'employeeCount'], 'groupBy': ['department$caption']}},
    {'id': 'T16', 'name': 'join: partner by country', 'model': 'OdooResPartnerQueryModel',
     'payload': {'columns': ['country$caption', 'partnerCount'], 'groupBy': ['country$caption']}},
    {'id': 'T22', 'name': 'self-ref: company+parent', 'model': 'OdooResCompanyQueryModel',
     'payload': {'columns': ['name', 'parent$caption'], 'limit': 5}},
    {'id': 'T31', 'name': 'sort: crm expectedRevenue DESC', 'model': 'OdooCrmLeadQueryModel',
     'payload': {'columns': ['name', 'expectedRevenue'],
                 'orderBy': [{'field': 'expectedRevenue', 'direction': 'DESC'}], 'limit': 5}},
    {'id': 'T42', 'name': 'agg: employeeCount total', 'model': 'OdooHrEmployeeQueryModel',
     'payload': {'columns': ['employeeCount']}},
]


def odoo_login():
    s = requests.Session()
    s.headers['Content-Type'] = 'application/json'
    r = s.post(f'{ODOO}/web/session/authenticate', json={
        'jsonrpc': '2.0', 'params': {'db': DB, 'login': LOGIN, 'password': PASSWORD}
    })
    assert r.status_code == 200 and 'error' not in r.json(), f'Login failed: {r.text[:200]}'
    return s


def set_param(session, key, val):
    session.post(f'{ODOO}/web/dataset/call_kw', json={
        'jsonrpc': '2.0', 'id': 1, 'method': 'call',
        'params': {'model': 'ir.config_parameter', 'method': 'set_param',
                   'args': [key, val], 'kwargs': {}}
    })


def get_param(session, key):
    r = session.post(f'{ODOO}/web/dataset/call_kw', json={
        'jsonrpc': '2.0', 'id': 1, 'method': 'call',
        'params': {'model': 'ir.config_parameter', 'method': 'get_param',
                   'args': [key], 'kwargs': {}}
    })
    return r.json().get('result', '')


def mcp_query(session, model, payload):
    start = time.time()
    r = session.post(f'{ODOO}/foggy-mcp/rpc', json={
        'jsonrpc': '2.0', 'id': 1,
        'method': 'tools/call',
        'params': {'name': 'dataset.query_model', 'arguments': {'model': model, 'payload': payload}},
    }, timeout=30)
    duration = (time.time() - start) * 1000

    if r.status_code != 200:
        return {'success': False, 'error': f'HTTP {r.status_code}', 'items': [], 'sql': '', 'duration_ms': duration}

    body = r.json()
    if 'error' in body:
        return {'success': False, 'error': str(body['error']), 'items': [], 'sql': '', 'duration_ms': duration}

    text = body.get('result', {}).get('content', [{}])[0].get('text', '{}')
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {'success': False, 'error': f'JSON parse: {text[:100]}', 'items': [], 'sql': '', 'duration_ms': duration}

    # Normalize Java {code, data} vs Python {items}
    if 'code' in data and 'data' in data:
        ok = data.get('code') == 200
        inner = data.get('data', {})
        err = data.get('msg', '') if not ok else ''
    else:
        ok = 'error' not in data
        inner = data
        err = data.get('error', '')

    return {
        'success': ok,
        'items': inner.get('items', []),
        'sql': (inner.get('debug') or {}).get('extra', {}).get('sql', ''),
        'duration_ms': round((inner.get('debug') or {}).get('durationMs', duration), 2),
        'error': err,
    }


def run_all(session, label):
    results = []
    for sc in SCENARIOS:
        r = mcp_query(session, sc['model'], sc['payload'])
        record = {
            'id': sc['id'], 'name': sc['name'], 'model': sc['model'],
            'success': r['success'], 'row_count': len(r['items']),
            'duration_ms': r['duration_ms'], 'error': r['error'],
            'sql': r['sql'], 'sample_data': r['items'][:3],
        }
        results.append(record)
        tag = 'PASS' if r['success'] else 'FAIL'
        print(f'  [{tag}] {sc["id"]} {sc["name"]}: rows={len(r["items"])} ({r["duration_ms"]}ms)')
        if r['error']:
            print(f'        error: {r["error"][:100]}')
    return results


def main():
    session = odoo_login()
    os.makedirs('tests/results', exist_ok=True)
    all_results = {}

    # ═══ Phase 1: Embedded ═══
    print('\n=== Phase 1: Embedded Python (via Odoo MCP) ===')
    set_param(session, 'foggy_mcp.engine_mode', 'embedded')
    time.sleep(1)
    print(f'  engine_mode={get_param(session, "foggy_mcp.engine_mode")}')
    all_results['embedded'] = run_all(session, 'embedded')
    with open('tests/results/embedded-results.json', 'w', encoding='utf-8') as f:
        json.dump({'engine_mode': 'embedded', 'via': 'odoo-mcp',
                   'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
                   'scenarios': all_results['embedded']}, f, ensure_ascii=False, indent=2)

    # ═══ Phase 2: Java Gateway ═══
    print('\n=== Phase 2: Java Gateway (via Odoo MCP) ===')
    set_param(session, 'foggy_mcp.engine_mode', 'gateway')
    set_param(session, 'foggy_mcp.server_url', JAVA_URL)
    set_param(session, 'foggy_mcp.namespace', 'odoo')
    time.sleep(1)
    print(f'  engine_mode={get_param(session, "foggy_mcp.engine_mode")}, '
          f'server_url={get_param(session, "foggy_mcp.server_url")}, '
          f'namespace={get_param(session, "foggy_mcp.namespace")}')
    all_results['java'] = run_all(session, 'java-gateway')
    with open('tests/results/java-gateway-results.json', 'w', encoding='utf-8') as f:
        json.dump({'engine_mode': 'java-gateway', 'via': 'odoo-mcp',
                   'server_url': JAVA_URL, 'namespace': 'odoo',
                   'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
                   'scenarios': all_results['java']}, f, ensure_ascii=False, indent=2)

    # ═══ Phase 3: Python Gateway ═══
    print('\n=== Phase 3: Python Gateway (via Odoo MCP) ===')
    set_param(session, 'foggy_mcp.server_url', PYTHON_URL)
    set_param(session, 'foggy_mcp.endpoint_path', '/mcp/analyst/rpc')
    set_param(session, 'foggy_mcp.namespace', '')
    time.sleep(1)
    print(f'  server_url={get_param(session, "foggy_mcp.server_url")}, '
          f'namespace="{get_param(session, "foggy_mcp.namespace")}"')
    all_results['python'] = run_all(session, 'python-gateway')
    with open('tests/results/python-gateway-results.json', 'w', encoding='utf-8') as f:
        json.dump({'engine_mode': 'python-gateway', 'via': 'odoo-mcp',
                   'server_url': PYTHON_URL, 'namespace': '',
                   'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
                   'scenarios': all_results['python']}, f, ensure_ascii=False, indent=2)

    # ═══ Switch back ═══
    set_param(session, 'foggy_mcp.engine_mode', 'embedded')
    print('\n=== Switched back to embedded mode ===')

    # ═══ Summary ═══
    ep = sum(1 for r in all_results['embedded'] if r['success'])
    jp = sum(1 for r in all_results['java'] if r['success'])
    pp = sum(1 for r in all_results['python'] if r['success'])
    print(f'\n{"="*80}')
    print(f'SUMMARY: Embedded={ep}/15  Java={jp}/15  Python={pp}/15')
    print(f'{"="*80}')

    # Row count comparison
    print(f'\n{"ID":<5} {"Scenario":<35} {"Embedded":>8} {"Java":>8} {"Python":>8} {"Match":>6}')
    print('-' * 75)
    all_match = True
    for i, sc in enumerate(SCENARIOS):
        e = all_results['embedded'][i]['row_count']
        j = all_results['java'][i]['row_count']
        p = all_results['python'][i]['row_count']
        match = 'Y' if e == j == p else 'N'
        if match == 'N':
            all_match = False
        print(f'{sc["id"]:<5} {sc["name"]:<35} {e:>8} {j:>8} {p:>8} {match:>6}')
    print(f'\nAll rows match: {all_match}')

    # SQL comparison
    print(f'\n=== SQL COMPARISON ===')
    for i, sc in enumerate(SCENARIOS):
        e_sql = all_results['embedded'][i]['sql']
        j_sql = all_results['java'][i]['sql']
        p_sql = all_results['python'][i]['sql']
        sql_match = 'Y' if e_sql == j_sql == p_sql else 'N'
        if sql_match == 'N' and (e_sql or j_sql or p_sql):
            print(f'\n--- {sc["id"]} {sc["name"]} (SQL DIFF) ---')
            if e_sql:
                print(f'  Embedded: {e_sql[:200]}')
            if j_sql:
                print(f'  Java:     {j_sql[:200]}')
            if p_sql:
                print(f'  Python:   {p_sql[:200]}')

    # Save full comparison
    with open('tests/results/comparison-full.json', 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'via': 'odoo-mcp (unified entry point)',
            'summary': {'embedded': f'{ep}/15', 'java': f'{jp}/15', 'python': f'{pp}/15'},
            'all_rows_match': all_match,
            'scenarios': [
                {
                    'id': SCENARIOS[i]['id'],
                    'name': SCENARIOS[i]['name'],
                    'model': SCENARIOS[i]['model'],
                    'embedded': {'rows': all_results['embedded'][i]['row_count'],
                                 'sql': all_results['embedded'][i]['sql'],
                                 'data': all_results['embedded'][i]['sample_data']},
                    'java':     {'rows': all_results['java'][i]['row_count'],
                                 'sql': all_results['java'][i]['sql'],
                                 'data': all_results['java'][i]['sample_data']},
                    'python':   {'rows': all_results['python'][i]['row_count'],
                                 'sql': all_results['python'][i]['sql'],
                                 'data': all_results['python'][i]['sample_data']},
                }
                for i in range(len(SCENARIOS))
            ],
        }, f, ensure_ascii=False, indent=2)
    print(f'\nFull comparison saved to tests/results/comparison-full.json')


if __name__ == '__main__':
    main()
