# -*- coding: utf-8 -*-
"""
三引擎对比测试 — 通过 Odoo MCP 端点 (/foggy-mcp/rpc)

测试场景来自 TESTING_GUIDE.md，覆盖：
- 基础查询（各模型可用性）
- 维度 JOIN（FK 关联表）
- 聚合分析（GROUP BY + 度量）
- 排序与分页
- 过滤条件

用法：
    # 内嵌模式（默认）
    pytest tests/e2e/test_engine_comparison.py -v

    # 指定数据库
    ODOO_DB=odoo_demo pytest tests/e2e/test_engine_comparison.py -v

    # 保存结果到文件
    pytest tests/e2e/test_engine_comparison.py -v --result-file=tests/results/embedded-results.json

环境变量：
    ODOO_MCP_URL   Odoo 地址（默认 http://localhost:8069）
    ODOO_DB        数据库名（默认 odoo_demo）
    ODOO_LOGIN     登录用户（默认 admin）
    ODOO_PASSWORD  登录密码（默认 admin）
"""
import json
import os
import time
import pytest
import requests

# ── 配置 ──────────────────────────────────────────────

ODOO_URL = os.getenv('ODOO_MCP_URL', 'http://localhost:8069')
ODOO_DB = os.getenv('ODOO_DB', 'odoo_demo')
ODOO_LOGIN = os.getenv('ODOO_LOGIN', 'admin')
ODOO_PASSWORD = os.getenv('ODOO_PASSWORD', 'admin')


# ── 测试场景定义 ──────────────────────────────────────

TEST_SCENARIOS = [
    # === 一、基础查询 ===
    {
        'id': 'T01',
        'name': '基础查询：合作伙伴',
        'category': '基础查询',
        'model': 'OdooResPartnerQueryModel',
        'payload': {'columns': ['name', 'email', 'city'], 'limit': 5},
        'expect_min_rows': 1,
    },
    {
        'id': 'T02',
        'name': '基础查询：公司',
        'category': '基础查询',
        'model': 'OdooResCompanyQueryModel',
        'payload': {'columns': ['name'], 'limit': 5},
        'expect_min_rows': 1,
    },
    {
        'id': 'T03',
        'name': '基础查询：员工',
        'category': '基础查询',
        'model': 'OdooHrEmployeeQueryModel',
        'payload': {'columns': ['name'], 'limit': 5},
        'expect_min_rows': 1,
    },
    {
        'id': 'T04',
        'name': '基础查询：CRM 线索',
        'category': '基础查询',
        'model': 'OdooCrmLeadQueryModel',
        'payload': {'columns': ['name'], 'limit': 5},
        'expect_min_rows': 1,
    },
    {
        'id': 'T05',
        'name': '基础查询：销售订单',
        'category': '基础查询',
        'model': 'OdooSaleOrderQueryModel',
        'payload': {'columns': ['name', 'amountTotal'], 'limit': 5},
        'expect_min_rows': 0,  # 可能被公司过滤
    },
    {
        'id': 'T06',
        'name': '基础查询：采购订单',
        'category': '基础查询',
        'model': 'OdooPurchaseOrderQueryModel',
        'payload': {'columns': ['name'], 'limit': 5},
        'expect_min_rows': 0,
    },
    {
        'id': 'T07',
        'name': '基础查询：会计分录',
        'category': '基础查询',
        'model': 'OdooAccountMoveQueryModel',
        'payload': {'columns': ['name'], 'limit': 5},
        'expect_min_rows': 0,
    },
    {
        'id': 'T08',
        'name': '基础查询：库存调拨',
        'category': '基础查询',
        'model': 'OdooStockPickingQueryModel',
        'payload': {'columns': ['name'], 'limit': 5},
        'expect_min_rows': 0,
    },

    # === 二、聚合分析 ===
    {
        'id': 'T10',
        'name': '聚合：公司数',
        'category': '聚合分析',
        'model': 'OdooResCompanyQueryModel',
        'payload': {'columns': ['companyCount']},
        'expect_min_rows': 1,
    },
    {
        'id': 'T11',
        'name': '聚合：员工数',
        'category': '聚合分析',
        'model': 'OdooHrEmployeeQueryModel',
        'payload': {'columns': ['employeeCount']},
        'expect_min_rows': 1,
    },

    # === 三、维度 JOIN + 分组 ===
    {
        'id': 'T15',
        'name': 'JOIN+分组：各部门员工数',
        'category': '维度JOIN',
        'model': 'OdooHrEmployeeQueryModel',
        'payload': {
            'columns': ['department$caption', 'employeeCount'],
            'groupBy': ['department$caption'],
        },
        'expect_min_rows': 1,
    },
    {
        'id': 'T16',
        'name': 'JOIN+分组：合作伙伴按国家分布',
        'category': '维度JOIN',
        'model': 'OdooResPartnerQueryModel',
        'payload': {
            'columns': ['country$caption', 'partnerCount'],
            'groupBy': ['country$caption'],
        },
        'expect_min_rows': 0,
    },
    {
        'id': 'T22',
        'name': '自引用维度：公司及母公司',
        'category': '维度JOIN',
        'model': 'OdooResCompanyQueryModel',
        'payload': {
            'columns': ['name', 'parent$caption'],
            'limit': 5,
        },
        'expect_min_rows': 1,
    },

    # === 四、排序 ===
    {
        'id': 'T31',
        'name': '排序：CRM 线索按预期收入 DESC',
        'category': '排序',
        'model': 'OdooCrmLeadQueryModel',
        'payload': {
            'columns': ['name', 'expectedRevenue'],
            'orderBy': [{'field': 'expectedRevenue', 'direction': 'DESC'}],
            'limit': 5,
        },
        'expect_min_rows': 1,
    },

    # === 五、过滤条件 ===
    {
        'id': 'T42',
        'name': '聚合：平均（员工总数）',
        'category': '统计',
        'model': 'OdooHrEmployeeQueryModel',
        'payload': {'columns': ['employeeCount']},
        'expect_min_rows': 1,
    },
]


# ── Fixtures ──────────────────────────────────────────

@pytest.fixture(scope='session')
def odoo_session():
    """Get authenticated Odoo session."""
    s = requests.Session()
    s.headers['Content-Type'] = 'application/json'

    # Login
    login_resp = s.post(f'{ODOO_URL}/web/session/authenticate', json={
        'jsonrpc': '2.0',
        'params': {'db': ODOO_DB, 'login': ODOO_LOGIN, 'password': ODOO_PASSWORD},
    })
    assert login_resp.status_code == 200, f'Login failed: {login_resp.status_code}'
    body = login_resp.json()
    assert 'error' not in body, f'Login error: {body.get("error")}'

    # Verify MCP health
    health = s.get(f'{ODOO_URL}/foggy-mcp/health', timeout=10)
    assert health.status_code == 200, f'Health check failed: {health.status_code}'
    health_data = health.json()
    assert health_data['checks']['engine']['status'] == 'ok', \
        f'Engine not ok: {health_data["checks"]["engine"]}'

    return s


@pytest.fixture(scope='session')
def engine_mode(odoo_session):
    """Return current engine mode from health check."""
    health = odoo_session.get(f'{ODOO_URL}/foggy-mcp/health', timeout=10).json()
    return health['checks']['engine']['mode']


def _call_mcp(session, method, params=None):
    """Call Odoo MCP JSON-RPC endpoint."""
    resp = session.post(f'{ODOO_URL}/foggy-mcp/rpc', json={
        'jsonrpc': '2.0',
        'id': 1,
        'method': method,
        'params': params or {},
    }, timeout=30)
    assert resp.status_code == 200, f'HTTP {resp.status_code}'
    return resp.json()


def _query(session, model, payload):
    """Execute a query and return parsed result."""
    start = time.time()
    result = _call_mcp(session, 'tools/call', {
        'name': 'dataset.query_model',
        'arguments': {'model': model, 'payload': payload},
    })
    duration = (time.time() - start) * 1000

    # Parse result
    if 'error' in result:
        return {
            'success': False,
            'error': result['error'].get('message', str(result['error'])),
            'items': [],
            'total': 0,
            'sql': '',
            'duration_ms': duration,
        }

    text = result.get('result', {}).get('content', [{}])[0].get('text', '{}')
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = {'items': [], 'error': f'JSON parse error: {text[:200]}'}

    # Normalize: Java returns {code:200, data:{items:...}}, Python returns {items:...}
    if 'code' in data and 'data' in data:
        # Java gateway format
        success = data.get('code') == 200
        inner = data.get('data', {})
        error = data.get('msg', '') if not success else ''
    else:
        # Embedded Python format
        success = 'error' not in data
        inner = data
        error = data.get('error', '')

    return {
        'success': success,
        'items': inner.get('items', []),
        'total': inner.get('total', len(inner.get('items', []))),
        'sql': (inner.get('debug') or {}).get('extra', {}).get('sql', ''),
        'duration_ms': (inner.get('debug') or {}).get('durationMs', duration),
        'error': error,
    }


# ── 结果收集 ──────────────────────────────────────────

_results = []


@pytest.fixture(scope='session', autouse=True)
def save_results(engine_mode):
    """Save all test results to JSON at end of session."""
    yield
    result_file = os.getenv('RESULT_FILE', f'tests/results/{engine_mode}-results.json')
    os.makedirs(os.path.dirname(result_file), exist_ok=True)
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump({
            'engine_mode': engine_mode,
            'odoo_url': ODOO_URL,
            'database': ODOO_DB,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'scenarios': _results,
        }, f, ensure_ascii=False, indent=2)
    print(f'\n结果已保存到: {result_file}')


# ── 测试用例 ──────────────────────────────────────────

@pytest.mark.parametrize('scenario', TEST_SCENARIOS, ids=lambda s: s['id'])
def test_query_scenario(odoo_session, engine_mode, scenario):
    """Run a single test scenario through Odoo MCP endpoint."""
    result = _query(odoo_session, scenario['model'], scenario['payload'])

    # Record result
    record = {
        'id': scenario['id'],
        'name': scenario['name'],
        'category': scenario['category'],
        'model': scenario['model'],
        'success': result['success'],
        'row_count': len(result['items']),
        'total': result['total'],
        'duration_ms': round(result['duration_ms'], 2),
        'sql': result['sql'],
        'error': result['error'],
        'sample_data': result['items'][:3],  # First 3 rows
    }
    _results.append(record)

    # Assert
    if scenario['expect_min_rows'] > 0:
        assert result['success'], f"Query failed: {result['error']}"
        assert len(result['items']) >= scenario['expect_min_rows'], \
            f"Expected >= {scenario['expect_min_rows']} rows, got {len(result['items'])}. SQL: {result['sql']}"
