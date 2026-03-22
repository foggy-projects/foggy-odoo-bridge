# 三引擎对比测试

运行三引擎（内嵌 Python / Java 网关 / Python 网关）对比测试并生成报告。

## 触发条件

当用户需要运行引擎对比测试、验证三引擎一致性、或使用 `/engine-test` 时触发。

## 前置条件

| 引擎 | 前置要求 |
|------|---------|
| 内嵌 Python | Odoo 运行中（port 8069），`engine_mode=embedded` |
| Java 网关 | Java MCP Server 运行中（port 7108），使用 `/java-mcp` 启动 |
| Python 网关 | Python MCP Server 运行中（port 8066），使用 `/python-mcp` 启动 |

## 测试方式

### 方式一：通过 Odoo MCP 端点（内嵌/Java 网关）

```bash
cd D:/foggy-projects/foggy-data-mcp/foggy-odoo-bridge

# 内嵌模式测试
pytest tests/e2e/test_engine_comparison.py -v \
  --result-file=tests/results/embedded-results.json

# Java 网关模式测试（需先在 Odoo 切换 engine_mode=gateway）
pytest tests/e2e/test_engine_comparison.py -v \
  --result-file=tests/results/java-gateway-results.json
```

### 方式二：直接调用引擎 API

**Java 网关**（注意 X-NS header）：
```bash
curl -X POST http://localhost:7108/mcp/analyst/rpc \
  -H 'Content-Type: application/json' \
  -H 'X-NS: odoo' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"dataset.query_model","arguments":{"model":"OdooResCompanyQueryModel","payload":{"columns":["name"],"limit":5}}}}'
```

**Python 网关**（无需 namespace）：
```bash
curl -X POST http://localhost:8066/api/v1/query/OdooResCompanyQueryModel \
  -H 'Content-Type: application/json' \
  -d '{"columns": ["name"], "limit": 5}'
```

## 15 个测试场景

| ID | 场景 | 覆盖能力 |
|----|------|---------|
| T01-T08 | 9 个模型基础查询 (name/email/amountTotal) | Properties + Measures |
| T10-T11 | 聚合 (companyCount, employeeCount) | Measures + COUNT_DISTINCT |
| T15-T16 | JOIN + 分组 (department$caption, country$caption) | Dimension JOINs + GROUP BY |
| T22 | 自引用维度 (company + parent$caption) | 闭包表 + 自引用 JOIN |
| T31 | 排序 (expectedRevenue DESC) | ORDER BY |
| T42 | 员工总数聚合 | 总量聚合 |

## 结果文件

- `tests/results/embedded-results.json`
- `tests/results/java-gateway-results.json`
- `tests/results/python-gateway-results.json`
- `tests/results/comparison-report.md` — 汇总报告

## 关键差异点

| 方面 | Java 网关 | Python 网关 |
|------|----------|------------|
| **namespace** | 需要 `X-NS: odoo` header | 不需要 |
| **响应格式** | `{code:200, data:{items:[...]}}` | `{items:[...]}` |
| **MCP 端点** | `/mcp/analyst/rpc` (JSON-RPC) | `/api/v1/query/{model}` (REST) |
| **认证** | Bearer token 或 `foggy.auth.token` | 无 |
