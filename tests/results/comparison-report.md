# 三引擎对比测试报告

**日期**: 2026-03-22
**数据库**: odoo_demo (Odoo 17 Demo Data)
**测试环境**: Windows 11, Docker Desktop (PostgreSQL + Odoo 17)

## 总结

| 引擎 | 通过/总计 | 通过率 |
|------|----------|--------|
| **内嵌 Python** | 15/15 | 100% |
| **Java 网关** | 15/15 | 100% |
| **Python 网关** | 15/15 | 100% |

**三引擎全部 15/15 通过。**

## 引擎架构

```
内嵌 Python:  AI Client → Odoo MCP → embedded foggy-python → PostgreSQL
Java 网关:    AI Client → Odoo MCP → HTTP → Java MCP Server (port 7108) → PostgreSQL
Python 网关:  AI Client → Python MCP Server REST API (port 8066) → PostgreSQL
```

## 测试场景对比

| ID | 场景名称 | 内嵌 Python | Java 网关 | Python 网关 |
|----|---------|------------|----------|------------|
| T01 | 基础查询：合作伙伴 (name+email+city) | PASS | PASS (61 rows) | PASS (5 rows) |
| T02 | 基础查询：公司 (name) | PASS (2) | PASS (2) | PASS (2) |
| T03 | 基础查询：员工 (name) | PASS (20) | PASS (20) | PASS (5) |
| T04 | 基础查询：CRM 线索 (name) | PASS (44) | PASS (44) | PASS (5) |
| T05 | 基础查询：销售订单 (name+amountTotal) | PASS (24) | PASS (24) | PASS (5) |
| T06 | 基础查询：采购订单 (name) | PASS (11) | PASS (11) | PASS (5) |
| T07 | 基础查询：会计分录 (name) | PASS (24) | PASS (24) | PASS (5) |
| T08 | 基础查询：库存调拨 (name) | PASS (25) | PASS (25) | PASS (5) |
| T10 | 聚合：公司数 | PASS (2) | PASS (2) | PASS (1) |
| T11 | 聚合：员工数 | PASS (20) | PASS (20) | PASS (1) |
| T15 | JOIN+分组：各部门员工数 | PASS (7) | PASS (7) | PASS (7) |
| T16 | JOIN+分组：合作伙伴按国家 | PASS | PASS (2) | PASS (2) |
| T22 | 自引用维度：公司+母公司 | PASS (2) | PASS (2) | PASS (2) |
| T31 | 排序：CRM 按预期收入 DESC | PASS | PASS (44) | PASS (5) |
| T42 | 聚合：员工总数 | PASS (20) | PASS (20) | PASS (1) |

## 已解决的问题

### Java 网关（本次迭代修复）

| 问题 | 修复前 | 修复后 |
|------|--------|--------|
| `selfAndDescendantsOf` 在 `$or` 内不支持 | T01 返回 0 行 | T01 返回 61 行 |

### Python 网关（本次迭代修复）

| 问题 | 修复前 | 修复后 |
|------|--------|--------|
| Measure 名称缺失 (`amountTotal` 等) | SaleOrder 仅 1 measure | 5 measures 全部加载 |
| Dimension JOINs 缺失 | `$caption` 返回 FK ID | 返回文本（如 "Management"） |
| Properties 不可查询 | `name` 等列 404 | 正常查询 |
| QM 文件未加载 | 仅 9 TM | 18 个 (9 TM + 9 QM) |

## 数据一致性

| 度量 | 内嵌 Python | Java 网关 | Python 网关 |
|------|------------|----------|------------|
| companyCount | 2 | 2 | 2 |
| employeeCount | 20 | 20 | 20 |
| 部门分组数 | 7 | 7 | 7 |

## 结论

1. **三引擎功能对等**，均通过全部 15 个测试场景
2. **内嵌 Python 引擎**推荐作为默认模式（零外部依赖、最低延迟）
3. **Java/Python 网关**适用于需要独立部署引擎或多数据源场景

## 测试结果文件

- `tests/results/embedded-results.json` — 内嵌 Python 引擎
- `tests/results/java-gateway-results.json` — Java 网关模式
- `tests/results/python-gateway-results.json` — Python 网关模式
