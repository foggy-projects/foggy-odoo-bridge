# Java 团队问题报告：`selfAndAncestorsOf` 操作符导致 res_partner 查询返回空结果

**日期**: 2026-03-22
**报告方**: Odoo Bridge 团队
**优先级**: 中（功能缺陷，不阻塞主流程）
**影响范围**: Java 网关模式下 `OdooResPartnerQueryModel` 查询

## 问题描述

三引擎对比测试中，Java 网关模式下 `OdooResPartnerQueryModel` 查询返回 **0 行数据**（API 调用本身不报错），而内嵌 Python 引擎同场景返回正常数据。

### 测试结果对比

| 场景 | 内嵌 Python | Java 网关 |
|------|------------|-----------|
| T01：基础查询合作伙伴 | ✅ 有数据 | ⚠️ 0 行 |
| T16：合作伙伴按国家分布 | ✅ 有数据 | ⚠️ 0 行 |
| 其他 13 个场景 | ✅ 全部通过 | ✅ 全部通过 |

## 根因分析

### 1. Odoo ir.rule 产生的权限条件

`res.partner` 模型在 Odoo 17 中有一条默认全局 ir.rule（多公司规则），domain 为：

```python
# ir.rule: res_partner_rule (全局规则, perm_read=True)
['|', ('company_id', '=', False), ('company_id', 'child_of', company_ids)]
```

### 2. Odoo Bridge 权限桥接的转换

`permission_bridge.py` 将 `child_of` 转换为 Foggy 闭包表操作符：

```python
# permission_bridge.py 第 388-394 行
if op in ('child_of', 'parent_of'):
    dim_field = HIERARCHY_FIELD_MAP.get(field)  # company_id → company$id
    if dim_field:
        foggy_op = 'selfAndDescendantsOf' if op == 'child_of' else 'selfAndAncestorsOf'
        expanded.append((dim_field, foggy_op, norm_value))
```

转换结果，注入到 `payload.slice`：

```json
{
  "$or": [
    {"field": "company$id", "op": "is null"},
    {"field": "company$id", "op": "selfAndDescendantsOf", "value": [1, 2]}
  ]
}
```

### 3. Java 引擎处理结果

Java 引擎收到 `selfAndDescendantsOf` 操作符后，需要通过 `res_company_closure` 闭包表进行 JOIN 查询。

**可能的问题点**（请 Java 团队确认）：

1. **闭包表操作符在 `$or` 内的 slice 不被支持**：`selfAndDescendantsOf` 可能只在顶级 slice 条件中生效，嵌套在 `$or` 内时被忽略或处理错误，导致整个 `$or` 条件被评估为 false → 0 行
2. **`is null` + `selfAndDescendantsOf` 的 OR 组合**：这种组合可能在 SQL 生成阶段出错（闭包表 JOIN 与 IS NULL 的 OR 逻辑矛盾）
3. **闭包表 JOIN 对 res_partner 的 company_id 维度处理异常**：res_partner 的 company_id 允许为 NULL（个人联系人无公司），闭包表 JOIN 可能过滤掉了 company_id 为 NULL 的记录

### 4. 为什么 Python 内嵌引擎正常？

Python 引擎的 `selfAndDescendantsOf` 实现可能对 `$or` 嵌套和 NULL 值有不同的处理方式，或者 Python 侧的 slice 处理更宽松。

## 复现步骤

### 方式一：直接通过 Java MCP Server

```bash
# 1. 确保 Java MCP Server 运行中（port 7108）且 odoo 数据源已配置

# 2. 查询 res_partner（不带 slice → 有数据）
curl http://localhost:7108/mcp -X POST -H 'Content-Type: application/json' -d '{
  "jsonrpc": "2.0", "id": 1,
  "method": "tools/call",
  "params": {
    "name": "dataset.query_model",
    "arguments": {
      "model": "OdooResPartnerQueryModel",
      "payload": {"columns": ["name", "email", "city"], "limit": 5}
    }
  }
}'
# 期望：返回数据

# 3. 查询 res_partner（带 slice → 0 行）
curl http://localhost:7108/mcp -X POST -H 'Content-Type: application/json' -d '{
  "jsonrpc": "2.0", "id": 1,
  "method": "tools/call",
  "params": {
    "name": "dataset.query_model",
    "arguments": {
      "model": "OdooResPartnerQueryModel",
      "payload": {
        "columns": ["name", "email", "city"],
        "limit": 5,
        "slice": [
          {"$or": [
            {"field": "company$id", "op": "is null"},
            {"field": "company$id", "op": "selfAndDescendantsOf", "value": [1, 2]}
          ]}
        ]
      }
    }
  }
}'
# 实际：返回 0 行
```

### 方式二：通过 Odoo MCP 端点（Java 网关模式）

```bash
# 1. 切换 Odoo 到网关模式
# 设置 foggy_mcp.engine_mode = 'gateway'
# 设置 foggy_mcp.foggy_server_url = 'http://localhost:7108'

# 2. 运行对比测试
cd D:/foggy-projects/foggy-data-mcp/foggy-odoo-bridge
pytest tests/e2e/test_engine_comparison.py::test_query_scenario[T01] -v
```

## 期望行为

`selfAndDescendantsOf` 在 `$or` 条件内应正常工作。SQL 应类似：

```sql
SELECT rp.id, rp.name, rp.email, rp.city
FROM res_partner rp
LEFT JOIN res_company_closure rcc ON rp.company_id = rcc.descendant_id
WHERE rp.company_id IS NULL
   OR rcc.ancestor_id IN (1, 2)
LIMIT 5
```

## 相关文件

| 文件 | 说明 |
|------|------|
| `foggy_mcp/services/permission_bridge.py` 第 356-409 行 | Odoo ir.rule child_of → selfAndDescendantsOf 转换逻辑 |
| `foggy_mcp/services/permission_bridge.py` 第 784-808 行 | `in` + `False` → `$or` IS NULL 组合生成逻辑 |
| `tests/results/java-gateway-results.json` T01 | 测试结果（success=true, row_count=0） |
| `tests/e2e/test_engine_comparison.py` T01, T16 | 测试场景定义 |
| `OdooResPartnerModel.tm` | res_partner 表模型定义（含 company 维度） |

## 建议

请 Java 团队检查 `selfAndDescendantsOf` / `selfAndAncestorsOf` 操作符在以下场景的支持情况：

1. **嵌套在 `$or` 条件内**的闭包表操作符
2. **与 `is null` 组成 OR** 的组合（`company_id IS NULL OR closure_join`）
3. 如果当前不支持嵌套场景，是否可以扩展支持？

## 联系

如有疑问，请联系 Odoo Bridge 团队。
