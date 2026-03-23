# API 签名不一致报告 — get_metadata_v3 属性字段命名

**日期**: 2026-03-23
**报告方**: foggy-odoo-bridge（Odoo 插件团队）
**目标方**: foggy-data-mcp-bridge-python（Python 引擎团队）
**严重程度**: 中（影响 AI Chat 字段发现和查询构造）

## 问题描述

`SemanticQueryService.get_metadata_v3()` 方法在处理事实表自有维度（fact table own dimensions）时，错误地给字段名添加了 `$id` 后缀。

### Java 行为（正确）

Java 的 `SemanticServiceV3Impl` 中，自有维度直接使用字段名：

| 字段类型 | 字段名格式 | 示例 |
|----------|-----------|------|
| JOIN 维度 | `joinName$id`, `joinName$caption` | `department$id`, `department$caption` |
| 自有属性 | `fieldName`（无后缀） | `name`, `workEmail`, `jobTitle` |
| 度量 | `measureName` | `employeeCount` |

### Python 行为（错误）

`SemanticQueryService.get_metadata_v3()` 第 1041-1059 行：

```python
# Fact table own dimensions → expand to $id and $caption  ← 错误注释
for dim_name, dim in model.dimensions.items():
    id_fn = f"{dim_name}$id"          # ← Bug: 不应加 $id
    if id_fn not in fields:
        fields[id_fn] = {
            "name": f"{dim.alias or dim_name}(ID)",  # ← 也不应加 (ID)
            ...
        }
```

导致输出：`name$id`, `workEmail$id`, `jobTitle$id`, `active$id` 等。

### 影响

1. **AI Chat 幻觉**：LLM 调用 `describe_model_internal` 获取字段列表后，会使用 `workEmail$id` 去构造查询，但查询引擎实际期望的是 `workEmail`（无后缀），导致查询失败或 LLM 猜测使用错误字段名。
2. **与 Markdown 版不一致**：同一文件的 `_build_single_model_markdown()` 方法（第 1149 行）正确使用了 `dim_name`（无后缀），两个方法输出不一致。

## 预期修复

`get_metadata_v3()` 中第 1041-1059 行，自有维度应直接使用 `dim_name` 而非 `f"{dim_name}$id"`：

```python
# Fact table own dimensions → use plain field name (no $id suffix)
for dim_name, dim in model.dimensions.items():
    if dim_name not in fields:
        fields[dim_name] = {
            "name": dim.alias or dim_name,
            "fieldName": dim_name,
            "meta": f"属性 | {dim.data_type.value}",
            "type": dim.data_type.value.upper(),
            "filterType": "text",
            "filterable": dim.filterable,
            "measure": False,
            "aggregatable": False,
            "sourceColumn": dim.column,
            "models": {},
        }
    fields[dim_name]["models"][model_name] = {
        "description": dim.description or dim.alias or dim_name,
    }
```

## 受影响文件

- `foggy-data-mcp-bridge-python/src/foggy/dataset_model/semantic/service.py`
  - 方法：`get_metadata_v3()`，第 1041-1059 行

## 验证方式

修复后，通过 Odoo MCP 端点调用 `dataset.describe_model_internal`：

```bash
# 预期：属性字段不带 $id 后缀
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H "Content-Type: application/json" \
  -b "session_id=xxx" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"dataset.describe_model_internal","arguments":{"model":"OdooHrEmployeeQueryModel"}}}'

# 预期 fields 中包含：
# "name" (非 "name$id")
# "workEmail" (非 "workEmail$id")
# "jobTitle" (非 "jobTitle$id")
```

## Odoo 侧状态

- **当前**：已暂停使用 `get_metadata_v3` JSON 格式的属性字段名进行查询构造
- **临时方案**：Odoo 侧的 `embedded_backend.py` 默认使用 JSON 格式（已上线），LLM 依赖此格式发现字段。属性字段查询可能失败。
- **等待**：Python 团队修复后更新 vendored 代码即可恢复正常
