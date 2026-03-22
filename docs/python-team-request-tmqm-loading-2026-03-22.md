# Python 团队协助请求：TM/QM 加载器修复 + 多数据源解析

**日期**: 2026-03-22（更新）
**请求方**: Odoo Bridge 团队
**优先级**: 高

## 背景

三引擎对比测试已完成。Python 独立 MCP Server 通过 TM/QM 文件加载了 9 个 Odoo 模型，测试结果 **13/15 通过**。以下是需要 Python 团队修复的问题清单。

**测试报告**: [`tests/results/comparison-report.md`](../tests/results/comparison-report.md)

---

## 需求 1（已完成）：TM/QM 文件加载 ✅

`create_app()` lifespan 中 `load_models_from_directory()` 已实现，9 个 TM 模型成功加载。

---

## 需求 2：多数据源解析（NamedDataSourceResolver）

### 问题描述

TM 文件中定义了 `dataSourceName: odoo`，Java 侧有完整的运行时数据源解析链路，Python 侧缺失此能力。

`app.py` 已有 `ExecutorManager` 管理多 executor（第 98-118 行），且 `set_executor_manager()` 已注入到 `SemanticQueryService`。但 `SemanticQueryService` 查询时仍只使用 default executor，不按 `model.source_datasource` 路由。

### Java 侧实现（标准）

```
TM: dataSourceName: "odoo"
  → NamedDataSourceResolver.resolve("odoo")
    → DataSourceManager.getDataSource("odoo")
      → 返回对应的 javax.sql.DataSource 连接池
```

### 期望实现

`SemanticQueryService` 执行查询时，根据 model 的 `source_datasource` 字段从 `ExecutorManager` 选择对应的 executor：

```python
# SemanticQueryService._execute_query() 中
executor = self._executor_manager.get(model.source_datasource) or self._executor
```

---

## 需求 3：`_adapt_fsscript_tm()` Measure 名称缺失（BUG）

### 问题描述

TM 文件中很多 measure **没有 `name` 字段**（只有 `column` + `caption`），`_adapt_fsscript_tm()` 没有为其生成 `name`，导致 `_load_measures()` 的 `if not measure_name: continue` 过滤掉了大量 measure。

### 复现

```python
# OdooSaleOrderModel.tm 的 measures 部分（FSScript 求值后）
[
    {'column': 'amount_untaxed', 'caption': 'Untaxed Amount', 'type': 'MONEY', 'aggregation': 'sum'},
    {'column': 'amount_tax',     'caption': 'Taxes',          'type': 'MONEY', 'aggregation': 'sum'},
    {'column': 'amount_total',   'caption': 'Total',          'type': 'MONEY', 'aggregation': 'sum'},
    {'column': 'currency_rate',  'caption': 'Currency Rate',  'type': 'NUMBER'},
    # 以上 4 个都没有 name 字段 → 全部被丢弃！
    {'column': 'id', 'name': 'orderCount', 'caption': 'Order Count', 'type': 'INTEGER', 'aggregation': 'COUNT_DISTINCT'},
    # 只有这个有 name → 仅此一个被加载
]
```

### 影响

| 模型 | TM 定义的 measures | 实际加载的 measures | 丢失 |
|------|-------------------|-------------------|------|
| OdooSaleOrderModel | 5 | 1 (`orderCount`) | 4 (amountUntaxed, amountTax, amountTotal, currencyRate) |
| OdooAccountMoveModel | 8 | 1 (`entryCount`) | 7 |
| OdooCrmLeadModel | 8 | 1 (`leadCount`) | 7 (expectedRevenue, probableRevenue 等) |
| OdooPurchaseOrderModel | 4 | 1 (`orderCount`) | 3 |
| OdooSaleOrderLineModel | 10 | 1 (`lineCount`) | 9 |

### 修复方案

在 `_adapt_fsscript_tm()` 中，为没有 `name` 的 measure 基于 `column` 生成 camelCase 名称：

```python
# _adapt_fsscript_tm() measures 适配
for m in raw_measures:
    measure = dict(m)
    if "name" not in measure and "column" in measure:
        # amount_total → amountTotal
        measure["name"] = _snake_to_camel(measure["column"])
    adapted_measures.append(measure)
```

Java 侧 TM 文件中没有 `name` 的 measure 是用 `column` 的 camelCase 形式作为默认名称。

---

## 需求 4：Dimension JOINs 缺失（`$caption` 返回 FK ID）

### 问题描述

TM 中 dimensions 定义了外键关系（`tableName`, `primaryKey`, `captionColumn`），`_load_dimensions()` 将这些信息存入了 `DbModelDimensionImpl.table` 字段，但 **`DimensionJoinDef` 对象未被创建**，导致 `model.dimension_joins` 始终为空列表。

### 影响

查询 `department$caption` 时，不生成 JOIN SQL，返回 FK ID 而非部门名称：

```sql
-- 当前（错误）
SELECT t.department_id AS "Department" FROM hr_employee AS t

-- 期望（正确）
SELECT d_dept.complete_name AS "Department"
FROM hr_employee AS t
LEFT JOIN hr_department AS d_dept ON t.department_id = d_dept.id
```

### 修复方案

在 `_load_dimensions()` 或 `_adapt_fsscript_tm()` 中，为有 `tableName` 的 dimension 创建对应的 `DimensionJoinDef`：

```python
# _load_dimensions 后追加
if dim_def.get("tableName"):
    join = DimensionJoinDef(
        name=dim_name,
        table_name=dim_def["tableName"],
        foreign_key=dim_def.get("column", dim_name),
        primary_key=dim_def.get("primaryKey", "id"),
        caption_column=dim_def.get("captionColumn", "name"),
        caption=dim_def.get("alias"),
    )
    model.dimension_joins.append(join)
```

---

## 需求 5：Properties（`model.columns`）查询支持

### 问题描述

TM `properties` 被正确加载到 `model.columns` 字典中（6-20 个属性/模型），但 `SemanticQueryService._build_query()` 构建 SQL 时不识别这些列。

### 影响

```python
# 查询 name（property 列）
payload = {"columns": ["name"], "limit": 5}
# 返回: {"warnings": ["Column not found: name"]}
```

### Java 行为

Java 引擎的 properties 可以直接出现在 `columns` 列表中，生成 `SELECT t.name` 语句。

### 修复方案

`SemanticQueryService._resolve_column()` 中增加对 `model.columns` 的查找，在 dimensions/measures 找不到时 fallback 到 properties。

---

## 需求 6：FSScript `@service` 导入支持

### 问题描述

TM 文件的 `dicts.fsscript` 导入 `@jdbcModelDictService`：

```javascript
import { registerDict } from '@jdbcModelDictService';
```

`FileModuleLoader` 将 `@` 前缀当作文件路径解析，报 `ModuleNotFoundError`。导致 `ODOO_DATA_SOURCE_NAME` 常量和所有字典定义无法加载。

### 当前 Workaround

我们在启动脚本中 monkey-patch 了 `FileModuleLoader.load_module`，对 `@` 前缀返回 `{'registerDict': lambda d: d}` 的 mock。

### 期望实现

`FileModuleLoader` 内建对 `@service` 导入的处理：
- 对 `@` 前缀的模块路径，查找已注册的 SPI 服务提供者
- 未找到时返回 no-op stub（而非抛异常），确保不影响其他 export

---

## 需求 7：QM 文件加载

### 问题描述

`load_models_from_directory()` 只扫描 `.tm` 文件（`model_path.rglob("*.tm")`），不处理 `.qm` 文件。

QM 文件定义查询模型（column groups、access control、calculated fields），是 Java 侧完整功能的必要部分。

### 影响

- 无法通过 QM 名称（如 `OdooSaleOrderQueryModel`）查询
- 需要在外部脚本中手动注册 TM → QM 名称别名
- 缺少 column groups 和 computed fields 定义

### 期望实现

1. 扫描 `.qm` 文件，解析其 `tableModel` 引用关系
2. 将 QM 注册为可查询模型（关联到对应的 TM）
3. QM 的 `columnGroups` 信息用于 `describe_model` 返回

---

## 优先级建议

| 优先级 | 需求 | 影响 |
|--------|------|------|
| P0 | 需求 3：Measure 名称缺失 | 大量 measure 丢失（如 amountTotal），影响 80% 查询 |
| P0 | 需求 4：Dimension JOINs | `$caption` 返回 ID 而非文本，JOIN 查询完全不工作 |
| P1 | 需求 5：Properties 查询 | name/email 等基本列不可查，影响所有基础查询 |
| P1 | 需求 7：QM 文件加载 | 查询模型名称不匹配，需手动别名 |
| P2 | 需求 6：@service 导入 | 当前有 workaround，但不优雅 |
| P2 | 需求 2：多数据源解析 | 单数据源场景不影响，多数据源时阻塞 |

## 测试验证

```bash
# 启动 Python MCP Server（使用 Odoo TM/QM 文件）
python scripts/start_python_mcp.py --port 8066

# 验证 measure 加载
curl -s -X POST http://localhost:8066/api/v1/query/OdooSaleOrderQueryModel \
  -H 'Content-Type: application/json' \
  -d '{"columns": ["amountTotal"], "limit": 5}'
# 期望: 返回销售订单金额数据

# 验证 dimension JOIN
curl -s -X POST http://localhost:8066/api/v1/query/OdooHrEmployeeQueryModel \
  -H 'Content-Type: application/json' \
  -d '{"columns": ["department$caption", "employeeCount"], "groupBy": ["department$caption"]}'
# 期望: department$caption 返回部门名称（如 "Management"），不是 FK ID

# 验证 property 查询
curl -s -X POST http://localhost:8066/api/v1/query/OdooResCompanyQueryModel \
  -H 'Content-Type: application/json' \
  -d '{"columns": ["name"], "limit": 5}'
# 期望: 返回公司名称列表
```

## 联系

如有疑问，请联系 Odoo Bridge 团队。
