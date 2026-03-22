# Odoo Bridge 团队 → Python 团队：三引擎对比测试建议

**日期**: 2026-03-22
**背景**: 三引擎对比测试全部 15/15 通过（内嵌 Python / Java 网关 / Python 网关）

---

## 1. 本次修复已全部验证通过 ✅

感谢 Python 团队快速修复了以下 6 个问题，三引擎对比测试 Python 网关 15/15 全部通过：

| 问题 | 修复前 | 修复后 |
|------|--------|--------|
| Measure 名称缺失 | SaleOrder 仅 1 measure | 5 measures（含 amountTotal） |
| Dimension JOINs 缺失 | `$caption` 返回 FK ID | 返回文本（"Management"） |
| Properties 不可查询 | `name` 列 404 | 正常查询 |
| QM 文件未加载 | 仅 9 TM | 18 个（9 TM + 9 QM） |
| `@service` 导入异常 | 抛 ModuleNotFoundError | 需外部 mock（见建议 2） |
| 多数据源架构 | ExecutorManager 已到位 | 查询路由待完善（见建议 3） |

---

## 2. FSScript `@service` 导入内建支持

### 现状

TM 文件的 `dicts.fsscript` 导入 `@jdbcModelDictService`（Java SPI 服务），Python `FileModuleLoader` 将 `@` 前缀当作文件路径解析，抛 `ModuleNotFoundError`。

当前 Odoo 侧在 `start_python_mcp.py` 中 monkey-patch 了 `FileModuleLoader.load_module`：

```python
def _patched_load(self, module_path, context):
    if module_path.startswith('@'):
        return {'registerDict': lambda d: d}
    return _original_load(self, module_path, context)
```

### 建议

在 `FileModuleLoader` 内建对 `@` 前缀的处理：

```python
def load_module(self, module_path, context):
    if module_path.startswith('@'):
        # SPI service import — return no-op stub if no provider registered
        provider = self._spi_registry.get(module_path)
        if provider:
            return provider
        logger.debug("SPI service %s not available, returning stub", module_path)
        return self._create_spi_stub(module_path)
    # ... normal file loading
```

这样调用方不需要做 monkey-patch，同时支持 Java 团队未来注册更多 SPI 服务时 Python 侧自动降级。

---

## 3. 多数据源查询路由

### 现状

`app.py` 中 `ExecutorManager` 已为每个数据源创建了独立 executor，且通过 `set_executor_manager()` 注入了 `SemanticQueryService`。但查询时 `SemanticQueryService` 仍使用默认 executor，不按 `model.source_datasource` 路由。

当前单数据源场景（`odoo`）不影响功能。但多数据源场景（如同时查 Odoo + 仓储系统）需要路由能力。

### 建议

在 `SemanticQueryService` 查询执行时增加数据源路由：

```python
# SemanticQueryService._execute_query() 或 query_model() 中
ds_name = model.source_datasource
executor = self._executor_manager.get(ds_name) if self._executor_manager else self._executor
if not executor:
    return SemanticQueryResponse.from_error(f"No executor for datasource: {ds_name}")
```

---

## 4. Property 列名的 camelCase 映射

### 现状

TM 的 `properties` 中 `column` 是数据库字段名（如 `date_order`），`_adapt_fsscript_tm()` 将 `name` 字段生成为 camelCase（如 `dateOrder`）。查询时用 `dateOrder` 能匹配，但生成的 SQL 中 `SELECT` 的别名也是 camelCase。

这与 Java 引擎行为一致，目前没有问题。只是提醒注意 **property name（camelCase）和 column（snake_case）的双向映射**需要在查询引擎中保持正确。

### 可能的边界情况

- `WHERE` 条件中使用 property name（`dateOrder`）时，需要正确映射到 DB column（`date_order`）
- `ORDER BY` 同理

---

## 5. MCP JSON-RPC 端点响应格式对齐

### 现状

| 方面 | Java MCP | Python MCP |
|------|----------|-----------|
| MCP 端点 | `/mcp/analyst/rpc` | `/mcp/analyst/rpc` ✅ |
| REST 端点 | 无 | `/api/v1/query/{model}` |
| 响应格式 | `{"code":200,"data":{"items":[...]}}` | `{"items":[...]}` |
| 错误格式 | `{"code":600,"msg":"..."}` | `{"error":"..."}` |

对比测试中 Python 网关用的是 REST API（`/api/v1/query/{model}`），未测试 MCP JSON-RPC 端点。

### 建议

- 确保 Python 的 `/mcp/analyst/rpc` 端点在 `tools/call` 的 `dataset.query_model` 响应中使用和 Java 一致的格式（`{code, data, msg}`）
- 这样 Odoo `GatewayBackend` 的 `_query()` 解析逻辑可以通用，不需要区分 Java/Python 网关

---

## 6. T16 country$caption 返回 ID（Minor）

### 现状

T16 测试场景 `合作伙伴按国家分布`，`country$caption` 在 Python 网关返回 `233`（FK ID）而非 `"United States"`（Java 网关正常返回文本）。

### 可能原因

`OdooResPartnerModel.tm` 的 `country` dimension 定义中 `captionColumn` 可能在 `_adapt_fsscript_tm()` 适配后丢失，或 `DimensionJoinDef` 的 `caption_column` 未正确设置。

建议检查 `country` dimension 的 JOIN SQL 是否正确生成了 `LEFT JOIN res_country ... ON ... SELECT res_country.name`。

---

## 联系

如有疑问，请联系 Odoo Bridge 团队。
