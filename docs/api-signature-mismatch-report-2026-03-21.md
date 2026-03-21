# API 签名不一致报告

> **日期**：2026-03-21
> **报告方**：foggy-odoo-bridge 团队
> **阻塞状态**：⛔ 当前阻塞内嵌模式（embedded）开发
> **涉及项目**：
> - `foggy-data-mcp-bridge`（Java，API 签名标准方）
> - `foggy-data-mcp-bridge-python`（Python，vendored 到 `foggy_mcp/lib/foggy/`）
> - `foggy-odoo-bridge`（Odoo 插件，调用方）

---

## 背景

foggy-odoo-bridge 的内嵌模式（embedded mode）直接在 Odoo 进程内调用 vendored 的 foggy-python 引擎（`foggy_mcp/lib/foggy/`），不经过外部 Java/Python 服务器。

按照项目约定，**Python 引擎与 Java 引擎的业务方法签名必须保持一致，以 Java 为标准**。

当前发现 3 处签名不一致，导致 Odoo 侧 `embedded_backend.py` 无法直接调用 Python 引擎，内嵌模式被阻塞。

---

## 问题清单

### 问题 1：`query_model()` — 第二参数类型不匹配

**严重程度**：🔴 高（核心查询接口）

| 维度 | Java MCP（标准） | Python `SemanticQueryService` |
|------|-----------------|-------------------------------|
| 方法签名 | `queryModel(String model, Map<String, Object> payload)` | `query_model(model: str, request: SemanticQueryRequest, mode: str, context: Optional)` |
| 第二参数类型 | **`Map` / plain dict**（JSON payload 直接传入） | **`SemanticQueryRequest` dataclass 对象** |
| 文件位置 | Java 侧对应 Service | `foggy/dataset_model/semantic/service.py:138` |

**具体差异**：

```python
# Java 侧（标准）的调用方式：
result = service.queryModel("sale_order", {"columns": ["name", "amount"], "limit": 10})
#                                          ↑ plain dict/Map

# Python 侧实际签名：
def query_model(self, model: str, request: SemanticQueryRequest, mode: str = "execute", context = None)
#                                  ↑ 要求 SemanticQueryRequest 对象，不接受 dict
```

**影响**：
- Odoo 的 `embedded_backend.py` 从 MCP 协议收到的是 JSON dict，无法直接传给 `query_model()`
- 需要手动构造 `SemanticQueryRequest` 对象，增加了不必要的耦合
- `SemanticQueryRequest` 类型定义在 `foggy.mcp.spi` 中，而该模块在 vendored 版本中不可用（见问题 3）

**建议修改**（⚠️ Python 团队）：
```python
# 方案 A（推荐）：query_model() 第二参数支持 dict，内部自动转换
def query_model(self, model: str, request, mode: str = "execute", context = None):
    if isinstance(request, dict):
        request = SemanticQueryRequest.from_dict(request)
    # ... 原有逻辑

# 方案 B：新增便捷方法
def query_model_dict(self, model: str, payload: dict, mode: str = "execute"):
    request = SemanticQueryRequest.from_dict(payload)
    return self.query_model(model, request, mode)
```

---

### 问题 2：`describe_model()` — Python 侧方法缺失

**严重程度**：🔴 高（模型描述接口）

| 维度 | Java MCP（标准） | Python `SemanticQueryService` |
|------|-----------------|-------------------------------|
| 方法 | `describeModel(String model)` ✅ | ❌ **方法不存在** |
| 返回值 | 单个模型的字段定义（维度、度量、层级等） | — |
| 可能的替代 | — | `get_metadata_v3(model_names: List[str])` |
| 文件位置 | Java 侧 Service | `foggy/dataset_model/semantic/service.py`（无此方法） |

**影响**：
- Odoo `embedded_backend.py` 的 `_handle_describe_model()` 调用 `self._service.describe_model(model)` 会直接报 `AttributeError`
- 虽然有 `get_metadata_v3()`，但返回格式与 Java 侧 `describeModel()` 可能不同
- MCP 工具 `dataset.describe_model_internal` 依赖此方法

**建议修改**（⚠️ Python 团队）：
```python
# 在 SemanticQueryService 中添加：
def describe_model(self, model: str) -> dict:
    """描述单个模型的字段定义，返回格式与 Java describeModel() 一致。

    Args:
        model: 模型名称

    Returns:
        dict: 包含 dimensions, measures, columns 等字段定义
    """
    result = self.get_metadata_v3(model_names=[model])
    # 转换为与 Java describeModel() 一致的返回格式
    ...
```

**需要 Java 团队确认**：`describeModel()` 的精确返回 JSON 结构（字段名、嵌套层级），以便 Python 团队对齐。

---

### 问题 3：`foggy.mcp.spi` 模块 — vendored 版本中缺失关键类型

**严重程度**：🔴 高（阻塞引擎初始化）

| 维度 | 完整 pip 版 foggy-python | vendored 版 (`lib/foggy/`) |
|------|--------------------------|---------------------------|
| `foggy.mcp.spi` 模块 | ✅ 包含 `SemanticQueryRequest` 等 7 个类型 | ❌ **模块不存在** |
| `foggy.mcp_spi` 模块 | ✅ | ✅ 但只有 `McpTool`, `ToolResult` 等 |
| `LocalDatasetAccessor` | ✅ `foggy.mcp.spi.LocalDatasetAccessor` | ❌ 不存在 |

**`foggy.mcp.spi` 中被 `service.py` 依赖的 7 个类型**：
```python
from foggy.mcp.spi import (
    SemanticServiceResolver,      # 抽象基类
    SemanticMetadataResponse,     # 元数据响应
    SemanticQueryResponse,        # 查询响应（含 from_error, from_legacy 工厂方法）
    SemanticMetadataRequest,      # 元数据请求
    SemanticQueryRequest,         # 查询请求（问题 1 的类型）
    SemanticRequestContext,       # 请求上下文
    DebugInfo,                    # 调试信息
)
```

**影响**：
- `service.py` 第 20-28 行 import 直接失败：`No module named 'foggy.mcp'`
- 整个 `SemanticQueryService` 类无法被加载
- 内嵌引擎初始化在第一次请求时崩溃

**建议修改**（⚠️ Python 团队，二选一）：

```
方案 A（推荐）：将 foggy.mcp.spi 中的类型定义移入 foggy.mcp_spi
    - 这样 vendored 版本（已包含 mcp_spi/）可以正常工作
    - service.py 的 import 路径改为 from foggy.mcp_spi import ...

方案 B：在 vendor 规范中将 foggy/mcp/ 目录纳入（至少包含 spi.py）
    - 更新 vendor 脚本，增加: cp -r "$PY_SRC/mcp/spi.py" "$VENDOR/mcp/"
    - 需确保 mcp/spi.py 没有额外的重依赖
```

---

## 当前临时状态

| 项目 | 状态 |
|------|------|
| foggy-odoo-bridge 内嵌模式 | ⛔ 阻塞（引擎无法初始化） |
| foggy-odoo-bridge 网关模式 | ✅ 正常（通过 HTTP 调用 Java 服务器） |
| `embedded_backend.py` | 已移除 `LocalDatasetAccessor` 依赖，但仍无法工作 |

Odoo 侧 **不会做临时 workaround**（如创建 shim 模块），等待 Python 团队修复后再继续。

---

## 需要各团队的行动

### Python 团队（foggy-data-mcp-bridge-python）

1. **【必须】** 解决 `foggy.mcp.spi` 在 vendored 场景下的可用性（问题 3）
2. **【必须】** `query_model()` 支持 dict payload 入参（问题 1）
3. **【必须】** 新增 `describe_model(model)` 方法（问题 2）
4. **【建议】** 将 `LocalDatasetAccessor` 移入 `mcp_spi/` 模块

### Java 团队（foggy-data-mcp-bridge）

1. **【配合】** 提供 `describeModel()` 的精确返回 JSON 结构规范，供 Python 团队对齐
2. **【确认】** `queryModel()` 的 payload dict 结构文档（字段名、类型、可选/必须）

### Odoo 团队（foggy-odoo-bridge）

- 等待上述修复完成后，更新 vendored 代码并恢复内嵌模式开发

---

## 复现步骤

```bash
# 1. 启动环境
cd foggy-odoo-bridge
docker compose up -d postgres odoo

# 2. 安装模块
docker exec foggy-odoo pip3 install litellm
docker exec foggy-odoo odoo --database=odoo --db_host=postgres \
    --db_user=odoo --db_password=odoo -i foggy_mcp \
    --stop-after-init --without-demo=all
docker restart foggy-odoo

# 3. 观察错误
curl http://localhost:8069/foggy-mcp/health | python3 -m json.tool
# engine.status = "error"

docker logs foggy-odoo | grep "foggy"
# 输出：内嵌引擎 ping 失败：foggy-python 未安装或版本不兼容：No module named 'foggy.mcp'
```

---

## 相关文件索引

| 文件 | 项目 | 说明 |
|------|------|------|
| `foggy/dataset_model/semantic/service.py:20-28` | foggy-python (vendored) | 导入 `foggy.mcp.spi`（失败点） |
| `foggy/dataset_model/semantic/service.py:138` | foggy-python (vendored) | `query_model()` 签名 |
| `foggy/dataset_model/semantic/service_v3.py:54-65` | foggy-python (vendored) | V3 版 `SemanticQueryRequest` dataclass |
| `foggy/mcp_spi/__init__.py` | foggy-python (vendored) | 仅含 McpTool 等，缺少 Semantic 类型 |
| `foggy_mcp/services/embedded_backend.py` | foggy-odoo-bridge | 内嵌模式后端（调用方） |
