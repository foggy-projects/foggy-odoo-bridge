# Foggy Odoo Bridge

[English](./README.md) | [简体中文](./README.zh-CN.md)

在保留 Odoo 权限规则的前提下，为 Odoo 数据提供受治理的 MCP 访问能力，支持内置 AI Chat。

Foggy Odoo Bridge 是一个 Odoo 插件，让 Claude、Cursor、Odoo 内置 AI Chat 以及其他 MCP 客户端可以查询 Odoo 业务数据，而不会绕过 Odoo 的权限体系。

```text
AI client -> MCP -> Odoo bridge -> Foggy semantic layer -> SQL -> PostgreSQL
```

它不会让大模型直接对 ERP 数据库生成原始 SQL，而是把认证、模型可见性和行级规则保留在 Odoo 内部，再把查询交给 Foggy 引擎执行。

## 为什么这件事重要

大多数 “AI + ERP” 演示只能做到“能问到数据”，但一旦模型开始生成 SQL，就很难继续安全地保留业务权限。

Foggy Odoo Bridge 通过在查询执行前增加一层 Odoo 感知的权限与模型层来解决这个问题：

- 认证仍然留在 Odoo 中
- `ir.model.access` 控制哪些查询模型可见
- `ir.rule` 域规则会被转换为 DSL 切片条件
- 多公司边界仍由服务端强制执行
- 下游引擎接收到的是受治理的语义查询，而不是裸 SQL

## 你首先能得到什么

- 通过 MCP 在 Odoo 上做自然语言分析
- 在查询执行前注入 Odoo 权限规则
- 为 Claude Desktop 和 Cursor 提供 API Key 接入
- Odoo 内置 AI Chat
- 面向常见 Odoo 业务对象的内置 TM/QM 模型
- 权限解析失败时默认拒绝，避免放行
- 一个面向 Odoo 的可落地 AI 数据治理层

## 依赖说明

| 场景 | Odoo 环境中需要的额外 Python 包 |
|---|---|
| 仅作为 MCP 服务使用，不启用内置 AI Chat | 无 |
| 内置 AI Chat + OpenAI 兼容 provider | `openai` |
| 内置 AI Chat + Anthropic / Claude | `anthropic` |

说明：

- 如果你只是把这个插件作为 Claude Desktop、Cursor 或其他 MCP 客户端的 MCP 服务使用，那么 Odoo 环境中**不需要** `openai` 或 `anthropic`
- `openai` / `anthropic` 只是内置 AI Chat 功能的可选 SDK 依赖

## 数据库支持范围

当前这个 Odoo Bridge 版本的正式支持范围是：

- 目前已验证的数据库只有 PostgreSQL
- 当前版本还**不把 MySQL 作为正式支持项**
- 如果后续开放 MySQL 支持，更合理的节奏是放到 `v1.1`，并单独完成验证

这个判断和当前向导流程、SQL 资源、测试覆盖以及实际验证环境保持一致，它们目前都围绕 Odoo + PostgreSQL。

## 快速开始

### 使用设置向导（推荐）

1. **安装 Odoo 插件**：将 `foggy_mcp/` 复制到 Odoo addons 路径并完成安装
2. **打开设置向导**：`Settings -> Foggy MCP -> Setup Wizard`
3. **按步骤完成**：初始化闭包表 → 创建 API Key → 完成

无需部署外部服务 — 查询引擎运行在 Odoo 进程内。

### 内置 AI Chat 的依赖

内置 AI Chat 是可选功能。

- 使用 OpenAI 兼容 provider：`pip install openai`
- 使用 Anthropic / Claude：`pip install anthropic`
- 如果你不用 AI Chat，可以完全跳过这两个包

### 生成 API Key

1. 进入 `Settings -> Foggy MCP -> API Keys`
2. 点击 `Create`
3. 复制生成的 `fmcp_xxx` key

### 连接 Claude Desktop

在 `claude_desktop_config.json` 中加入：

```json
{
  "mcpServers": {
    "odoo": {
      "url": "https://your-odoo.com/foggy-mcp/rpc",
      "headers": {
        "Authorization": "Bearer fmcp_your_key_here"
      }
    }
  }
}
```

然后你就可以问这样的问题：

- “本月销售额最高的客户是谁？”
- “哪些采购订单已经延期？”
- “按仓库统计本周的库存调拨情况”
- “哪些发票仍然未付款？”

## 最适合先落地的场景

- 销售分析
- 采购分析
- 发票与账单查询
- 库存调拨报表
- 员工目录查询
- 客户与伙伴分析

## 它和普通集成的差异

很多集成只解决“LLM 能接到 Odoo 数据”的问题。这个项目关注的是“受治理的数据访问”，而不是单纯连通性。

- 它在查询执行前保留 Odoo 的授权语义
- 它把 ERP 模型映射成业务友好的语义查询模型
- 它让 AI 客户端远离原始 SQL 和裸 schema 提示
- 它的目标是真实内部部署，而不是只做演示

## 架构

```text
AI Client -> MCP -> Odoo (foggy_mcp 插件) -> Foggy 语义引擎 -> PostgreSQL
```

- **Odoo MCP Gateway**（`foggy_mcp/`）：负责 MCP 协议、认证、API Key、权限解析和 `payload.slice` 注入
- **内嵌 Foggy 引擎**：语义查询引擎，内置 Odoo TM/QM 模型，运行在 Odoo 进程内

## 支持的 Odoo 模型

| Odoo 模型 | QM 名称 | 说明 |
|---|---|---|
| `sale.order` | OdooSaleOrderQueryModel | 销售分析 |
| `sale.order.line` | OdooSaleOrderLineQueryModel | 销售明细 |
| `purchase.order` | OdooPurchaseOrderQueryModel | 采购分析 |
| `account.move` | OdooAccountMoveQueryModel | 发票与账单 |
| `stock.picking` | OdooStockPickingQueryModel | 库存调拨 |
| `hr.employee` | OdooHrEmployeeQueryModel | 员工目录 |
| `res.partner` | OdooResPartnerQueryModel | 伙伴目录 |

## 核心特性

- **自然语言查询**：直接用业务语言提问并得到结构化结果
- **行级安全**：Odoo `ir.rule` 会被转换成 `payload.slice` 中的 DSL 条件
- **按用户过滤工具**：`ir.model.access` 控制查询模型可见性
- **多公司支持**：公司隔离在服务端强制执行
- **API key 认证**：在 Odoo 中创建给 Claude Desktop / Cursor 使用的 key
- **失败即关闭**：权限处理失败时默认拒绝而不是放行

## 安全模型

### 认证

- **API Key**：`Authorization: Bearer fmcp_xxx`
- **Session**：Odoo Web 会话 Cookie

### 授权流程（按每次 tools/call）

```text
1. 用户完成认证（API key -> uid）
2. 对 dataset.query_model 调用：
   a. 从参数中读取模型名
   b. 预检查 ir.model.access 的读权限
   c. 把 QM 名称映射为 Odoo 模型（例如 OdooSaleOrderQueryModel -> sale.order）
   d. 读取该用户在该模型上的 ir.rule（全局规则 + 组规则）
   e. 计算 domain_force（解析 user.id、company_ids 等）
   f. 解析 domain（前缀表达式 AST -> AND/OR/NOT 树）
   g. 展平为 DSL slice 条件（支持 $or / $and 嵌套）
   h. 注入到 arguments.payload.slice
3. 转发到 Foggy MCP Server，由 DSL 引擎原生处理这些 slice
```

### 权限桥接：Domain 解析

Odoo 的 `ir.rule` domain 使用前缀表达式。权限桥接完整支持以下模式：

| Domain 模式 | DSL 输出 | 示例 |
|---|---|---|
| `('field', '=', value)` | `{"field": "x", "op": "=", "value": v}` | 多公司、本人记录 |
| `['|', A, B]` | `{"$or": [A, B]}` | 本人或未分配 |
| `['!', A]` | 否定条件 | 排除已取消 |
| `['&', A, '|', B, C]` | `[A, {"$or": [B, C]}]` | 公司 +（本人或未分配） |
| `['!', '|', A, B]` | 德摩根转换：`[NOT(A), NOT(B)]` | 两者都不是 |
| `['!', '&', A, B]` | 德摩根转换：`{"$or": [NOT(A), NOT(B)]}` | 至少一个取反 |
| OR 中嵌套 AND | `{"$or": [{"$and": [A, B]}, C]}` | 复杂组规则 |

**Odoo 规则语义保持一致：**

- 全局规则（`groups=False`）之间使用 AND
- 组规则（`groups=specific`）之间使用 OR，然后再与全局规则做 AND
- 最终语义：`global1 AND global2 AND (group_rule1 OR group_rule2)`

### 注入到 `payload.slice` 的 DSL 格式

```json
[
  {"field": "company_id", "op": "in", "value": [1, 3]},
  {"$or": [
    {"field": "user_id", "op": "=", "value": 42},
    {"field": "user_id", "op": "is null"}
  ]}
]
```

### 支持的过滤操作符

| DSL 操作符 | SQL | 来源于 Odoo |
|---|---|---|
| `=` | `= ?` | `=` |
| `!=` | `!= ?` | `!=` |
| `>`, `>=`, `<`, `<=` | `>`, `>=`, `<`, `<=` | 同名 |
| `in` | `IN (?, ...)` | `in` |
| `not in` | `NOT IN (?, ...)` | `not in` |
| `is null` | `IS NULL` | `= False` |
| `is not null` | `IS NOT NULL` | `!= False` |
| `like` | `LIKE ?` | `like`, `ilike` |

## 测试

```bash
# Run permission bridge unit tests (no Odoo runtime needed)
cd addons/foggy-odoo-bridge
python -m pytest tests/test_permission_bridge.py -v
```

当前共有 45 个测试，覆盖：AST 解析、叶子节点转换、操作符取反、德摩根规则、`$or`/`$and` 嵌套、`payload` 注入模拟，以及真实 Odoo domain 场景。

## 如何扩展自定义模型

自定义 TM/QM 模型可以通过两种方式接入：

### 方式 1：加入 `foggy-odoo-bridge-java` 模块

把你的 TM/QM 文件加入 `foggy-odoo-bridge-java` 的资源目录，然后重新构建 Docker 镜像。

### 方式 2：外部 Bundle（高级）

挂载一个外部 bundle 目录，并让 Foggy 在启动时加载它：

```bash
java -jar foggy-mcp-launcher.jar \
  --spring.profiles.active=lite \
  --foggy.bundle.external.enabled=true \
  --foggy.bundle.external.bundles[0].name=custom-models \
  --foggy.bundle.external.bundles[0].path=/path/to/custom-models \
  --foggy.bundle.external.bundles[0].namespace=custom
```

### 创建 TM 文件（`model/MyCustomModel.tm`）

```javascript
export const model = {
    name: 'MyCustomModel',
    caption: 'My Custom Table',
    tableName: 'my_table',
    idColumn: 'id',
    dimensions: [/* ... */],
    properties: [/* ... */],
    measures: [/* ... */]
};
```

### 2. 创建 QM 文件（`query/MyCustomQueryModel.qm`）

```javascript
const m = loadTableModel('MyCustomModel');

export const queryModel = {
    name: 'MyCustomQueryModel',
    caption: 'My Custom Query',
    loader: 'v2',
    model: m,
    columnGroups: [/* ... */],
    accesses: []  // permissions via payload.slice injection
};
```

### 3. 在 `tool_registry.py` 中加入模型映射

```python
MODEL_MAPPING = {
    # ...existing...
    'my.custom.model': 'MyCustomQueryModel',
}
```

### 4. 重启 Foggy MCP Server 以重新加载模型

## 升级模块

更新 `foggy_mcp` 文件后，必须执行模块升级 — 仅重启容器不够：

```bash
docker exec foggy-odoo bash -c \
  "odoo -d <数据库名> -u foggy_mcp --stop-after-init \
   --db_host=<PG容器名> --db_port=5432 --db_user=odoo --db_password=odoo"
docker restart foggy-odoo
```

详见[安装指南](INSTALL_GUIDE.zh-CN.md#升级模块)。

## 配置项

| 参数 | 默认值 | 说明 |
|---|---|---|
| `foggy_mcp.server_url` | `http://foggy-mcp:8080` | Foggy MCP Server URL |
| `foggy_mcp.endpoint_path` | `/mcp/analyst/rpc` | MCP JSON-RPC 端点路径 |
| `foggy_mcp.request_timeout` | `30` | HTTP 超时（秒） |
| `foggy_mcp.namespace` | `odoo` | 模型命名空间 |
| `foggy_mcp.cache_ttl` | `300` | 工具缓存 TTL（秒） |

## 许可证

Apache License 2.0
