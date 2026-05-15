# Foggy Odoo Bridge — 测试与体验手册

> 涵盖安装验证、功能体验、外部客户端接入、三引擎对比、权限验证。

## 前置条件

| 组件 | 确认方式 |
|------|---------|
| Docker Desktop | 运行中 |
| PostgreSQL (foggy-odoo-postgres) | `docker ps` 确认 healthy |
| Odoo 17 (foggy-odoo) | http://localhost:8069 可访问 |
| Java MCP Server（可选） | port 7108，三引擎对比时启动 |
| Python MCP Server（可选） | port 8066，三引擎对比时启动 |

```bash
# 健康检查
curl -s http://localhost:8069/foggy-mcp/health | python -m json.tool
# 确认 status=ok, engine.mode=embedded, tool_count>=3, models 含 12 个模型
```

**登录信息**：

| 字段 | 值 |
|------|---|
| Database | `odoo_demo` |
| Email | `admin` |
| Password | `admin` |

---

## 一、Settings 页面

### 1.1 导航

Settings → 顶部 Tab → **Foggy MCP**

### 1.2 检查项

| 检查项 | 预期 | ✓ |
|--------|------|---|
| Tab 栏只有一个 "Foggy MCP" | 无重复菜单 | ✓ |
| 引擎模式 | 两个 radio：内嵌模式（推荐，默认选中）/ 网关模式，标签不换行 | ✓ |
| 选中内嵌 | 提示"查询引擎在 Odoo 进程内运行" | ✓ |
| 选中网关 | 出现服务器 URL / Namespace / 超时配置 | ✓ |
| 快速设置 | "启动设置向导"按钮可点击 | ✓ |
| AI Chat / LLM 配置 | Community Edition 不显示内置 AI Chat、LLM provider、LLM API Key 等配置 | ✓ |

## 二、Setup Wizard（设置向导）

Settings → Foggy MCP → 启动设置向导

### 2.1 内嵌模式向导

| 步骤 | 预期 | ✓ |
|------|------|---|
| 欢迎 | 引擎模式 radio，内嵌默认选中，标签不换行 | ✓ |
| 闭包表 | "初始化闭包表"按钮不换行，可点击 | ✓ |
| 完成 | 显示完成信息 | ✓ |
| 总步骤数 | 3 步 | ✓ |

### 2.2 网关模式向导

在欢迎步骤切换为网关模式后：

| 步骤 | 预期 | ✓ |
|------|------|---|
| 总步骤数 | 6 步（欢迎→部署→连接→数据源→闭包表→完成） | ✓ |
| 部署 | Docker 命令生成，按钮不换行 | ✓ |
| 连接 | 服务器 URL 测试 | ✓ |
| 数据源 | PostgreSQL 配置预览 | ✓ |

## 三、API 密钥管理

### 3.1 导航

Settings → Foggy MCP → **MCP API 密钥**

### 3.2 检查项

| 检查项 | 预期 | ✓ |
|--------|------|---|
| 列表页 | 名称 / 用户 / 密钥预览 / 启用 / 最后使用 / 创建时间 | ✓ |
| 自动生成的密钥 | 有一条 `fmcp_` 开头的密钥 | ✓ |
| 点击进入详情 | 可复制 Key，看到 MCP 配置说明 | ✓ |
| "New" 按钮 | 可创建新密钥 | ✓ |

### 3.3 API 密钥认证测试

```bash
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer fmcp_YOUR_KEY' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

---

## 四、Community AI Chat 边界

Community Edition 不提供 Odoo 内置 AI Chat。验证目标是确认不存在可见入口或运行时配置残留。

| 检查项 | 预期 | ✓ |
|--------|------|---|
| 主导航栏 | 不显示 **Foggy AI** / AI Chat 入口 | ✓ |
| Settings → Foggy MCP | 不显示 LLM provider、LLM API Key、模型名、Base URL、Temperature、Custom Prompt | ✓ |
| Odoo Python 环境 | 不要求安装 `openai` / `anthropic` | ✓ |
| 外部客户端 | 仍通过 `/foggy-mcp/rpc` 使用 API Key 查询 | ✓ |

### 4.1 非管理员用户（demo 账号）

**登录信息**：`demo` / `demo`（uid=6）
**API Key**：请在本地环境中为该用户自行生成测试 Key

**demo 的角色**：
- Sales: Own Documents Only（只看自己的销售订单）
- CRM: Own Documents Only（只看自己的商机）
- HR: Officer（看所有员工）
- Purchase/Inventory/Invoicing: User/Billing（看所有）

**权限对比（admin vs demo 预期可见数据量）**：

| 模型 | admin | demo | 差异原因 |
|------|:---:|:---:|------|
| sale.order | 24 | **17** | Personal Orders: user_id=6 或 NULL |
| crm.lead | 44 | **23** | Personal Leads: user_id=6 或 NULL |
| hr.employee | 20 | 20 | Officer 看所有 |
| purchase.order | 11 | 11 | User 看所有 |
| stock.picking | 25 | 25 | User 看所有 |
| account.move | 24 | 24 | Billing + All Invoices |

| 检查项 | 预期 | ✓ |
|--------|------|---|
| 无 Settings 菜单 | 无管理配置入口 | ✓ |
| API Key | 管理员为 demo 生成 API Key 后可通过 MCP 端点验证权限 | ✓ |
| 销售订单数量 | 应为 17 条（修复权限后） | ✓ |
| CRM 商机数量 | 应为 23 条（修复权限后） | ✓ |
| 员工数量 | 20 条（与 admin 相同） | ✓ |


---

## 五、MCP 端点验证

### 5.1 健康检查

```bash
curl http://localhost:8069/foggy-mcp/health | python -m json.tool
```

预期：`status: "ok"`, `engine.mode: "embedded"`, `tool_count >= 3`, `models: 12 个`

### 5.2 直接查询

```bash
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer fmcp_YOUR_KEY' \
  -d '{
    "jsonrpc": "2.0", "id": 1,
    "method": "tools/call",
    "params": {
      "name": "dataset__query_model",
      "arguments": {
        "model": "OdooSaleOrderQueryModel",
        "payload": {
          "columns": ["name", "partner$caption", "amountTotal"],
          "limit": 5
        }
      }
    }
  }' | python -m json.tool
```

---

## 六、外部 AI 客户端配置

### 6.1 Claude Desktop

编辑 `claude_desktop_config.json`：

```json
{
  "mcpServers": {
    "foggy-odoo": {
      "url": "http://localhost:8077/foggy-mcp/rpc",
      "headers": {
        "Authorization": "Bearer fmcp_YOUR_KEY"
      }
    }
  }
}
```

### 6.2 Cursor / VS Code

`.cursor/mcp.json`：

```json
{
  "mcpServers": {
    "foggy-odoo": {
      "url": "http://localhost:8077/foggy-mcp/rpc",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

### 6.3 示例问题（英文）

- "List the available Odoo query models."
- "Show the latest 10 sales orders with customer names and total amounts."
- "Summarize sales order revenue by customer."
- "Which invoices are still unpaid?"
- "Show recent inventory transfers by status."

### 6.4 Cherry Studio

MCP Server → 添加：类型 `Streamable HTTP`，URL `http://localhost:8077/foggy-mcp/rpc`，Header `Authorization: Bearer <key>`

---

## 七、三引擎对比测试

> 所有测试通过 Odoo MCP 端点 `/foggy-mcp/rpc`，确保权限注入一致。

### 7.1 自动化测试

```bash
python -m pytest tests/e2e/test_query_with_permissions.py tests/e2e/test_metadata_response.py -v
# 预期：20/20 通过
```

### 7.2 手动引擎切换

#### 内嵌模式（默认）

1. Settings → 引擎模式 = 内嵌模式 → Save
2. 健康检查确认 `mode: embedded`
3. 直接调用 MCP 端点 → 返回数据

#### Java 网关模式

1. 启动 Java MCP Server（port 7108）
2. Settings → 网关模式 → URL = `http://host.docker.internal:7108`
3. **Namespace = `odoo17`**（Java 必须，通过 X-NS header）
4. Save → 健康检查 `mode: gateway` → MCP 查询结果一致

#### Python 网关模式

1. 启动 Python MCP Server（port 8066）
2. Settings → URL = `http://host.docker.internal:8066`
3. **Namespace = `odoo17`**（与内嵌引擎模型命名空间保持一致）
4. Save → 健康检查 `mode: gateway` → MCP 查询结果一致

#### 测试完成后

**务必切回内嵌模式。**

---

## 八、权限验证

### 8.1 权限模型

```
Admin 用户
  ├─ ir.model.access: 可读全部 12 个模型
  ├─ ir.rule (global): company_id in user.company_ids
  └─ ir.rule (group): 按用户组过滤（OR 语义）
```

Gateway 自动将 ir.rule 解析为 DSL slice 注入查询，AI 客户端**无法绕过**。

### 8.2 验证要点

| 检查项 | 预期 | ✓ |
|--------|------|---|
| admin 用户查询 | 看到所有公司数据 | ✓ |
| 受限用户查询 | 自动注入 company_id 过滤 | ✓ |
| 无模型读权限用户 | tools/list 不含该模型工具 | ✓ |
| API Key 认证 | `fmcp_` 密钥可替代 Session Cookie | ✓ |

---

## 九、Demo 数据参考

| 模型 | 记录数 | 说明 |
|------|--------|------|
| sale.order | 24 | 草稿+已确认 |
| sale.order.line | ~50+ | 多行明细 |
| purchase.order | 11 | |
| account.move | 24 | 发票和账单 |
| stock.picking | 25 | |
| hr.employee | 20 | create_date 为 2010-01-01（Demo 假日期） |
| res.partner | 61 | 客户、供应商、联系人 |
| crm.lead | 44 | 4 个阶段 |
| res.company | 2 | 均无父公司 |

---

## 十、Odoo Apps 上架前静态检查

在准备 Community 上架包前执行：

```bash
bash scripts/check-no-pro-content.sh
bash scripts/check-model-drift.sh
bash scripts/sync-community-models.sh --dry-run
bash scripts/check-odoo-apps-readiness.sh
node scripts/generate-odoo-apps-assets.mjs
```

`check-odoo-apps-readiness.sh` 会检查 manifest 必填信息、免费 Community 定价、icon/banner/screenshots、`static/description/index.html` 外链和 JavaScript、AI/Pro runtime 残留、Python 依赖和 Apache 2.0 license 映射。`generate-odoo-apps-assets.mjs` 会重新生成商品首页图和 Playwright 截图，默认使用 `http://localhost:8077` / `community_smoke`。

---

## 十一、验收 Checklist

**Settings & Wizard**：
- [x] Foggy MCP Tab 布局正确，radio/按钮不换行
- [x] 引擎模式切换正常
- [x] 向导流程完整（内嵌 3 步 / 网关 6 步）

**API 密钥**：
- [x] 创建、列表、认证正常

**Community AI Chat 边界**：
- [x] 不显示内置 AI Chat 入口
- [x] 不显示 LLM provider/API key 配置
- [x] 不要求 `openai` / `anthropic` 依赖

**MCP 端点**：
- [x] 健康检查 ok
- [x] 工具列表正确
- [x] curl 直接查询有数据

**三引擎对比**：
- [x] 内嵌/Java/Python 结果一致
- [x] 回归测试 20/20 通过

**权限**：
- [x] ir.rule 自动注入
- [x] 非管理员用户数据受限

**最后**：
- [x] 引擎切回内嵌模式
- [x] Odoo Apps readiness 静态检查通过
