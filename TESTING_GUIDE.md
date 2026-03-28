# Foggy Odoo Bridge — 测试与体验手册

> 涵盖安装验证、功能体验、AI Chat 对话、外部客户端接入、三引擎对比、权限验证。

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
# 确认 status=ok, engine.mode=embedded, tool_count>=3, models 含 9 个模型
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
| AI 对话 | 提供商下拉 / API 密钥 / 模型名称 / 基础地址 / 温度 | ✓ |
| 业务上下文 | 多行文本框，placeholder 含示例 | ✓|
| LLM 提供商选项 | OpenAI / Anthropic / DeepSeek / Ollama / 自定义 | ✓ |

---

## 检查项体验说明
建议选择网关模式，加入网关模式的说明，及场景，比如独立布署的优势，及需要一定的技术和动手能力
在切换引擎模式或修改了任一输入框后，点击启动设置向导，或测试连接，都会触发保存机制，保存后会刷新页面，还回不到Foggy MCP设置页，这里体验不友好，这两个应该是不需要保存就可以独立触发的操作

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

---

## 网关模式向导体验说明
部署步骤中，从 Odoo 配置自动读取（只读）卡片中的内容有换行，如数据库名称，odoo_demo这个词展示了4行
数据源是步骤中的"数据源配置预览"卡片同样

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

## 四、Foggy AI 对话

### 4.1 导航

主导航栏 → **Foggy AI** → AI 对话
（也可通过 Settings → Foggy MCP → Foggy AI 对话 进入）

### 4.2 界面检查

| 检查项 | 预期 | ✓ |
|--------|------|---|
| 左侧面板 | 历史对话列表 + "New Chat" 按钮 | ✓ |
| 欢迎区 | "Foggy AI Data Analyst" 标题 + 预设问题按钮 | ✓ |
| 输入框 | placeholder "询问你的业务数据..." | ✓ |

### 4.3 LLM 配置（各提供商）

| 提供商 | Provider | Model | 其他 |
|--------|----------|-------|------|
| OpenAI | `OpenAI` | `gpt-4o-mini` 或 `gpt-4o` | |
| DeepSeek | `DeepSeek` | `deepseek-chat` | |
| Ollama | `Ollama（本地）` | `llama3` / `qwen2.5` | Base URL: `http://host.docker.internal:11434/v1` |
| 自定义 | `自定义（OpenAI 兼容）` | 按提供商填 | 填写 Base URL |

### 4.4 对话测试

| 问题 | 预期 | ✓ |
|------|------|---|
| "公司有几家？" | 返回 2 家公司 | ✓ |
| "各部门分别有多少员工？" | 按部门分组 → 7 个部门 | ✓ |
| "列出所有在职员工，包含姓名和部门" | 约 20 人 | ✓ |
| "本月销售订单总金额是多少？" | 按日期过滤（AI 知道当前日期） | ✓ |
| "最近5笔销售订单？显示订单号、客户、金额" | 表格，客户名称正确 | ✓ |
| "哪个客户的销售总额最高？" | 按 partner 分组 SUM 排序 | ✓ |
| "当前各阶段有多少商机？预期收入？" | 按 stage 分组统计 CRM | ✓ |
| "其中研发部门有多少人？" | 使用 like 或 selfAndDescendantsOf | ✓ |

### 4.5 非管理员用户（demo 账号）

**登录信息**：`demo` / `demo`（uid=6）
**API Key**：`fmcp_IZuGqHwLoRQJc7kjVRyrKu2nmIWrU9wmH0xfnInF`

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

> ⚠️ **已知问题**：当前 `FieldMappingRegistry` 的动态 column_map 映射 `company_id` → `company`（缺少 `$id` 后缀），导致部分 group rule 的 slice 注入字段名不正确（如 `salesperson` 而非 `salesperson$id`），查询引擎无法正确过滤。待修复后 demo 的 sale.order 和 crm.lead 数量应与预期一致。

| 检查项 | 预期 | ✓ |
|--------|------|---|
| 无 Settings 菜单 | 但可通过 Foggy AI 菜单进入对话 | ✓ |
| My API Key | Foggy AI → 我的 API 密钥，可看到自己的密钥 | ✓ |
| 销售订单数量 | 应为 17 条（修复权限后） | ✓ |
| CRM 商机数量 | 应为 23 条（修复权限后） | ✓ |
| 员工数量 | 20 条（与 admin 相同） | ✓ |


---

## 五、MCP 端点验证

### 5.1 健康检查

```bash
curl http://localhost:8069/foggy-mcp/health | python -m json.tool
```

预期：`status: "ok"`, `engine.mode: "embedded"`, `tool_count >= 3`, `models: 9 个`

### 5.2 直接查询

```bash
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer fmcp_YOUR_KEY' \
  -d '{
    "jsonrpc": "2.0", "id": 1,
    "method": "tools/call",
    "params": {
      "name": "dataset.query_model",
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
    "odoo": {
      "url": "http://localhost:8069/foggy-mcp/rpc",
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
    "odoo": {
      "url": "http://localhost:8069/foggy-mcp/rpc",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

### 6.3 Cherry Studio

MCP Server → 添加：类型 `Streamable HTTP`，URL `http://localhost:8069/foggy-mcp/rpc`，Header `Authorization: Bearer <key>`

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
3. AI Chat 提问 → 返回数据

#### Java 网关模式

1. 启动 Java MCP Server（port 7108）
2. Settings → 网关模式 → URL = `http://host.docker.internal:7108`
3. **Namespace = `odoo`**（Java 必须，通过 X-NS header）
4. Save → 健康检查 `mode: gateway` → AI Chat 结果一致

#### Python 网关模式

1. 启动 Python MCP Server（port 8066）
2. Settings → URL = `http://host.docker.internal:8066`
3. **Namespace = 留空**（Python 不需要）
4. Save → 健康检查 `mode: gateway` → AI Chat 结果一致

#### 测试完成后

**务必切回内嵌模式。**

---

## 八、权限验证

### 8.1 权限模型

```
Admin 用户
  ├─ ir.model.access: 可读全部 9 个模型
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

## 十、验收 Checklist

**Settings & Wizard**：
- [x] Foggy MCP Tab 布局正确，radio/按钮不换行
- [x] 引擎模式切换正常
- [x] 向导流程完整（内嵌 3 步 / 网关 6 步）

**API 密钥**：
- [x] 创建、列表、认证正常

**AI Chat**：
- [x] 界面完整，LLM 配置正确
- [x] 基础查询返回数据
- [x] 聚合分析正确
- [x] 日期过滤正确（本月销售额）
- [x] CRM 商机查询正确
- [x] 多轮对话上下文连续

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
