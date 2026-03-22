# Foggy Odoo Bridge — 手工测试引导

> 三引擎模式完整测试指南（v1.0 发布前验收）

## 前置条件

| 组件 | 状态确认 |
|------|---------|
| Docker Desktop | 运行中 |
| PostgreSQL (foggy-odoo-postgres) | `docker ps` 确认 healthy |
| Odoo 17 (foggy-odoo) | http://localhost:8069 可访问 |
| Java MCP Server (可选) | port 7108，用 `/java-mcp` skill 启动 |
| Python MCP Server (可选) | port 8066，用 `/python-mcp` skill 启动 |

---

## 一、Settings 页面验证

### 1.1 导航路径

Settings → 顶部 Tab 栏 → **Foggy MCP**

### 1.2 检查项

| 检查项 | 预期 | 实际 |
|--------|------|------|
| 顶部 Tab 栏只有一个 "Foggy MCP" | 无重复菜单 | □ |
| 引擎模式区域 | 两个 radio：内嵌模式（推荐，默认选中）/ 网关模式 | □ |
| 选中内嵌时 | 显示提示 "内嵌模式：查询引擎在 Odoo 进程内运行" | □ |
| 选中网关时 | 出现服务器 URL / Namespace / 超时配置 | □ |
| 快速设置 | "启动设置向导" 按钮 | □ |
| AI 对话（LLM 配置） | 提供商下拉 / API 密钥 / 模型名称 / API 基础地址 / 温度 / 业务上下文 | □ |

### 1.3 LLM 提供商选项

- [ ] OpenAI
- [ ] Anthropic (Claude)
- [ ] DeepSeek
- [ ] Ollama（本地）
- [ ] 自定义（OpenAI 兼容）

---

## 二、Setup Wizard（设置向导）验证

### 2.1 内嵌模式向导

Settings → Foggy MCP → 启动设置向导

| 步骤 | 预期 | 实际 |
|------|------|------|
| 步骤 1：欢迎 | 显示引擎模式选择，内嵌默认选中 | □ |
| 步骤 2：闭包表 | 显示闭包表初始化选项 | □ |
| 步骤 3：完成 | 显示健康检查结果 | □ |
| 总步骤数 | 3 步（欢迎→闭包表→完成） | □ |

### 2.2 网关模式向导

在步骤 1 切换为网关模式后：

| 步骤 | 预期 | 实际 |
|------|------|------|
| 总步骤数 | 6 步（欢迎→部署→连接→数据源→闭包表→完成） | □ |
| 部署步骤 | Docker Compose 配置说明 | □ |
| 连接步骤 | 服务器 URL 测试 | □ |
| 数据源步骤 | PostgreSQL 数据源配置 | □ |

---

## 三、API 密钥管理验证

### 3.1 导航路径

Settings → Foggy MCP 菜单 → **MCP API 密钥**

### 3.2 检查项

| 检查项 | 预期 | 实际 |
|--------|------|------|
| 列表页标题 | "我的 API 密钥" | □ |
| 表头 | 名称 / 用户 / 密钥预览 / 启用 / 最后使用 / Created on | □ |
| 自动生成的密钥 | 有一条 `fmcp_` 开头的密钥 | □ |
| "New" 按钮 | 可创建新密钥 | □ |

### 3.3 API 密钥功能测试

```bash
# 获取密钥后测试 MCP 端点
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer fmcp_YOUR_KEY' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"dataset.get_metadata","arguments":{}}}'
```

---

## 四、AI Chat 对话验证

### 4.1 导航路径

Settings → Foggy MCP 菜单 → **Foggy AI 对话**

### 4.2 界面检查

| 检查项 | 预期 | 实际 |
|--------|------|------|
| 左侧面板 | 历史对话列表 + "New Chat" 按钮 | □ |
| 右侧欢迎区 | "Foggy AI Data Analyst" 标题 + 预设问题按钮 | □ |
| 预设问题 | Top customers / Monthly sales / CRM pipeline | □ |
| 输入框 | placeholder "询问你的业务数据..." | □ |
| 快捷键提示 | "Press Enter to send, Shift+Enter for new line" | □ |

### 4.3 对话功能测试（需要 LLM 配置正确）

依次测试以下问题，确认 AI 能调用 MCP 工具查询数据：

| 问题 | 预期行为 | 实际 |
|------|---------|------|
| "公司有几家？" | 调用 dataset.query_model → OdooResCompanyQueryModel → 返回 2 | □ |
| "各部门分别有多少员工？" | 调用 query_model → OdooHrEmployeeQueryModel → 7 个部门 | □ |
| "列出所有在职员工，包含姓名和部门" | 调用 query_model → 返回 20 名员工 | □ |
| "本月销售订单总金额是多少？" | 调用 query_model → OdooSaleOrderQueryModel → amountTotal | □ |

---

## 五、MCP 端点验证

### 5.1 健康检查

```bash
curl http://localhost:8069/foggy-mcp/health | python -m json.tool
```

预期返回：
```json
{
  "status": "ok",
  "checks": {
    "engine": {"status": "ok", "mode": "embedded"},
    "tool_cache": {"status": "ok", "tool_count": 6}
  }
}
```

### 5.2 工具列表

```bash
# 使用 Session Cookie（登录后）或 API Key
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer fmcp_YOUR_KEY' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

预期工具：`dataset.query_model`, `dataset.get_metadata`, `dataset.describe_model_internal` 等

---

## 六、三引擎对比测试

> 所有测试通过 Odoo MCP 端点 `/foggy-mcp/rpc`，确保权限注入一致。

### 6.1 自动化测试

```bash
# 确保三个服务都运行
python tests/e2e/run_comparison_via_odoo.py
```

预期：15/15 全部通过，所有行数完全一致（`All rows match: True`）。

### 6.2 手动引擎切换测试

#### 内嵌模式（默认）

1. Settings → Foggy MCP → 引擎模式 = 内嵌模式 → Save
2. 健康检查：`curl http://localhost:8069/foggy-mcp/health` → `mode: embedded`
3. AI Chat 提问 "公司有几家？" → 返回 2

#### Java 网关模式

1. 启动 Java MCP Server（port 7108）
2. Settings → 引擎模式 = 网关模式
3. 服务器 URL = `http://host.docker.internal:7108`
4. **Namespace = `odoo`**（Java 引擎必须传 namespace，通过 `X-NS` header）
5. Save → 健康检查 → `mode: gateway`
6. AI Chat 提问同样问题 → 结果应与内嵌一致

#### Python 网关模式

1. 启动 Python MCP Server（port 8066）
2. Settings → 服务器 URL = `http://host.docker.internal:8066`
3. **Namespace = 留空**（Python 引擎不需要 namespace）
4. Save → 健康检查 → `mode: gateway`
5. AI Chat 提问同样问题 → 结果应与内嵌一致

#### 测试完成后

**务必切回内嵌模式。**

---

## 七、权限验证

### 7.1 非管理员用户

1. 创建一个普通用户（仅 Sales / Employee 权限）
2. 用该用户登录
3. 调用 MCP 端点 → 工具列表应仅包含该用户有权限的模型
4. 查询应自动注入 ir.rule 权限过滤（行级安全）

### 7.2 API 密钥认证

```bash
# 用 fmcp_ 密钥直接调用（无需 Session Cookie）
curl -X POST http://localhost:8069/foggy-mcp/rpc \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer fmcp_YOUR_KEY' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

---

## 八、已知问题

| 问题 | 状态 | 备注 |
|------|------|------|
| Setup Wizard 显示 "foggy-python 未安装" | 正常 | vendored 版本可用，pip 安装仅影响提示 |
| T01/T16 res_partner 返回 0 行 | 正常 | admin 的 ir.rule 多公司权限过滤结果 |
| "Created on" 列头未翻译 | 小问题 | Odoo 原生字段，非插件控制 |

---

## 九、验收 Checklist

- [ ] Settings → Foggy MCP Tab 布局正确，无重复菜单
- [ ] 引擎模式切换正常（内嵌/网关）
- [ ] Setup Wizard 内嵌 3 步 / 网关 6 步流程完整
- [ ] API 密钥创建、列表、认证正常
- [ ] AI Chat 界面完整，对话可用
- [ ] MCP 健康检查返回 ok
- [ ] 三引擎对比 15/15 通过，行数一致
- [ ] 权限过滤正常（ir.rule → slice）
- [ ] 引擎切回内嵌模式
