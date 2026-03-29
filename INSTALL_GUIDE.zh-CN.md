# Foggy MCP Gateway - 安装指南

[English](./INSTALL_GUIDE.md) | [简体中文](./INSTALL_GUIDE.zh-CN.md)

> 连接 Odoo 与 AI 客户端的数据桥梁，让 AI 安全地查询 Odoo 业务数据。

## 前置条件

- Odoo 17 已安装并正常运行
- 当前 Odoo 数据库为 PostgreSQL

当前版本的支持范围：

- 目前这个 Odoo Bridge 已验证的数据库是 PostgreSQL
- 当前版本还**不把 MySQL 作为正式支持项**
- 如果后续要提供 MySQL 支持，更合理的做法是放到 `v1.1`，并单独完成环境验证

## 依赖说明

按使用方式区分：

| 场景 | Odoo Python 环境额外依赖 |
|---|---|
| 仅作为独立 MCP 服务使用，不启用内置 AI Chat | 无 |
| 启用内置 AI Chat，使用 OpenAI 兼容接口 | `openai` |
| 启用内置 AI Chat，使用 Anthropic / Claude | `anthropic` |
| 使用内嵌引擎模式 | `foggy-python` |

说明：

- 如果你只是把本插件作为独立 MCP 服务，供 Claude Desktop、Cursor 等外部 AI 客户端连接，则 **不需要** 安装 `openai` 或 `anthropic`
- `openai` / `anthropic` 仅在使用 Odoo 内置 **AI Chat** 功能时才需要
- 如果使用网关模式连接外部 Foggy Java / Python 服务，Odoo 侧也不需要额外安装这两个 LLM SDK

---

## 安装步骤

### 1. 下载插件

```bash
curl -LO https://github.com/foggy-projects/foggy-data-mcp-bridge/releases/download/main/foggy-odoo-addon.zip
unzip foggy-odoo-addon.zip   # 解压为 foggy_mcp/
```

### 2. 安装到 Odoo

**方式 A：Docker 部署**

```yaml
# docker-compose.yml 添加卷挂载
volumes:
  - ./foggy_mcp:/mnt/extra-addons/foggy_mcp:ro
```

**方式 B：本地安装**

```bash
cp -r foggy_mcp /path/to/odoo/addons/
```

### 3. 启用模块

1. Odoo -> **Settings -> Technical -> Apps**
2. 点击 **Update Apps List**
3. 搜索 `foggy_mcp` -> 点击 **Install**

> 如果只使用独立 MCP 服务能力，到这里仍然不需要安装 `openai` / `anthropic`。

---

## Setup Wizard 配置向导

安装完成后，进入 **Settings -> Foggy MCP -> Setup Wizard**，按向导完成配置。

### Step 1: 选择引擎模式

| 方式 | 说明 |
|---|---|
| **Embedded** | 查询引擎运行在 Odoo 进程内，无需单独部署外部 Foggy 服务 |
| **Gateway** | 将请求转发到外部 Foggy 服务，可接 Java 或 Python 引擎 |

### Step 2: 服务器配置

如果选择 **Gateway** 模式，向导会自动检测 Odoo 数据库连接信息并生成部署配置：

- **Docker 方式**：复制生成的 `docker-compose.yml`，然后执行 `docker compose up -d`
- **Manual 方式**：复制生成的 `java -jar` 命令并执行

如果选择 **Embedded** 模式，则无需部署外部服务；只需保证 Odoo Python 环境已安装 `foggy-python`。

> 模型文件已经内嵌在插件或 Foggy 引擎中，无需额外下载。

### Step 3: 初始化闭包表

点击 **Initialize Closure Tables**，用于支持层级查询（例如公司树、部门树）。

### Step 4: 测试连接

点击 **Test Connection**，验证 Foggy MCP Server 是否可以正常访问。

### Step 5: 创建 API Key

点击 **Finish**，继续进入 API Key 页面。

---

## 可选：启用内置 AI Chat

只有在你要直接在 Odoo 内使用 **Foggy AI Chat** 时，才需要安装 LLM SDK：

```bash
# OpenAI / DeepSeek / Ollama / 其他 OpenAI 兼容接口
pip install openai

# Anthropic / Claude
pip install anthropic
```

如果不使用 AI Chat，可跳过本节。

---

## 创建 API Key

1. 进入 **Foggy MCP -> API Keys**
2. 点击 **Create** -> 选择用户 -> **Generate Key**
3. 保存 API Key（格式：`fmcp_xxxxxxxxxxxx`，通常只显示一次）

---

## 验证 MCP 端点

```bash
# 替换 YOUR_API_KEY
curl -s http://localhost:8069/foggy-mcp/rpc \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{
    "jsonrpc": "2.0", "id": 1,
    "method": "tools/list",
    "params": {}
  }' | python3 -m json.tool
```

---

## 接入 AI 客户端

### Claude Desktop / Cursor

```json
{
  "mcpServers": {
    "foggy-odoo": {
      "url": "http://localhost:8069/foggy-mcp/rpc",
      "headers": {
        "Authorization": "Bearer YOUR_API_KEY"
      }
    }
  }
}
```

自然语言查询示例：

- “显示最近 10 笔销售订单”
- “按客户汇总销售额”
- “本月新建了多少采购单”

---

## 架构说明

```text
AI Client
    │ MCP Protocol
    ▼
Odoo (foggy_mcp 插件)
    │ 权限过滤 (ir.rule -> DSL slice)
    ▼
Foggy MCP Server
    │ SQL
    ▼
PostgreSQL (Odoo 数据库)
```

安全机制：

- 用户只能查询自己有权限的数据
- 权限条件由服务端注入，客户端无法绕过
- Fail-closed：权限解析失败时默认拒绝访问

---

## 故障排查

### Foggy MCP Server 连接失败

1. 确认服务运行：`curl http://localhost:7108/actuator/health`
2. 检查数据库连接配置是否正确
3. 检查防火墙和网络策略

### 闭包表初始化失败

确保 Odoo 数据库用户有创建表的权限。

### 查询返回空数据

1. 确认用户拥有对应模型的访问权限
2. 确认数据库中确实存在业务数据
