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

| 场景 | Odoo Python 环境额外依赖 |
|---|---|
| 仅作为 MCP 服务使用，不启用内置 AI Chat | 无 |
| 启用内置 AI Chat，使用 OpenAI 兼容接口 | `openai` |
| 启用内置 AI Chat，使用 Anthropic / Claude | `anthropic` |

说明：

- 如果你只是把本插件作为 MCP 服务，供 Claude Desktop、Cursor 等外部 AI 客户端连接，则 **不需要** 安装 `openai` 或 `anthropic`
- `openai` / `anthropic` 仅在使用 Odoo 内置 **AI Chat** 功能时才需要

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

### Step 1: 初始化闭包表

点击 **Initialize Closure Tables**，用于支持层级查询（例如公司树、部门树）。

### Step 2: 创建 API Key

点击 **Finish**，继续进入 API Key 页面。

无需部署外部服务 — 查询引擎运行在 Odoo 进程内。

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

## 升级模块

更新 `foggy_mcp` 文件后（例如下载了新版本），**必须执行 Odoo 模块升级命令** — 仅重启容器是不够的。Odoo 会将视图、字段定义和安全规则缓存在数据库中，不显式升级这些变更不会生效。

```bash
# 1. 执行模块升级（根据实际情况替换数据库名和 PG 容器名）
docker exec foggy-odoo bash -c \
  "odoo -d <数据库名> -u foggy_mcp --stop-after-init \
   --db_host=<PG容器名> --db_port=5432 --db_user=odoo --db_password=odoo"

# 2. 重启 Odoo 以加载升级后的注册表
docker restart foggy-odoo
```

将 `<数据库名>` 替换为你的 Odoo 数据库名（如 `odoo_demo`），`<PG容器名>` 替换为 PostgreSQL 容器名（如 `foggy-odoo-postgres`）。

> **提示：** 如果 Odoo 未运行在 Docker 中，使用等效命令：
> ```bash
> odoo -d <数据库名> -u foggy_mcp --stop-after-init
> ```

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
