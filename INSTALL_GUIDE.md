# Foggy MCP Gateway - Installation Guide

[English](./INSTALL_GUIDE.md) | [简体中文](./INSTALL_GUIDE.zh-CN.md)

> A governed data bridge between Odoo and AI clients, allowing AI to query Odoo business data safely.

## Prerequisites

- Odoo 17 is installed and running
- The Odoo database is PostgreSQL

Current release status:

- PostgreSQL is the only validated database for this addon today
- MySQL is not officially supported by this Odoo bridge release yet
- If you want to expose MySQL support later, it is safer to treat that as a `v1.1` target after dedicated validation

## Dependency Notes

Requirements depend on how you use the addon:

| Scenario | Extra Python packages needed in the Odoo environment |
|---|---|
| Standalone MCP service only, without built-in AI Chat | None |
| Built-in AI Chat with OpenAI-compatible providers | `openai` |
| Built-in AI Chat with Anthropic / Claude | `anthropic` |
| Embedded engine mode | `foggy-python` |

Notes:

- If you only use this addon as a standalone MCP service for Claude Desktop, Cursor, or other external AI clients, you do **not** need `openai` or `anthropic`
- `openai` / `anthropic` are only required when using the built-in **AI Chat** feature inside Odoo
- If you use gateway mode with an external Foggy Java or Python service, the Odoo side still does not need those LLM SDKs

---

## Installation Steps

### 1. Download the Addon

```bash
curl -LO https://github.com/foggy-projects/foggy-data-mcp-bridge/releases/download/main/foggy-odoo-addon.zip
unzip foggy-odoo-addon.zip   # extracts to foggy_mcp/
```

### 2. Install into Odoo

**Option A: Docker deployment**

```yaml
# add a volume mount in docker-compose.yml
volumes:
  - ./foggy_mcp:/mnt/extra-addons/foggy_mcp:ro
```

**Option B: Local installation**

```bash
cp -r foggy_mcp /path/to/odoo/addons/
```

### 3. Enable the Module

1. Odoo -> **Settings -> Technical -> Apps**
2. Click **Update Apps List**
3. Search for `foggy_mcp` -> click **Install**

> If you only use the standalone MCP service capability, you still do not need `openai` / `anthropic` at this point.

---

## Setup Wizard

After installation, go to **Settings -> Foggy MCP -> Setup Wizard** and follow the guided flow.

### Step 1: Choose Engine Mode

| Mode | Description |
|---|---|
| **Embedded** | The query engine runs inside the Odoo process. No external Foggy service is required |
| **Gateway** | Requests are forwarded to an external Foggy service, which can be Java or Python |

### Step 2: Server Setup

If you choose **Gateway** mode, the wizard detects Odoo database settings and generates the required deployment configuration:

- **Docker mode**: copy the generated `docker-compose.yml`, then run `docker compose up -d`
- **Manual mode**: copy the generated `java -jar` command and run it

If you choose **Embedded** mode, no external service is required; you only need `foggy-python` installed in the Odoo Python environment.

> Model files are already bundled in the addon or the Foggy engine. No extra model download is required.

### Step 3: Initialize Closure Tables

Click **Initialize Closure Tables** to enable hierarchy-aware queries such as company trees and department trees.

### Step 4: Test Connection

Click **Test Connection** to verify that the Foggy MCP Server is reachable.

### Step 5: Create an API Key

Click **Finish** and continue to the API key page.

---

## Optional: Enable Built-in AI Chat

Only install LLM SDK packages if you want to use **Foggy AI Chat** directly inside Odoo:

```bash
# OpenAI / DeepSeek / Ollama / other OpenAI-compatible endpoints
pip install openai

# Anthropic / Claude
pip install anthropic
```

If you do not use AI Chat, you can skip this section entirely.

---

## Create an API Key

1. Go to **Foggy MCP -> API Keys**
2. Click **Create** -> choose a user -> **Generate Key**
3. Save the API key (`fmcp_xxxxxxxxxxxx`, shown once)

---

## Verify the MCP Endpoint

```bash
# replace YOUR_API_KEY
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

## Connect AI Clients

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

Example questions:

- "Show the latest 10 sales orders"
- "Summarize sales revenue by customer"
- "How many purchase orders were created this month?"

---

## Architecture Overview

```text
AI Client
    │ MCP Protocol
    ▼
Odoo (foggy_mcp addon)
    │ Permission filtering (ir.rule -> DSL slice)
    ▼
Foggy MCP Server
    │ SQL
    ▼
PostgreSQL (Odoo database)
```

Security model:

- Users can only query data they are allowed to access
- Permission filters are injected on the server side and cannot be bypassed from the client
- Fail-closed behavior: permission errors deny access

---

## Troubleshooting

### Foggy MCP Server connection failed

1. Verify the service is running: `curl http://localhost:7108/actuator/health`
2. Check whether the database connection is correct
3. Check firewall and network rules

### Closure table initialization failed

Make sure the Odoo database user has permission to create tables.

### Query returns empty data

1. Confirm the user has access to the corresponding model
2. Confirm business data exists in the database
