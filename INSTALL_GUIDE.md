# Foggy MCP Gateway - Installation Guide

[English](./INSTALL_GUIDE.md) | [简体中文](./INSTALL_GUIDE.zh-CN.md)

> A governed data bridge between Odoo and AI clients, allowing AI to query Odoo business data safely.

## Prerequisites

- Odoo 17 is installed and running
- The Odoo database is PostgreSQL

Current release status:

- PostgreSQL is the only validated database for this addon today
- MySQL is not officially supported by this Odoo bridge release yet
- If MySQL support is introduced later, it should be treated as a separate future release after dedicated validation

## Dependency Notes

| Scenario | Extra Python packages needed in the Odoo environment |
|---|---|
| MCP service only (no built-in AI Chat) | None |
| Built-in AI Chat with OpenAI-compatible providers | `openai` |
| Built-in AI Chat with Anthropic / Claude | `anthropic` |

Notes:

- If you only use this addon as an MCP service for Claude Desktop, Cursor, or other external AI clients, you do **not** need `openai` or `anthropic`
- `openai` / `anthropic` are only required when using the built-in **AI Chat** feature inside Odoo

---

## Installation Steps

### 1. Download the Addon

Download the addon package from this repository's Releases page, or clone the repository and use the `foggy_mcp/` addon directory directly.

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

### Step 1: Initialize Closure Tables

Click **Initialize Closure Tables** to enable hierarchy-aware queries such as company trees and department trees.

### Step 2: Create an API Key

Click **Finish** and continue to the API key page.

No external service is required — the query engine runs inside the Odoo process.

---

## Optional: Enable Built-in AI Chat

Only install LLM SDK packages if you want to use **Foggy AI Chat** directly inside Odoo:

**Standard (non-Docker) environment:**

```bash
# OpenAI / DeepSeek / Ollama / other OpenAI-compatible endpoints
pip install openai

# Anthropic / Claude
pip install anthropic
```

**Docker environment:**

The included `Dockerfile` already installs `openai` and `anthropic` when you build with `docker compose`. If you used `docker compose up -d` during installation, the packages are already available.

To rebuild after changes (e.g., updating SDK versions):

```bash
docker compose build odoo
docker compose up -d
```

**Alternative: modify `docker-compose.yml` directly** (no custom image build needed):

```yaml
odoo:
  image: odoo:17.0                    # use the official image as-is
  # ...
  command: >
    bash -c "pip install openai anthropic &&
    exec odoo --database=odoo_demo
    --addons-path=/mnt/extra-addons
    --db_host=postgres --db_port=5432
    --db_user=odoo --db_password=odoo"
```

This installs the packages on every container start — no need to rebuild an image.

**Quick one-time install** (does **not** persist after container restart):

```bash
docker exec foggy-odoo pip install openai anthropic
docker restart foggy-odoo
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

## Upgrading the Module

When you update the `foggy_mcp` files (for example, after downloading a new release), **you must run the Odoo module upgrade command** — a simple container restart is not enough. Odoo caches views, field definitions, and security rules in the database; without an explicit upgrade these changes will not take effect.

```bash
# 1. Run module upgrade (adjust database name and DB host as needed)
docker exec foggy-odoo bash -c \
  "odoo -d <DATABASE> -u foggy_mcp --stop-after-init \
   --db_host=<POSTGRES_CONTAINER> --db_port=5432 --db_user=odoo --db_password=odoo"

# 2. Restart Odoo to pick up the upgraded registry
docker restart foggy-odoo
```

Replace `<DATABASE>` with your Odoo database name (e.g. `odoo_demo`) and `<POSTGRES_CONTAINER>` with your PostgreSQL container name (e.g. `foggy-odoo-postgres`).

> **Tip:** If you are running Odoo outside Docker, use the equivalent CLI:
> ```bash
> odoo -d <DATABASE> -u foggy_mcp --stop-after-init
> ```

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
