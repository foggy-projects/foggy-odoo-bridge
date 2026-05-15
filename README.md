# Foggy Odoo Bridge Community Edition

[English](./README.md) | [简体中文](./README.zh-CN.md)

Governed MCP access to Odoo data with Odoo permission preservation.

Foggy Odoo Bridge is an Odoo addon that lets Claude, Cursor, and other MCP clients query Odoo business data without bypassing Odoo permissions.

This repository is the Community Edition baseline. It is designed for personal users, developers, and lightweight technical evaluation. It focuses on the core governed MCP workflow, the Odoo permission bridge, and a practical starter feature set for Foggy engine adoption.

Foggy Odoo Bridge Pro is the commercial edition for production Odoo teams that need built-in governed AI Chat, richer model packs, export, audit, commercial installation experience, and support.

Public technical notes are kept under [`docs/`](./docs/README.md).

```text
AI client -> MCP -> Odoo bridge -> Foggy semantic layer -> SQL -> PostgreSQL
```

Instead of letting an LLM invent raw SQL against your ERP database, the addon keeps authentication, model visibility, and row-level rules inside Odoo before the query reaches the Foggy engine.

## Why It Matters

Most "AI + ERP" demos can answer a question, but they cannot safely preserve business permissions once the model starts generating SQL.

Foggy Odoo Bridge solves that by placing an Odoo-aware permission and model layer in front of query execution:

- Odoo authentication stays in Odoo
- `ir.model.access` gates available query models
- `ir.rule` domains are converted into DSL slice conditions
- multi-company boundaries stay enforced server-side
- the downstream engine receives governed semantic queries

## What You Get First

- Natural language analytics for Odoo through MCP
- Odoo-aware permission injection before query execution
- API key access for Claude Desktop and Cursor
- Built-in TM/QM models for common Odoo business objects
- Fail-closed behavior when permission evaluation fails
- A practical MCP and semantic-query evaluation path for Odoo

## Dependency Notes

The Community Edition does not ship built-in AI Chat. You do not need `openai` or `anthropic` in the Odoo environment. External MCP clients manage their own model/provider dependencies outside Odoo.

Embedded semantic query execution requires `asyncpg`, `pydantic`, and `PyYAML` in the Odoo Python environment. The included Dockerfile installs these runtime dependencies without adding AI provider SDKs.

For a manual Odoo installation, install the runtime dependencies before installing or upgrading the addon:

```bash
pip install -r requirements.txt
```

## Community vs Pro

| Need | Community Edition | Foggy Odoo Bridge Pro |
|---|---|---|
| Personal Odoo + MCP evaluation | Yes | Yes |
| External MCP clients such as Claude Desktop / Cursor | Yes | Yes |
| Built-in AI Chat inside Odoo | No | Yes |
| Export, audit, and commercial governance workflows | No | Yes |
| Richer commercial Odoo model pack | Limited | Yes |
| Production support and Odoo Apps commercial experience | No | Yes |

Community is not a limited trial of Pro. It is the open and lightweight entry point for MCP, TM/QM, and semantic-query adoption. Use Pro when the requirement is a commercial Odoo user experience with governed in-app AI workflows.

## Database Support

Current release scope for this Odoo bridge:

- PostgreSQL is the only validated database for the addon today
- MySQL is not an officially supported database target in the current release
- If MySQL support is exposed later, it should be treated as a `v1.1` target after dedicated validation

This matches the current wizard flow, SQL assets, test coverage, and verified deployment environment, all of which are centered on Odoo with PostgreSQL.

## Quick Start

1. **Install the Odoo addon**: Copy `foggy_mcp/` to your Odoo addons path and install
2. **Open Setup Wizard**: Settings → Foggy MCP → 🧙 Setup Wizard
3. **Follow the steps**: Initialize closure tables → Create API Key → Done

No external service is required — the query engine runs inside the Odoo process.

### Generate an API Key

1. Go to Settings → Foggy MCP → API Keys
2. Click "Create"
3. Copy the generated `fmcp_xxx` key

### Connect Claude Desktop

For clients that support Streamable HTTP / remote MCP servers, add this endpoint configuration:

```json
{
  "mcpServers": {
    "foggy-odoo": {
      "url": "https://your-odoo.com/foggy-mcp/rpc",
      "headers": {
        "Authorization": "Bearer fmcp_your_key_here"
      }
    }
  }
}
```

Now you can ask questions like:

- "List the available Odoo query models."
- "Show the latest 10 sales orders with customer names and total amounts."
- "Summarize sales order revenue by customer."
- "Which invoices are still unpaid?"
- "Show recent inventory transfers by status."

## Best First Use Cases

- Sales analysis
- Purchase analysis
- Invoice and billing lookups
- Inventory transfer reporting
- Employee directory queries
- Partner and customer exploration

## Why This Is Different

Most integrations stop at "LLM can reach Odoo data." This project is about governed access, not just connectivity.

- It preserves Odoo authorization semantics before query execution.
- It maps ERP models into business-friendly semantic query models.
- It keeps the AI client away from raw SQL and direct schema prompting.
- It is designed for real internal deployments, not just demos.

## Architecture

```text
AI Client -> MCP -> Odoo (foggy_mcp addon) -> Foggy semantic engine -> PostgreSQL
```

- **Odoo MCP Gateway** (`foggy_mcp/`): Handles MCP protocol, authentication, API keys, permission resolution, and payload slice injection
- **Embedded Foggy engine**: Semantic query engine with built-in Odoo TM/QM models, runs inside the Odoo process

## Supported Odoo Models

| Odoo Model | QM Name | Description |
|---|---|---|
| `sale.order` | OdooSaleOrderQueryModel | Sales analysis |
| `sale.order.line` | OdooSaleOrderLineQueryModel | Sales line details |
| `purchase.order` | OdooPurchaseOrderQueryModel | Purchase analysis |
| `account.move` | OdooAccountMoveQueryModel | Invoice & billing |
| `stock.picking` | OdooStockPickingQueryModel | Inventory transfers |
| `hr.employee` | OdooHrEmployeeQueryModel | Employee directory |
| `res.partner` | OdooResPartnerQueryModel | Partner directory |

## Key Features

- **Natural language queries**: Ask business questions and get structured results
- **Row-level security**: Odoo `ir.rule` becomes DSL slice conditions in `payload.slice`
- **Per-user tool filtering**: `ir.model.access` controls query model visibility
- **Multi-company support**: Company isolation enforced server-side
- **API key auth**: Create keys in Odoo for Claude Desktop / Cursor
- **Fail-closed security**: Permission failures deny access instead of allowing it

## Security Model

### Authentication
- **API Key**: `Authorization: Bearer fmcp_xxx` header
- **Session**: Odoo cookie-based session (for web clients)

### Authorization Flow (per tools/call)

```
1. User authenticates (API key → uid)
2. For `dataset__query_model` calls:
   a. Read model name from arguments
   b. Pre-check: ir.model.access read permission (model-level gate)
   c. Map QM name → Odoo model (e.g., OdooSaleOrderQueryModel → sale.order)
   d. Read ir.rule for that model + user (global + group rules)
   e. Evaluate domain_force (resolve user.id, company_ids, etc.)
   f. Parse domain (Polish notation AST → AND/OR/NOT tree)
   g. Flatten tree → DSL slice conditions (with $or/$and nesting)
   h. Inject into arguments.payload.slice
3. Forward to Foggy MCP Server (DSL engine processes slices natively)
```

### Permission Bridge: Domain Parsing

Odoo `ir.rule` domains use Polish (prefix) notation. The permission bridge fully supports:

| Domain Pattern | DSL Output | Example |
|---|---|---|
| `('field', '=', value)` | `{"field": "x", "op": "=", "value": v}` | Multi-company, own records |
| `['|', A, B]` | `{"$or": [A, B]}` | Own or unassigned records |
| `['!', A]` | Negated condition | Exclude cancelled |
| `['&', A, '|', B, C]` | `[A, {"$or": [B, C]}]` | Company + (own OR unassigned) |
| `['!', '|', A, B]` | De Morgan: `[NOT(A), NOT(B)]` | Neither state |
| `['!', '&', A, B]` | De Morgan: `{"$or": [NOT(A), NOT(B)]}` | Either negated |
| AND inside OR | `{"$or": [{"$and": [A, B]}, C]}` | Complex group rules |

**Odoo rule semantics preserved:**
- Global rules (`groups=False`): AND'd together
- Group rules (`groups=specific`): OR'd across rules, then AND'd with globals
- Result: `global1 AND global2 AND (group_rule1 OR group_rule2)`

### DSL Slice Format (injected into payload.slice)

```json
[
  {"field": "company_id", "op": "in", "value": [1, 3]},
  {"$or": [
    {"field": "user_id", "op": "=", "value": 42},
    {"field": "user_id", "op": "is null"}
  ]}
]
```

### Supported Filter Operators

| DSL Operator | SQL | From Odoo |
|---|---|---|
| `=` | `= ?` | `=` |
| `!=` | `!= ?` | `!=` |
| `>`, `>=`, `<`, `<=` | `>`, `>=`, `<`, `<=` | same |
| `in` | `IN (?, ...)` | `in` |
| `not in` | `NOT IN (?, ...)` | `not in` |
| `is null` | `IS NULL` | `= False` |
| `is not null` | `IS NOT NULL` | `!= False` |
| `like` | `LIKE ?` | `like`, `ilike` |

## Testing

```bash
# Run permission bridge unit tests (no Odoo runtime needed)
cd addons/foggy-odoo-bridge
python -m pytest tests/test_permission_bridge.py -v
```

45 tests covering: AST parsing, leaf conversion, operator negation, De Morgan's laws, `$or`/`$and` nesting, payload injection simulation, and real-world Odoo domain patterns.

Before preparing an Odoo Apps Community listing package, also run:

```bash
bash scripts/check-no-pro-content.sh
bash scripts/check-model-drift.sh
bash scripts/sync-community-models.sh --dry-run
bash scripts/check-odoo-apps-readiness.sh
```

## Extending with Custom Models

Custom TM/QM models can be added in two ways:

### Option 1: Add to foggy-odoo-bridge-java module

Add your TM/QM files to the `foggy-odoo-bridge-java` module's resources and rebuild the Docker image.

### Option 2: External Bundle (Advanced)

Mount an external bundle directory and configure Foggy to load it:

```bash
java -jar foggy-mcp-launcher.jar \
  --spring.profiles.active=lite \
  --foggy.bundle.external.enabled=true \
  --foggy.bundle.external.bundles[0].name=custom-models \
  --foggy.bundle.external.bundles[0].path=/path/to/custom-models \
  --foggy.bundle.external.bundles[0].namespace=custom
```

### Create TM file (`model/MyCustomModel.tm`)

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

### 2. Create QM file (`query/MyCustomQueryModel.qm`)

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

### 3. Add model mapping in `tool_registry.py`

```python
MODEL_MAPPING = {
    # ...existing...
    'my.custom.model': 'MyCustomQueryModel',
}
```

### 4. Restart Foggy MCP Server to reload models

## Upgrading

After updating the `foggy_mcp` files, run the module upgrade — a container restart alone is not enough:

```bash
docker exec foggy-odoo bash -c \
  "odoo -d <DATABASE> -u foggy_mcp --stop-after-init \
   --db_host=<POSTGRES_CONTAINER> --db_port=5432 --db_user=odoo --db_password=odoo"
docker restart foggy-odoo
```

See the [Installation Guide](INSTALL_GUIDE.md#upgrading-the-module) for details.

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `foggy_mcp.server_url` | `http://foggy-mcp:8080` | Foggy MCP Server URL |
| `foggy_mcp.endpoint_path` | `/mcp/analyst/rpc` | MCP endpoint path |
| `foggy_mcp.request_timeout` | `30` | HTTP timeout (seconds) |
| `foggy_mcp.namespace` | `odoo17` | Model namespace |
| `foggy_mcp.cache_ttl` | `300` | Tool cache TTL (seconds) |

## License

Apache License 2.0
