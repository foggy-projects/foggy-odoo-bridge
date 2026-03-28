# Foggy Odoo Bridge

Let Claude, Cursor, and other MCP clients query Odoo data safely, while preserving Odoo permission rules.

## Why This Exists

Most "AI + ERP" demos fall apart on the same problem: the model can describe the question, but the system cannot safely preserve business permissions once it starts generating raw SQL.

Foggy Odoo Bridge solves that by placing an Odoo-aware permission and model layer in front of the query engine:

```text
AI client -> MCP -> Odoo bridge -> Foggy semantic layer -> SQL -> PostgreSQL
```

Instead of letting the model invent SQL:

- Odoo authentication stays in Odoo
- `ir.model.access` gates available query models
- `ir.rule` domains are converted into DSL slice conditions
- multi-company boundaries stay enforced server-side
- the downstream engine receives governed semantic queries

## What You Get

- Natural language analytics for Odoo through MCP
- Odoo-aware permission injection before query execution
- API key access for Claude Desktop and Cursor
- Built-in TM/QM models for common Odoo business objects
- Fail-closed behavior when permission evaluation fails
- A practical first scenario for enterprise AI data access

## Best First Use Cases

- Sales analysis
- Purchase analysis
- Invoice and billing lookups
- Inventory transfer reporting
- Employee directory queries
- Partner and customer exploration

## Architecture

```text
AI Client -> MCP -> Odoo MCP Gateway -> Foggy MCP Server -> PostgreSQL
            (Python addon)            (semantic layer + DSL engine)
```

- **Odoo MCP Gateway** (`foggy_mcp/`): Handles MCP protocol, authentication, API keys, permission resolution, and payload slice injection
- **Foggy MCP Server**: Semantic query engine with built-in Odoo TM/QM models
  - Docker image: `foggysource/foggy-odoo-mcp:v8.1.8-beta`
  - Dynamic DataSource configuration via API

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

## Quick Start

### Using Setup Wizard (Recommended)

1. **Install the Odoo addon**: Copy `foggy_mcp/` to your Odoo addons path and install
2. **Open Setup Wizard**: Settings → Foggy MCP → 🧙 Setup Wizard
3. **Follow the steps**:
   - Copy the generated Docker command
   - Run it to start Foggy MCP Server (models built-in)
   - Test connection
   - Configure data source (auto-fills Odoo DB info)
   - Initialize closure tables

### Docker Quick Start

```bash
# Start Foggy MCP Server with built-in Odoo models
docker run -d \
  --name foggy-mcp \
  -p 7108:8080 \
  -e SPRING_PROFILES_ACTIVE=lite,odoo \
  -e FOGGY_AUTH_TOKEN=your_token_here \
  --add-host=host.docker.internal:host-gateway \
  --restart unless-stopped \
  foggysource/foggy-odoo-mcp:v8.1.8-beta
```

Then use the Setup Wizard to configure the data source.

### Generate an API Key

1. Go to Settings → Foggy MCP → API Keys
2. Click "Create"
3. Copy the generated `fmcp_xxx` key

### Connect Claude Desktop

Add to `claude_desktop_config.json`:

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

Now you can ask questions like:

- "Who are the top customers by sales this month?"
- "Which purchase orders are delayed?"
- "Show inventory transfers by warehouse this week"
- "Which invoices are still unpaid?"

## Why This Is Different

Most integrations stop at "LLM can reach Odoo data." This project is about governed access, not just connectivity.

- It preserves Odoo authorization semantics before query execution.
- It maps ERP models into business-friendly semantic query models.
- It keeps the AI client away from raw SQL and direct schema prompting.
- It is designed for real internal deployments, not just demos.

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
2. For dataset.query_model calls:
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

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `foggy_mcp.server_url` | `http://foggy-mcp:8080` | Foggy MCP Server URL |
| `foggy_mcp.endpoint_path` | `/mcp/analyst/rpc` | MCP endpoint path |
| `foggy_mcp.request_timeout` | `30` | HTTP timeout (seconds) |
| `foggy_mcp.namespace` | `odoo` | Model namespace |
| `foggy_mcp.cache_ttl` | `300` | Tool cache TTL (seconds) |

## License

Apache License 2.0
