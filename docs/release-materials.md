# Foggy Odoo Bridge Release Materials

This document is a working pack for GitHub launch, forum posts, and eventual Odoo Apps publication.

## Product Positioning

Foggy Odoo Bridge lets AI clients query Odoo data through MCP while preserving Odoo permissions.

Core value:

- Odoo authentication and authorization stay in Odoo.
- Query execution goes through Foggy semantic models instead of raw SQL generation.
- The bridge supports both embedded and external engine topologies.

## Engine Modes

Use this wording consistently in docs, screenshots, and videos.

### 1. Embedded Python Engine

Recommended default.

- Runs inside the Odoo process.
- Uses the embedded `foggy-data-mcp-bridge-python` engine.
- No separate Foggy service is required.
- Best for demos, quick starts, and smaller self-hosted deployments.

Suggested one-line description:

`Run Foggy directly inside Odoo with zero extra services.`

### 2. External Python Engine

Optional gateway topology.

- Odoo keeps the bridge and permission logic.
- Queries are forwarded to an external Foggy Python service over HTTP.
- Useful when the engine should be isolated from Odoo or managed separately.

Suggested one-line description:

`Keep Odoo lightweight and route semantic queries to a dedicated Python service.`

### 3. External Java Engine

Optional gateway topology.

- Odoo keeps the bridge and permission logic.
- Queries are forwarded to an external Foggy Java service over HTTP.
- Best fit for teams standardizing on the Java Foggy runtime or shared semantic services.

Suggested one-line description:

`Use the Java Foggy server as a standalone governed analytics engine behind Odoo.`

## Short Capability Summary

Use this in README snippets, forum introductions, and landing-page copy.

- MCP endpoint for Claude Desktop, Cursor, and other MCP clients
- Odoo-aware permission enforcement with `ir.model.access` and `ir.rule`
- Built-in AI chat inside Odoo
- Embedded engine or external Foggy server deployment
- Support for common Odoo business models out of the box
- API-key based AI client access

## Suggested GitHub Announcement

`Foggy Odoo Bridge is an Odoo addon that exposes governed MCP access to Odoo business data. It keeps authentication and permission logic inside Odoo, injects row-level filters from ir.rule, and lets AI clients query approved semantic models instead of generating raw SQL. You can run the engine embedded in Odoo or connect to external Foggy Python / Java services.`

## Demo Video Plan

Target length: 90 to 150 seconds.

### Version A: Fast Product Demo

1. Hook, 0-10s
   Show the question: "Which customers generated the most revenue this quarter?"
   Narration: "This is Odoo data queried through MCP without bypassing Odoo permissions."

2. Problem, 10-25s
   Show architecture card or short animation.
   Narration: "Most AI demos stop at connectivity. Foggy Odoo Bridge focuses on governed access."

3. Engine choice, 25-45s
   Show Settings > Foggy MCP > Engine Mode.
   Narration: "You can run the engine embedded in Odoo or route requests to external Foggy Python or Java services."

4. Setup, 45-70s
   Show Setup Wizard and generated deploy command.
   Narration: "The setup wizard covers deployment, connection testing, data source registration, and closure table initialization."

5. AI chat, 70-105s
   Show Foggy AI Chat asking 2 sample questions.
   Narration: "Built-in chat lets business users ask questions directly inside Odoo."

6. MCP client, 105-130s
   Show API key page and MCP config JSON.
   Narration: "You can also connect Claude Desktop, Cursor, or any MCP-compatible client."

7. Close, 130-150s
   Show result table and architecture overlay.
   Narration: "Foggy Odoo Bridge gives AI access to Odoo data without dropping Odoo's permission model."

### Version B: Technical Demo

1. Explain `ir.rule` injection.
2. Show gateway vs embedded mode.
3. Run one query from AI Chat.
4. Run one query from an external MCP client.
5. End on architecture diagram.

## Screenshot List

Prepare at least 6 screenshots for release materials.

1. Settings page with engine mode selector visible
2. Setup Wizard welcome step
3. Setup Wizard deploy step with generated command
4. Setup Wizard data source step
5. AI Chat empty state with example prompts
6. AI Chat result state with a business answer and table
7. API key form with MCP config block
8. Optional architecture slide or diagram for GitHub/forum header

## Screenshot Notes

- Use English UI for public screenshots.
- Use clean demo data with realistic customer, order, and revenue values.
- Avoid exposing real hostnames, tokens, email addresses, or employee private data.
- Prefer a wide desktop viewport around 1440x1024.

## Image Briefs For Manual Generation

These are not screenshots. They are supplemental marketing visuals.

### Hero Image Prompt

`A clean enterprise product illustration for an Odoo AI data bridge, showing Odoo, MCP, governed semantic query flow, and analytics results, modern flat-isometric style, teal and slate palette, white background, subtle depth, professional SaaS marketing visual`

### Architecture Card Prompt

`Minimal technical architecture diagram on a bright background: AI client -> MCP -> Odoo bridge -> Foggy semantic engine -> PostgreSQL, clean labels, enterprise software aesthetic, sharp typography, teal, graphite, and soft blue accents`

### Permission-Safe AI Prompt

`Enterprise AI governance illustration, an AI assistant querying ERP data through policy checks and semantic models, no sci-fi clichés, modern B2B product visual, clean white canvas, teal and gray palette`

## Playwright Screenshot Workflow

Yes, Playwright CLI is suitable for product screenshots if the local Odoo instance is reachable and we have valid demo credentials.

Recommended flow:

1. Upgrade the addon so English UI strings are loaded.
2. Log into the local Odoo demo instance.
3. Navigate to the target screens.
4. Capture consistent desktop screenshots with a fixed viewport.
5. Repeat for chat, wizard, settings, and API key screens.

Suggested viewport:

- Width: 1440
- Height: 1024

## Public Messaging Guidance

When posting to GitHub or forums, avoid over-emphasizing implementation details first.

Lead with:

- governed AI access to Odoo
- MCP compatibility
- embedded or external deployment
- permission preservation

Then explain:

- Odoo permissions become query filters
- semantic models replace raw SQL prompting
- built-in chat and external MCP clients are both supported
