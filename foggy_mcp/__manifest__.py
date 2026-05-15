# -*- coding: utf-8 -*-
{
    'name': 'Foggy MCP Gateway',
    'version': '17.0.1.5.0',
    'category': 'Productivity',
    'summary': 'MCP gateway for secure Odoo data queries with Foggy Framework',
    'description': """
Foggy MCP Gateway
==================

Provides an MCP (Model Context Protocol) endpoint for external AI clients
to query Odoo business data through Foggy Framework.

Architecture::

    AI Client --MCP--> Odoo (this addon) --Foggy Engine--> PostgreSQL

Features:
    - MCP JSON-RPC 2.0 endpoint at /foggy-mcp/rpc
    - API Key authentication (Bearer token)
    - Per-user tool filtering based on ir.model.access
    - Automatic permission injection (ir.rule -> DSL slice conditions)
    - Closure table hierarchy queries (child_of/parent_of -> selfAndDescendantsOf/selfAndAncestorsOf)
    - Embedded Foggy Python engine or optional self-hosted external Foggy service
    - Multi-company support
    - 12 pre-built Odoo business models (Sale, Purchase, Invoice, Payment, Journal Items, Product, Stock, HR, Partner, Company, CRM)

Security:
    - Users only see data they have access to (enforced by Odoo ir.rule)
    - Forced filters are injected server-side and cannot be bypassed by the AI client
    - API keys are scoped to individual users
    - Fail-closed: permission errors deny access rather than granting it

Data Privacy Notice:
    In embedded mode, query execution runs inside the Odoo process.
    In gateway mode, this addon forwards query requests to a Foggy service
    that you deploy and control.
    No data is sent to OpenAI, Anthropic, or any other third-party cloud service by this addon.
    HTTPS is strongly recommended for production environments.
    """,
    'author': 'Foggy Framework',
    'website': 'https://github.com/nicholasgasior/foggy-data-mcp-bridge',
    'support': 'support@foggysource.com',
    'license': 'Other OSI approved licence',
    'depends': ['base', 'sale', 'purchase', 'account', 'stock', 'hr', 'crm'],
    'external_dependencies': {
        'python': ['asyncpg', 'pydantic', 'yaml'],
    },
    'data': [
        'security/foggy_security.xml',
        'security/ir.model.access.csv',
        'data/foggy_menus.xml',
        'views/connection_test_result_views.xml',
        'wizard/foggy_setup_wizard_views.xml',
        'views/foggy_field_mapping_status_views.xml',
        'views/foggy_config_views.xml',
        'views/foggy_api_key_views.xml',
        'data/foggy_data.xml',
        'data/foggy_cron.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'foggy_mcp/static/src/components/**/*.js',
            'foggy_mcp/static/src/components/**/*.xml',
            'foggy_mcp/static/src/components/**/*.scss',
        ],
    },
    'images': [
        'static/description/banner.png',
        'static/description/screenshot_settings.png',
        'static/description/screenshot_setup_wizard.png',
        'static/description/screenshot_api_keys.png',
    ],
    'price': 0,
    'currency': 'EUR',
    'installable': True,
    'application': True,
    'auto_install': False,
    'post_init_hook': 'post_init_hook',
}
