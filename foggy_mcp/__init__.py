# -*- coding: utf-8 -*-
import logging
import os
import sys

# ── Vendored foggy-python 引擎 ──────────────────────────
# 将 lib/ 目录加入 Python path，使 `import foggy` 可用。
# 如果用户已通过 pip 安装了 foggy-python，pip 版本优先。
_lib_dir = os.path.join(os.path.dirname(__file__), 'lib')
if _lib_dir not in sys.path:
    try:
        import foggy  # noqa: F401 — 检查是否已通过 pip 安装
    except ImportError:
        sys.path.insert(0, _lib_dir)

from . import models  # noqa: F401
from . import controllers  # noqa: F401
from . import wizard  # noqa: F401

_logger = logging.getLogger(__name__)


def post_init_hook(env):
    """
    Post-init hook to ensure safe module installation.

    Odoo 17 passes a single `env` argument (not cr, registry).
    Handles:
    1. Ensures config parameters have default values
    2. Logs successful installation for troubleshooting
    """
    _logger.info("Foggy MCP Gateway: Running post-init hook...")

    cr = env.cr
    from .services.odoo_namespace import resolve_foggy_namespace, sync_configured_foggy_namespace

    foggy_namespace = resolve_foggy_namespace(env)

    # Set default config parameters if not exists
    default_params = {
        'foggy_mcp.server_url': '',
        'foggy_mcp.endpoint_path': '/mcp/analyst/rpc',
        'foggy_mcp.request_timeout': '30',
        'foggy_mcp.namespace': foggy_namespace,
        'foggy_mcp.cache_ttl': '300',
        'foggy_mcp.auth_token': '',
        'foggy_mcp.engine_mode': 'embedded',
    }

    try:
        for key, default_value in default_params.items():
            cr.execute("""
                SELECT 1 FROM ir_config_parameter WHERE key = %s
            """, (key,))
            if not cr.fetchone():
                cr.execute("""
                    INSERT INTO ir_config_parameter (key, value)
                    VALUES (%s, %s)
                """, (key, default_value))
                _logger.debug("Created config parameter: %s", key)

        sync_configured_foggy_namespace(env)

        # ── Initialize closure tables and date dimension ──
        _init_auxiliary_tables(cr)

        _logger.info("Foggy MCP Gateway: Post-init hook completed successfully")
    except Exception as e:
        _logger.warning("Foggy MCP Gateway: Post-init hook encountered an issue: %s", e)


def _init_auxiliary_tables(cr):
    """Create and populate closure tables + date dimension table.

    Reads SQL files from setup/sql/ and executes them.
    Safe to run multiple times (CREATE IF NOT EXISTS + ON CONFLICT DO NOTHING).
    """
    import os

    sql_dir = os.path.join(os.path.dirname(__file__), 'setup', 'sql')

    # 1. Closure tables — create tables + refresh functions
    closure_sql = os.path.join(sql_dir, 'refresh_closure_tables.sql')
    if os.path.exists(closure_sql):
        try:
            with open(closure_sql, 'r', encoding='utf-8') as f:
                cr.execute(f.read())
            cr.execute("SELECT refresh_all_closures()")
            _logger.info("Closure tables initialized and refreshed")
        except Exception as e:
            _logger.warning("Failed to initialize closure tables: %s", e)
    else:
        _logger.warning("Closure SQL not found: %s", closure_sql)

    # 2. Date dimension table — create table + populate 2020-2035
    dim_date_sql = os.path.join(sql_dir, 'create_dim_date.sql')
    if os.path.exists(dim_date_sql):
        try:
            with open(dim_date_sql, 'r', encoding='utf-8') as f:
                cr.execute(f.read())
            cr.execute("SELECT create_or_refresh_dim_date(2020, 2035)")
            row_count = cr.fetchone()[0]
            _logger.info("Date dimension table initialized: %d rows (2020-2035)", row_count)
        except Exception as e:
            _logger.warning("Failed to initialize date dimension: %s", e)
    else:
        _logger.warning("Date dimension SQL not found: %s", dim_date_sql)
