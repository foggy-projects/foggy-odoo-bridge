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

    # Set default config parameters if not exists
    default_params = {
        'foggy_mcp.server_url': '',
        'foggy_mcp.endpoint_path': '/mcp/analyst/rpc',
        'foggy_mcp.request_timeout': '30',
        'foggy_mcp.namespace': 'odoo',
        'foggy_mcp.cache_ttl': '300',
        'foggy_mcp.auth_token': '',
        'foggy_mcp.engine_mode': 'embedded',
        'foggy_mcp.llm_provider': 'openai',
        'foggy_mcp.llm_api_key': '',
        'foggy_mcp.llm_model': 'gpt-4o-mini',
        'foggy_mcp.llm_base_url': '',
        'foggy_mcp.llm_temperature': '0.3',
        'foggy_mcp.llm_custom_prompt': '',
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

        _logger.info("Foggy MCP Gateway: Post-init hook completed successfully")
    except Exception as e:
        _logger.warning("Foggy MCP Gateway: Post-init hook encountered an issue: %s", e)
