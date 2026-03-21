# -*- coding: utf-8 -*-
"""
Engine Factory — 根据配置创建引擎后端

读取 foggy_mcp.engine_mode 配置参数，返回对应的 EngineBackend 实例。
"""
import logging

from .engine_backend import EngineBackend
from .gateway_backend import GatewayBackend
from .foggy_client import FoggyClient

_logger = logging.getLogger(__name__)


def create_backend(env) -> EngineBackend:
    """根据配置创建引擎后端。

    Args:
        env: Odoo 环境

    Returns:
        EngineBackend: 网关或内嵌引擎实例
    """
    ICP = env['ir.config_parameter'].sudo()
    mode = ICP.get_param('foggy_mcp.engine_mode', 'gateway')

    if mode == 'embedded':
        return _create_embedded_backend(env)
    else:
        return _create_gateway_backend(env)


def _create_gateway_backend(env):
    """创建网关模式后端。"""
    client = FoggyClient.from_config(env)
    return GatewayBackend(client)


def _create_embedded_backend(env):
    """创建内嵌模式后端。"""
    try:
        from .embedded_backend import EmbeddedBackend
    except ImportError as e:
        raise ValueError(
            "内嵌模式需要安装 foggy-python 包。\n"
            "请运行：pip install foggy-python\n"
            "或切换到网关模式。\n"
            f"导入错误：{e}"
        )

    from odoo.tools import config as odoo_config

    db_config = {
        'host': odoo_config.get('db_host') or 'localhost',
        'port': int(odoo_config.get('db_port') or 5432),
        'database': env.cr.dbname,
        'user': odoo_config.get('db_user') or 'odoo',
        'password': odoo_config.get('db_password') or '',
    }

    _logger.info("创建内嵌引擎后端：%s@%s:%s/%s",
                 db_config['user'], db_config['host'],
                 db_config['port'], db_config['database'])

    return EmbeddedBackend(db_config)
