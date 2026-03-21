# -*- coding: utf-8 -*-
"""
Gateway Backend — 网关模式实现

包装 FoggyClient，通过 HTTP 转发请求到外部 Foggy MCP Server（Java 或 Python）。
"""
import logging

from .engine_backend import EngineBackend

_logger = logging.getLogger(__name__)


class GatewayBackend(EngineBackend):
    """HTTP 网关模式：将请求转发到外部 Foggy MCP Server。"""

    def __init__(self, foggy_client):
        """
        Args:
            foggy_client: FoggyClient 实例
        """
        self._client = foggy_client

    @property
    def url(self):
        """外部服务器 URL（用于健康检查展示）。"""
        return self._client._url

    def call_tools_list(self):
        return self._client.call_tools_list()

    def call_tools_call(self, tool_name, arguments, trace_id=None):
        return self._client.call_tools_call(tool_name, arguments, trace_id)

    def ping(self):
        return self._client.ping()

    def get_mode(self):
        return 'gateway'
