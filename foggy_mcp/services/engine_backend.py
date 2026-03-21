# -*- coding: utf-8 -*-
"""
Engine Backend — 引擎抽象接口

定义统一接口，让 MCP Controller 不关心底层是 HTTP 网关还是内嵌引擎。
"""
from abc import ABC, abstractmethod


class EngineBackend(ABC):
    """查询引擎统一接口。"""

    @abstractmethod
    def call_tools_list(self):
        """返回 MCP 工具定义列表。

        Returns:
            list: 工具定义列表（与 MCP tools/list 格式一致）
        """

    @abstractmethod
    def call_tools_call(self, tool_name, arguments, trace_id=None):
        """执行工具调用。

        Args:
            tool_name: 工具名称（如 'dataset.query_model'）
            arguments: 工具参数（权限 slice 已由 Controller 注入）
            trace_id: 请求追踪 ID

        Returns:
            dict: 完整的 MCP JSON-RPC 响应
        """

    @abstractmethod
    def ping(self):
        """健康检查。

        Returns:
            bool: 引擎是否可用
        """

    @abstractmethod
    def get_mode(self):
        """返回引擎模式标识。

        Returns:
            str: 'gateway' 或 'embedded'
        """
