# -*- coding: utf-8 -*-
"""
Embedded Backend — 内嵌模式实现

直接在 Odoo 进程内运行 foggy-dataset-py 查询引擎，无需外部服务器。

依赖：foggy-python（pip install foggy-python）
数据库：使用 asyncpg 独立连接池连接 Odoo 的 PostgreSQL（只读）
异步桥接：SemanticQueryService 内置 async→sync 桥接，无需额外处理
"""
import json
import logging

from .engine_backend import EngineBackend

_logger = logging.getLogger(__name__)


class EmbeddedBackend(EngineBackend):
    """内嵌模式：foggy-dataset-py 引擎在 Odoo 进程内运行。"""

    def __init__(self, db_config):
        """
        Args:
            db_config: dict with {host, port, database, user, password}
        """
        self._db_config = db_config
        self._service = None
        self._accessor = None
        self._initialized = False

    def _ensure_initialized(self):
        """懒初始化：首次请求时创建引擎。"""
        if self._initialized:
            return

        try:
            from foggy.dataset_model.semantic.service import SemanticQueryService
            from foggy.mcp.spi import LocalDatasetAccessor
            from foggy.dataset.db.executor import create_executor_from_url
        except ImportError as e:
            raise ImportError(
                f"foggy-python 未安装或版本不兼容：{e}\n"
                "请运行：pip install foggy-python"
            )

        # 构造 PostgreSQL 连接 URL
        c = self._db_config
        url = f"postgresql://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"

        _logger.info("内嵌引擎初始化：连接 %s@%s:%s/%s",
                      c['user'], c['host'], c['port'], c['database'])

        # 创建引擎
        executor = create_executor_from_url(url)
        self._service = SemanticQueryService(executor=executor)
        self._accessor = LocalDatasetAccessor(self._service)

        # 注册模型
        self._register_models()
        self._initialized = True

        _logger.info("内嵌引擎初始化完成，已注册 %d 个模型",
                      len(self._service.get_all_model_names()))

    def _register_models(self):
        """注册 Odoo TM/QM 模型。

        优先注册 9 个 Odoo 专用模型（从 Java TM/QM 移植）。
        如果导入失败（foggy-python 版本不兼容），回退到 demo 模型。
        """
        try:
            from ..embedded_models import register_all_odoo_models
            count = register_all_odoo_models(self._service)
            if count > 0:
                _logger.info("已注册 %d 个 Odoo 模型", count)
                return
        except Exception as e:
            _logger.warning("Odoo 模型注册失败，回退到 demo 模型：%s", e)

        # 回退：注册 demo 模型
        try:
            from foggy.demo.models.ecommerce_models import register_all_models
            register_all_models(self._service)
            _logger.info("已注册 demo ecommerce 模型（回退）")
        except Exception as e:
            _logger.error("模型注册失败：%s", e)

    def call_tools_list(self):
        """返回工具定义列表。"""
        self._ensure_initialized()
        model_names = self._service.get_all_model_names()
        return self._build_tool_definitions(model_names)

    def call_tools_call(self, tool_name, arguments, trace_id=None):
        """执行工具调用。"""
        self._ensure_initialized()

        _logger.info("EmbeddedBackend.call_tools_call: tool=%s", tool_name)

        try:
            if tool_name == 'dataset.query_model':
                return self._handle_query(arguments)
            elif tool_name == 'dataset.get_metadata':
                return self._handle_get_metadata(arguments)
            elif tool_name == 'dataset.describe_model_internal':
                return self._handle_describe_model(arguments)
            elif tool_name == 'dataset.list_models':
                return self._handle_list_models()
            else:
                return self._mcp_result(f"未知工具：{tool_name}")
        except Exception as e:
            _logger.error("内嵌引擎执行失败：%s", e, exc_info=True)
            return self._mcp_result(f"查询执行失败：{e}")

    def ping(self):
        """健康检查。"""
        try:
            self._ensure_initialized()
            return True
        except Exception as e:
            _logger.warning("内嵌引擎 ping 失败：%s", e)
            return False

    def get_mode(self):
        return 'embedded'

    # ── 工具处理方法 ────────────────────────────────────

    def _handle_query(self, arguments):
        """处理 dataset.query_model 调用。"""
        model = arguments.get('model')
        payload = arguments.get('payload', {})

        if not model:
            return self._mcp_result("缺少 model 参数")

        response = self._accessor.query_model(model, payload)
        return self._mcp_result(response)

    def _handle_get_metadata(self, arguments):
        """处理 dataset.get_metadata 调用。"""
        model_names = self._service.get_all_model_names()
        metadata = {
            'models': {name: {'factTable': name} for name in model_names}
        }
        return self._mcp_result(metadata)

    def _handle_describe_model(self, arguments):
        """处理 dataset.describe_model_internal 调用。"""
        model = arguments.get('model')
        if not model:
            return self._mcp_result("缺少 model 参数")

        response = self._accessor.describe_model(model)
        return self._mcp_result(response)

    def _handle_list_models(self):
        """处理 dataset.list_models 调用。"""
        model_names = self._service.get_all_model_names()
        return self._mcp_result({'models': model_names})

    # ── 辅助方法 ────────────────────────────────────────

    @staticmethod
    def _mcp_result(data):
        """将数据包装为 MCP 响应格式。"""
        if isinstance(data, str):
            text = data
        elif isinstance(data, dict) or isinstance(data, list):
            text = json.dumps(data, ensure_ascii=False, default=str)
        else:
            text = str(data)

        return {
            'result': {
                'content': [
                    {'type': 'text', 'text': text}
                ]
            }
        }

    @staticmethod
    def _build_tool_definitions(model_names):
        """根据已注册模型构建 MCP 工具定义。"""
        tools = []

        # dataset.query_model
        tools.append({
            'name': 'dataset.query_model',
            'description': '使用语义层查询数据模型。支持维度筛选、度量聚合、排序和分页。',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'model': {
                        'type': 'string',
                        'description': '查询模型名称',
                        'enum': sorted(model_names),
                    },
                    'payload': {
                        'type': 'object',
                        'description': '查询参数（columns, slice, order, paging 等）',
                    },
                },
                'required': ['model'],
            }
        })

        # dataset.get_metadata
        tools.append({
            'name': 'dataset.get_metadata',
            'description': '获取所有可用模型的元数据概览。',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'format': {
                        'type': 'string',
                        'description': '输出格式',
                        'default': 'json',
                    },
                },
            }
        })

        # dataset.describe_model_internal
        tools.append({
            'name': 'dataset.describe_model_internal',
            'description': '获取指定模型的详细字段定义（维度、度量、层级等）。',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'model': {
                        'type': 'string',
                        'description': '模型名称',
                        'enum': sorted(model_names),
                    },
                    'format': {
                        'type': 'string',
                        'default': 'json',
                    },
                },
                'required': ['model'],
            }
        })

        return tools
