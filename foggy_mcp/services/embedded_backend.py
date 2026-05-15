# -*- coding: utf-8 -*-
"""
Embedded Backend — 内嵌模式实现

直接在 Odoo 进程内运行 foggy-dataset-py 查询引擎，无需外部服务器。

调用层次（遵循 Python 团队分层设计）：
    EmbeddedBackend → LocalDatasetAccessor（dict 入参）
                        → SemanticQueryService（强类型，内部）

不直接调用 SemanticQueryService 的查询方法，
统一通过 LocalDatasetAccessor 桥接（接受标准 JSON dict，与 Java 签名一致）。
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
            from foggy.dataset.db.executor import create_executor_from_url
            from foggy.mcp_spi import LocalDatasetAccessor
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

        # 创建引擎 + Accessor 桥接层
        executor = create_executor_from_url(url)
        self._service = SemanticQueryService(executor=executor)
        self._accessor = LocalDatasetAccessor(self._service)

        # 注册模型
        self._register_models()
        self._initialized = True

        _logger.info("内嵌引擎初始化完成，已注册 %d 个模型",
                      len(self._service.get_all_model_names()))

    def _register_models(self):
        """从 TM/QM 文件加载 Odoo 模型。

        使用 foggy-python 的 load_models_from_directory() 从
        foggy_mcp/setup/foggy-models/ 加载标准 TM/QM 文件。
        这是模型的唯一权威源，与 Java/Python 网关共用同一套定义。
        """
        import os
        from foggy.dataset_model.impl.loader import load_models_from_directory

        # 定位 TM/QM 文件目录：foggy_mcp/setup/foggy-models/
        module_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        model_dir = os.path.join(module_dir, 'setup', 'foggy-models')

        if not os.path.isdir(model_dir):
            _logger.error("TM/QM 模型目录不存在：%s", model_dir)
            return

        try:
            models = load_models_from_directory(model_dir)
            for model in models:
                self._service.register_model(model)
            _logger.info("已从 TM/QM 加载 %d 个模型（目录：%s）", len(models), model_dir)
        except Exception as e:
            _logger.error("TM/QM 模型加载失败：%s", e, exc_info=True)

    def call_tools_list(self):
        """返回工具定义列表。"""
        self._ensure_initialized()
        model_names = self._service.get_all_model_names()
        return self._build_tool_definitions(model_names)

    def call_tools_call(self, tool_name, arguments, trace_id=None):
        """执行工具调用。"""
        self._ensure_initialized()

        _logger.info("EmbeddedBackend.call_tools_call: tool=%s, args=%s",
                     tool_name, json.dumps(arguments, ensure_ascii=False, default=str)[:500])

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

    # ── 工具处理方法（通过 LocalDatasetAccessor 调用）────

    def _handle_query(self, arguments):
        """处理 dataset.query_model 调用。

        通过 LocalDatasetAccessor.query_model(model, payload_dict)，
        payload 使用 Java camelCase 格式，内部自动转换为 SemanticQueryRequest。
        """
        model = arguments.get('model')
        payload = arguments.get('payload', {})

        if not model:
            return self._mcp_result("缺少 model 参数")

        response = self._accessor.query_model(model, payload)
        return self._mcp_result(response)

    def _handle_get_metadata(self, arguments):
        """处理 dataset.get_metadata 调用。

        默认使用 markdown 格式（与 Java LocalDatasetAccessor 一致），
        因为 Python V3 JSON 版的自有维度字段名有 $id 后缀 Bug。
        Markdown 版正确且 token 更少（~40-60%）。

        This keeps the default path aligned with the currently validated
        metadata behavior in the embedded Python engine.
        """
        fmt = arguments.get('format', 'markdown')
        if fmt == 'json':
            result = self._service.get_metadata_v3()
            return self._mcp_result(result)
        else:
            result = self._service.get_metadata_v3_markdown()
            return self._mcp_result(result)

    def _handle_describe_model(self, arguments):
        """处理 dataset.describe_model_internal 调用。

        默认使用 markdown 格式（属性字段名正确，无错误 $id 后缀）。
        Python V3 JSON 版的 get_metadata_v3() 对自有维度错误追加 $id，
        待 Python 团队修复后可切回 json。

        This keeps the default path aligned with the currently validated
        metadata behavior in the embedded Python engine.
        """
        model = arguments.get('model')
        if not model:
            return self._mcp_result("缺少 model 参数")

        fmt = arguments.get('format', 'markdown')
        if fmt == 'json':
            result = self._service.get_metadata_v3([model])
            return self._mcp_result(result)
        else:
            result = self._service.get_metadata_v3_markdown([model])
            return self._mcp_result(result)

    def _handle_list_models(self):
        """处理 dataset.list_models 调用。"""
        model_names = self._service.get_all_model_names()
        return self._mcp_result({'models': model_names})

    # ── 辅助方法 ────────────────────────────────────────

    @staticmethod
    def _mcp_result(data):
        """将数据包装为 MCP 响应格式。支持 dict、str、Pydantic model。"""
        if isinstance(data, str):
            text = data
        elif isinstance(data, dict) or isinstance(data, list):
            text = json.dumps(data, ensure_ascii=False, default=str)
        elif hasattr(data, 'model_dump'):
            # Pydantic BaseModel（SemanticQueryResponse 等）
            text = json.dumps(
                data.model_dump(by_alias=True, exclude_none=True),
                ensure_ascii=False, default=str,
            )
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
        """Build MCP tool definitions from registered query models."""
        tools = []

        # dataset.query_model
        tools.append({
            'name': 'dataset.query_model',
            'description': (
                'Query Odoo data through the Foggy semantic layer. '
                'Supports dimension filters, metric aggregation, sorting, and paging.'
            ),
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'model': {
                        'type': 'string',
                        'description': 'Query model name.',
                        'enum': sorted(model_names),
                    },
                    'payload': {
                        'type': 'object',
                        'description': 'Query payload, such as columns, slice, order, and paging.',
                    },
                },
                'required': ['model'],
            }
        })

        # dataset.get_metadata
        tools.append({
            'name': 'dataset.get_metadata',
            'description': 'Get a metadata overview for all available query models.',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'format': {
                        'type': 'string',
                        'description': 'Output format.',
                        'default': 'json',
                    },
                },
            }
        })

        # dataset.list_models
        tools.append({
            'name': 'dataset.list_models',
            'description': 'List the query models loaded by the semantic engine.',
            'inputSchema': {
                'type': 'object',
                'properties': {},
            }
        })

        # dataset.describe_model_internal
        tools.append({
            'name': 'dataset.describe_model_internal',
            'description': (
                'Get detailed field definitions for one query model, '
                'including dimensions, metrics, and hierarchies.'
            ),
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'model': {
                        'type': 'string',
                        'description': 'Query model name.',
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
