# -*- coding: utf-8 -*-
"""
Foggy MCP Gateway Settings

Configuration stored in ir.config_parameter (no DB column needed).
Fields with config_parameter attribute are automatically mapped.

IMPORTANT: After adding new fields, always run module upgrade:
    docker exec <odoo_container> odoo -u foggy_mcp -d <database> --stop-after-init

This ensures Odoo properly registers the new fields without requiring
database columns (TransientModel fields are stored in ir.config_parameter).
"""
import logging

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    # ── 引擎模式 ──────────────────────────────────────

    foggy_engine_mode = fields.Selection([
        ('embedded', '内嵌模式（推荐）'),
        ('gateway', '网关模式'),
    ], string='引擎模式',
        config_parameter='foggy_mcp.engine_mode',
        default='embedded',
        help='内嵌模式：Python 引擎在 Odoo 进程内运行，零外部依赖。\n'
             '网关模式：通过 HTTP 转发到外部 Foggy 服务器。',
    )

    foggy_embedded_available = fields.Boolean(
        string='内嵌引擎可用',
        compute='_compute_embedded_available',
    )

    @api.depends('foggy_engine_mode')
    def _compute_embedded_available(self):
        for rec in self:
            try:
                import foggy.mcp_spi  # noqa: F401
                rec.foggy_embedded_available = True
            except ImportError:
                rec.foggy_embedded_available = False

    # ── Foggy MCP 服务器连接 ──────────────────────────

    foggy_mcp_url = fields.Char(
        string='Foggy MCP 服务地址',
        config_parameter='foggy_mcp.server_url',
        help='Foggy MCP 服务器的基础地址（例如：http://foggy-mcp:8080）',
    )
    foggy_mcp_endpoint = fields.Char(
        string='MCP 端点路径',
        config_parameter='foggy_mcp.endpoint_path',
        default='/mcp/analyst/rpc',
        help='Foggy 服务器上的 MCP JSON-RPC 端点路径',
    )
    foggy_mcp_timeout = fields.Integer(
        string='请求超时（秒）',
        config_parameter='foggy_mcp.request_timeout',
        default=30,
        help='调用 Foggy MCP 服务器的 HTTP 请求超时时间',
    )
    foggy_mcp_namespace = fields.Char(
        string='命名空间',
        config_parameter='foggy_mcp.namespace',
        default='odoo',
        help='Odoo 模型在 Foggy 中的命名空间（X-NS header 值）',
    )
    foggy_mcp_cache_ttl = fields.Integer(
        string='工具缓存时间（秒）',
        config_parameter='foggy_mcp.cache_ttl',
        default=300,
        help='缓存 Foggy tools/list 响应的时长',
    )
    foggy_mcp_auth_token = fields.Char(
        string='认证令牌',
        config_parameter='foggy_mcp.auth_token',
        help='用于 Foggy MCP 服务器认证的 Bearer token。服务器未启用认证时留空。',
    )

    # ── AI 对话（LLM）设置 ──────────────────────────

    foggy_llm_provider = fields.Selection([
        ('openai', 'OpenAI'),
        ('anthropic', 'Anthropic (Claude)'),
        ('deepseek', 'DeepSeek'),
        ('ollama', 'Ollama（本地）'),
        ('custom', '自定义（OpenAI 兼容）'),
    ], string='LLM 提供商',
        config_parameter='foggy_mcp.llm_provider',
        default='openai',
        help='内置对话功能的 AI 模型提供商。',
    )
    foggy_llm_api_key = fields.Char(
        string='LLM API 密钥',
        config_parameter='foggy_mcp.llm_api_key',
        help='LLM 提供商的 API 密钥（Ollama 无需填写）。',
    )
    foggy_llm_model = fields.Char(
        string='模型名称',
        config_parameter='foggy_mcp.llm_model',
        default='gpt-4o-mini',
        help='模型标识符（如 gpt-4o、claude-3-5-sonnet-20241022、deepseek-chat、llama3）。',
    )
    foggy_llm_base_url = fields.Char(
        string='API 基础地址',
        config_parameter='foggy_mcp.llm_base_url',
        help='自定义 API 端点。Ollama（http://localhost:11434/v1）和自定义提供商必填。',
    )
    foggy_llm_temperature = fields.Float(
        string='温度',
        config_parameter='foggy_mcp.llm_temperature',
        default=0.3,
        help='控制随机性。值越低越精确，值越高越有创造性。（0.0 - 1.0）',
    )
    foggy_llm_custom_prompt = fields.Char(
        string='业务上下文 & 自定义规则',
        config_parameter='foggy_mcp.llm_custom_prompt',
        help='注入 AI 系统提示的自定义内容。可添加：\n'
             '- 业务术语定义（如"大客户 = 年销售额 > 50 万"）\n'
             '- 额外的回答规则（如"数据展示时隐藏员工手机号"）\n'
             '- 公司特定上下文（如"财年从 4 月开始"）\n\n'
             '注意：核心安全规则（禁止编造数据）始终生效，不可覆盖。',
    )

    # ── 连接测试 ─────────────────────────────────────

    foggy_connection_status = fields.Text(
        string='连接状态',
        readonly=True,
    )

    # ── Safe Execute Override ─────────────────────────────────────

    def execute(self):
        """
        Override execute to provide better error messages for schema issues.

        When new config_parameter fields are added without running module upgrade,
        Odoo may raise "column does not exist" errors. This override catches
        those errors and provides actionable guidance.
        """
        try:
            return super().execute()
        except Exception as e:
            error_msg = str(e)

            # Check for common schema-related errors
            if 'column' in error_msg.lower() and 'does not exist' in error_msg.lower():
                _logger.error("Schema error when saving settings: %s", error_msg)
                raise UserError(_(
                    "无法保存设置，模块需要升级。\n\n"
                    "请运行以下命令：\n"
                    "    docker exec <odoo_container> odoo -u foggy_mcp -d <database> --stop-after-init\n\n"
                    "然后重启 Odoo 并重试。\n\n"
                    "技术详情：%s"
                ) % error_msg)

            # Re-raise other errors as-is
            raise

    def action_upgrade_module(self):
        """
        Action to trigger module upgrade from settings UI.
        Opens a wizard or shows instructions.
        """
        self.ensure_one()
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('需要升级模块'),
                'message': _(
                    '请运行：docker exec <odoo_container> odoo -u foggy_mcp -d <database> --stop-after-init'
                ),
                'type': 'warning',
                'sticky': True,
            }
        }

    def set_values(self):
        """保存设置时，如果引擎模式发生变更则重置单例。"""
        ICP = self.env['ir.config_parameter'].sudo()
        old_mode = ICP.get_param('foggy_mcp.engine_mode', 'embedded')
        res = super().set_values()
        new_mode = ICP.get_param('foggy_mcp.engine_mode', 'embedded')
        if old_mode != new_mode:
            _logger.info("引擎模式已变更：%s → %s，重置单例", old_mode, new_mode)
            from ..controllers.mcp_controller import _reset_singletons
            _reset_singletons()
        return res

    def action_test_connection(self):
        """
        Test connection to Foggy MCP Server with detailed error guidance.
        Provides user-friendly troubleshooting steps when connection fails.
        """
        self.ensure_one()

        url = self.foggy_mcp_url or 'http://localhost:8080'
        endpoint = self.foggy_mcp_endpoint or '/mcp/analyst/rpc'
        full_url = f"{url.rstrip('/')}{endpoint}"

        try:
            import requests

            # Try health check first
            try:
                health_url = f"{url.rstrip('/')}/actuator/health"
                r = requests.get(health_url, timeout=5)

                if r.status_code == 200:
                    # Server is healthy, try MCP endpoint
                    mcp_payload = {
                        "jsonrpc": "2.0",
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {},
                            "clientInfo": {"name": "odoo-test", "version": "1.0"}
                        },
                        "id": 1
                    }

                    headers = {'Content-Type': 'application/json'}
                    if self.foggy_mcp_auth_token:
                        headers['Authorization'] = f'Bearer {self.foggy_mcp_auth_token}'

                    r_mcp = requests.post(full_url, json=mcp_payload, headers=headers, timeout=10)

                    if r_mcp.status_code == 200:
                        result = r_mcp.json()
                        if 'result' in result:
                            server_info = result['result'].get('serverInfo', {})
                            status_msg = _(
                                "✅ 连接成功！\n\n"
                                "服务器：%(name)s v%(version)s\n"
                                "协议版本：%(protocol)s\n"
                                "地址：%(url)s"
                            ) % {
                                'name': server_info.get('name', 'Foggy MCP'),
                                'version': server_info.get('version', 'unknown'),
                                'protocol': result['result'].get('protocolVersion', 'unknown'),
                                'url': url,
                            }
                        else:
                            status_msg = _(
                                "⚠️ 服务器已响应，但 MCP 初始化失败。\n\n"
                                "响应：%(response)s"
                            ) % {'response': r_mcp.text[:200]}
                    elif r_mcp.status_code == 401:
                        status_msg = _(
                            "❌ 认证失败。\n\n"
                            "服务器需要有效的认证令牌。\n"
                            "请检查认证令牌设置。"
                        )
                    else:
                        status_msg = _(
                            "❌ MCP 端点返回 HTTP %(status)d。\n\n"
                            "响应：%(response)s"
                        ) % {'status': r_mcp.status_code, 'response': r_mcp.text[:200]}
                else:
                    status_msg = self._build_error_guidance(
                        url, f"Health check failed with HTTP {r.status_code}"
                    )

            except requests.exceptions.ConnectionError as e:
                status_msg = self._build_error_guidance(url, str(e))
            except requests.exceptions.Timeout:
                status_msg = self._build_error_guidance(
                    url, "Connection timed out after 5 seconds"
                )

        except ImportError:
            status_msg = _(
                "❌ 缺少必要的库。\n\n"
                "请安装：pip install requests"
            )

        # Show result in a modal dialog with the status message
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'foggy.connection.test.result',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_status_message': status_msg,
            }
        }

    def _build_error_guidance(self, url, error_detail):
        """
        Build user-friendly error message with troubleshooting steps.

        Args:
            url: The URL that failed to connect
            error_detail: Technical error message

        Returns:
            str: User-friendly error message with troubleshooting guidance
        """
        import os

        # Detect environment
        in_docker = os.path.exists('/.dockerenv')

        guidance_lines = [
            _("❌ 无法连接到 Foggy MCP 服务器"),
            "",
            _("地址：%(url)s") % {'url': url},
            _("错误：%(error)s") % {'error': error_detail},
            "",
            "═══════════════════════════════════════",
            _("排查步骤："),
            "",
            _("1. 检查 Foggy MCP 容器是否运行中："),
            "   docker ps | grep foggy-mcp",
            "",
            _("2. 查看容器日志是否有错误："),
            "   docker logs foggy-mcp",
            "",
        ]

        if in_docker:
            guidance_lines.extend([
                _("3. Odoo 和 Foggy 均在 Docker 中："),
                _("   • 确保它们在同一网络中：docker network ls"),
                _("   • 尝试使用地址：http://foggy-mcp:8080"),
                "",
                _("4. 或使用 Odoo 的容器网络："),
                "   docker run --network container:odoo ...",
            ])
        else:
            guidance_lines.extend([
                _("3. 检查端口是否可访问："),
                f"   curl http://localhost:8080/actuator/health",
                "",
                _("4. 如果运行在 Docker 中，检查端口映射："),
                "   docker port foggy-mcp",
            ])

        guidance_lines.extend([
            "",
            _("5. 确认认证令牌与服务器配置一致。"),
        ])

        return '\n'.join(guidance_lines)
