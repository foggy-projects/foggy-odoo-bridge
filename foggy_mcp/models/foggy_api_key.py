# -*- coding: utf-8 -*-
import logging
import secrets
import string

import markupsafe

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# API Key prefix for easy identification
API_KEY_PREFIX = 'fmcp_'
API_KEY_LENGTH = 40


def _generate_api_key():
    """Generate a secure random API key with prefix."""
    alphabet = string.ascii_letters + string.digits
    random_part = ''.join(secrets.choice(alphabet) for _ in range(API_KEY_LENGTH))
    return f"{API_KEY_PREFIX}{random_part}"


class FoggyApiKey(models.Model):
    _name = 'foggy.api.key'
    _description = 'Foggy MCP API Key'
    _order = 'create_date desc'

    name = fields.Char(
        string='名称',
        required=True,
        help='API 密钥的可读名称（例如："Claude Desktop - 工作电脑"）',
    )
    key = fields.Char(
        string='API 密钥',
        readonly=True,
        copy=False,
        help='API 密钥令牌，创建时自动生成。',
    )
    user_id = fields.Many2one(
        'res.users',
        string='用户',
        required=True,
        default=lambda self: self.env.uid,
        ondelete='cascade',
        help='此 API 密钥认证的 Odoo 用户。',
    )
    active = fields.Boolean(
        string='启用',
        default=True,
        help='停用可撤销访问权限而不删除密钥。',
    )
    last_used = fields.Datetime(
        string='最后使用',
        readonly=True,
        help='最后一次成功认证的时间。',
    )
    company_ids = fields.Many2many(
        'res.company',
        string='允许的公司',
        help='设置后将限制此密钥只能访问指定公司。留空表示允许用户所有公司。',
    )
    key_short = fields.Char(
        string='密钥预览',
        compute='_compute_key_short',
        help='密钥掩码预览（打开记录可复制完整密钥）。',
    )
    mcp_endpoint = fields.Char(
        string='MCP 端点',
        compute='_compute_mcp_config',
        help='此 Odoo 实例的完整 MCP 端点 URL。',
    )
    mcp_config_html = fields.Html(
        string='MCP 配置',
        compute='_compute_mcp_config',
        sanitize=False,
        help='即用的 MCP 客户端配置（格式化 HTML）。',
    )

    @api.depends('key')
    def _compute_key_short(self):
        for rec in self:
            if rec.key:
                # Show prefix + first 8 chars + mask
                rec.key_short = rec.key[:13] + '****'
            else:
                rec.key_short = ''

    _MCP_CONFIG_TEMPLATE = (
        '{\n'
        '  "mcpServers": {\n'
        '    "odoo": {\n'
        '      "url": "BASE_URL/foggy-mcp/rpc",\n'
        '      "headers": {\n'
        '        "Authorization": "Bearer API_KEY"\n'
        '      }\n'
        '    }\n'
        '  }\n'
        '}'
    )

    @api.depends('key')
    def _compute_mcp_config(self):
        base_url = self.env['ir.config_parameter'].sudo().get_param(
            'web.base.url', 'http://localhost:8069'
        )
        endpoint = base_url.rstrip('/') + '/foggy-mcp/rpc'
        pre_style = (
            'background:#f8f9fa; padding:12px; border-radius:4px; '
            'font-size:13px; line-height:1.6; margin:0;'
        )
        for rec in self:
            rec.mcp_endpoint = endpoint
            if rec.key:
                json_text = self._MCP_CONFIG_TEMPLATE.replace(
                    'BASE_URL', base_url.rstrip('/'),
                ).replace(
                    'API_KEY', rec.key,
                )
                escaped = markupsafe.escape(json_text)
                rec.mcp_config_html = markupsafe.Markup(
                    '<pre style="%s">%s</pre>'
                ) % (pre_style, escaped)
            else:
                rec.mcp_config_html = ''

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get('key'):
                vals['key'] = _generate_api_key()
        records = super().create(vals_list)
        return records

    def action_regenerate_key(self):
        """Regenerate the API key."""
        self.ensure_one()
        self.key = _generate_api_key()
        _logger.info("API key regenerated for user %s (key: %s)", self.user_id.login, self.id)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'API 密钥已重新生成',
                'message': f'新密钥：{self.key}',
                'type': 'success',
                'sticky': True,
            }
        }

    @api.model
    def ensure_user_key(self, user_id):
        """
        Ensure that a user has at least one active API key.
        Auto-creates one if none exists.

        Args:
            user_id: res.users record ID

        Returns:
            foggy.api.key record (existing or newly created)
        """
        existing = self.sudo().search([
            ('user_id', '=', user_id),
            ('active', '=', True),
        ], limit=1)
        if existing:
            return existing

        user = self.env['res.users'].sudo().browse(user_id)
        new_key = self.sudo().create({
            'name': f'Auto — {user.name}',
            'user_id': user_id,
        })
        _logger.info(
            "Auto-created Foggy MCP API key for user %s (uid=%s, key_id=%s)",
            user.login, user_id, new_key.id,
        )
        return new_key

    def action_open_my_api_key(self):
        """Open the current user's API key directly (create if needed)."""
        key = self.ensure_user_key(self.env.uid)
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'foggy.api.key',
            'res_id': key.id,
            'view_mode': 'form',
            'target': 'current',
        }

    @api.model
    def authenticate_by_key(self, key):
        """
        Authenticate a request by API key.

        Args:
            key: The API key (with or without 'Bearer ' prefix)

        Returns:
            res.users record if valid, False otherwise
        """
        if not key:
            return False

        # Strip 'Bearer ' prefix
        if key.startswith('Bearer '):
            key = key[7:]

        if not key.startswith(API_KEY_PREFIX):
            return False

        api_key = self.sudo().search([
            ('key', '=', key),
            ('active', '=', True),
        ], limit=1)

        if not api_key:
            _logger.warning("Invalid or inactive API key attempted: %s...", key[:12])
            return False

        # Update last_used timestamp
        api_key.sudo().write({'last_used': fields.Datetime.now()})
        _logger.debug("API key authenticated: user=%s, key_id=%s", api_key.user_id.login, api_key.id)
        return api_key.user_id
