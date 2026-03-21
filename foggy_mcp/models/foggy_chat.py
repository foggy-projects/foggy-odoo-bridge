# -*- coding: utf-8 -*-
"""
Foggy AI Chat — Odoo models for LLM-powered data analysis conversations.

Models:
    - foggy.chat.session: Chat session (one per conversation thread)
    - foggy.chat.message: Individual messages in a session
"""
import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class FoggyChatSession(models.Model):
    _name = 'foggy.chat.session'
    _description = 'Foggy AI 对话'
    _order = 'write_date desc'

    name = fields.Char(
        string='标题',
        default='新对话',
        help='从用户首条消息自动生成。',
    )
    user_id = fields.Many2one(
        'res.users',
        string='用户',
        required=True,
        default=lambda self: self.env.uid,
        ondelete='cascade',
    )
    message_ids = fields.One2many(
        'foggy.chat.message', 'session_id',
        string='消息',
    )
    message_count = fields.Integer(
        compute='_compute_message_count',
    )

    @api.depends('message_ids')
    def _compute_message_count(self):
        for rec in self:
            rec.message_count = len(rec.message_ids)


class FoggyChatMessage(models.Model):
    _name = 'foggy.chat.message'
    _description = 'Foggy AI 对话消息'
    _order = 'create_date asc, id asc'

    session_id = fields.Many2one(
        'foggy.chat.session',
        string='对话',
        required=True,
        ondelete='cascade',
    )
    role = fields.Selection([
        ('user', '用户'),
        ('assistant', '助手'),
        ('system', '系统'),
        ('tool', '工具结果'),
    ], string='角色', required=True)
    content = fields.Text(
        string='内容',
        help='消息文本内容。',
    )
    tool_calls_json = fields.Text(
        string='工具调用',
        help='LLM 发出的工具调用请求（JSON 编码）。',
    )
    tool_results_json = fields.Text(
        string='工具结果',
        help='工具执行结果（JSON 编码）。',
    )
