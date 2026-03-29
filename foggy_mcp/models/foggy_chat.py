# -*- coding: utf-8 -*-
"""
Foggy AI Chat — Odoo models for LLM-powered data analysis conversations.

Models:
    - foggy.chat.session: Chat session (one per conversation thread)
    - foggy.chat.message: Individual messages in a session
"""
import json
import logging

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)


class FoggyChatSession(models.Model):
    _name = 'foggy.chat.session'
    _description = 'Foggy AI Chat'
    _order = 'write_date desc'

    name = fields.Char(
        string='Title',
        default=lambda self: _('New Chat'),
        help='Generated automatically from the first user message.',
    )
    user_id = fields.Many2one(
        'res.users',
        string='User',
        required=True,
        default=lambda self: self.env.uid,
        ondelete='cascade',
    )
    message_ids = fields.One2many(
        'foggy.chat.message', 'session_id',
        string='Messages',
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
    _description = 'Foggy AI Chat Message'
    _order = 'create_date asc, id asc'

    session_id = fields.Many2one(
        'foggy.chat.session',
        string='Session',
        required=True,
        ondelete='cascade',
    )
    role = fields.Selection([
        ('user', 'User'),
        ('assistant', 'Assistant'),
        ('system', 'System'),
        ('tool', 'Tool Result'),
    ], string='Role', required=True)
    content = fields.Text(
        string='Content',
        help='Message text content.',
    )
    tool_calls_json = fields.Text(
        string='Tool Calls',
        help='Tool call requests emitted by the LLM (JSON encoded).',
    )
    tool_results_json = fields.Text(
        string='Tool Results',
        help='Tool execution results (JSON encoded).',
    )
