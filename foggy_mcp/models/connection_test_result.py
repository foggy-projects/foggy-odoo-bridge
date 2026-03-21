# -*- coding: utf-8 -*-
"""
Foggy MCP Connection Test Result Dialog

Transient model to display connection test results in a modal dialog.
"""
import logging

from odoo import fields, models

_logger = logging.getLogger(__name__)


class FoggyConnectionTestResult(models.TransientModel):
    _name = 'foggy.connection.test.result'
    _description = 'Foggy MCP 连接测试结果'

    status_message = fields.Text(
        string='连接结果',
        readonly=True,
    )
