# -*- coding: utf-8 -*-
"""Extend res.users for Foggy MCP."""

from odoo import models


class ResUsers(models.Model):
    _inherit = 'res.users'
