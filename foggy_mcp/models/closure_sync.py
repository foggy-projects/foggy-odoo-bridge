# -*- coding: utf-8 -*-
"""
Closure Table Auto-Sync — 标记脏 + 合并刷新。

当 res.company / hr.department / hr.employee / res.partner 的层级关系
（parent_id 字段）发生变化时，ORM 钩子仅标记对应闭包表为"脏"，
由高频 cron（每 5 分钟）统一执行全量刷新。

设计要点：
- ORM 钩子只做一次轻量 UPDATE（标记脏），不阻塞业务操作
- 批量导入 1000 条记录 → 1000 次标记（每次 < 1ms）→ cron 合并为 1 次刷新
- 多 worker 安全：脏标记存在 ir.config_parameter（DB 级共享）
- 等待合并：cron 执行时若发现脏标记，先清标记再刷新；刷新期间新的标记
  会在下一轮 cron 处理，不会丢失
- 全量刷新：因为 parent_id 变化影响整棵树，增量更新复杂度远高于全量重建
"""
import logging
import time

from odoo import api, models

_logger = logging.getLogger(__name__)

# ir.config_parameter key → PostgreSQL refresh function
_CLOSURE_MAP = {
    'foggy_mcp.closure_dirty.company': 'refresh_company_closure',
    'foggy_mcp.closure_dirty.department': 'refresh_department_closure',
    'foggy_mcp.closure_dirty.employee': 'refresh_employee_closure',
    'foggy_mcp.closure_dirty.partner': 'refresh_partner_closure',
}

# Odoo model → dirty flag key
_MODEL_DIRTY_KEY = {
    'res.company': 'foggy_mcp.closure_dirty.company',
    'hr.department': 'foggy_mcp.closure_dirty.department',
    'hr.employee': 'foggy_mcp.closure_dirty.employee',
    'res.partner': 'foggy_mcp.closure_dirty.partner',
}


def _mark_dirty(env, model_name):
    """Mark a closure table as dirty (needs refresh).

    Extremely lightweight — single UPDATE on ir_config_parameter.
    Safe for batch operations (1000 calls ≈ 1000 * ~0.1ms = ~100ms total).
    """
    key = _MODEL_DIRTY_KEY.get(model_name)
    if not key:
        return
    try:
        env['ir.config_parameter'].sudo().set_param(key, str(time.time()))
    except Exception as e:
        _logger.debug("Failed to mark closure dirty for %s: %s", model_name, e)


def refresh_dirty_closures(env):
    """Check dirty flags and refresh corresponding closure tables.

    Called by ir.cron every 5 minutes. Only refreshes tables that have been
    marked dirty since last refresh.

    Algorithm:
    1. Read all dirty flags
    2. Clear flags BEFORE refresh (so new marks during refresh aren't lost)
    3. Execute refresh functions for dirty tables
    """
    ICP = env['ir.config_parameter'].sudo()
    cr = env.cr

    refreshed = []
    for param_key, func_name in _CLOSURE_MAP.items():
        dirty_ts = ICP.get_param(param_key, '')
        if not dirty_ts:
            continue

        # Clear flag first — new dirty marks during refresh will trigger next cycle
        ICP.set_param(param_key, '')

        # Check if refresh function exists
        try:
            cr.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = %s)",
                (func_name,)
            )
            if not cr.fetchone()[0]:
                continue

            cr.execute(f"SELECT {func_name}()")
            refreshed.append(func_name)
        except Exception as e:
            _logger.warning("Closure refresh failed (%s): %s", func_name, e)
            # Re-mark dirty so next cycle retries
            ICP.set_param(param_key, str(time.time()))

    if refreshed:
        _logger.info("Closure tables refreshed: %s", ', '.join(refreshed))


# ── Cron entry point (called by ir.cron) ─────────────────────────────

class FoggyClosureCron(models.AbstractModel):
    """Abstract model providing cron-callable method for closure refresh."""
    _name = 'foggy.closure.cron'
    _description = 'Foggy Closure Table Cron'

    def _cron_flush_dirty(self):
        """Called by ir.cron every 5 minutes to flush dirty closure tables."""
        refresh_dirty_closures(self.env)


# ── ORM Hooks: mark dirty on parent_id changes ──────────────────────

class ResCompanyClosureSync(models.Model):
    _inherit = 'res.company'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _mark_dirty(self.env, 'res.company')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _mark_dirty(self.env, 'res.company')
        return result

    def unlink(self):
        result = super().unlink()
        _mark_dirty(self.env, 'res.company')
        return result


class HrDepartmentClosureSync(models.Model):
    _inherit = 'hr.department'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _mark_dirty(self.env, 'hr.department')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _mark_dirty(self.env, 'hr.department')
        return result

    def unlink(self):
        result = super().unlink()
        _mark_dirty(self.env, 'hr.department')
        return result


class HrEmployeeClosureSync(models.Model):
    _inherit = 'hr.employee'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _mark_dirty(self.env, 'hr.employee')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _mark_dirty(self.env, 'hr.employee')
        return result

    def unlink(self):
        result = super().unlink()
        _mark_dirty(self.env, 'hr.employee')
        return result


class ResPartnerClosureSync(models.Model):
    _inherit = 'res.partner'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _mark_dirty(self.env, 'res.partner')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _mark_dirty(self.env, 'res.partner')
        return result

    def unlink(self):
        result = super().unlink()
        _mark_dirty(self.env, 'res.partner')
        return result
