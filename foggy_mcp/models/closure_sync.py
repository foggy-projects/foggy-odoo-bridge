# -*- coding: utf-8 -*-
"""
Closure Table Auto-Sync — 通过 Odoo ORM 继承实现实时同步。

当 res.company / hr.department / hr.employee / res.partner 的层级关系
（parent_id 字段）发生变化时，自动刷新对应的闭包表。

策略：
- 仅在 parent_id 变化时触发（不是每次 write 都刷新）
- 使用 PostgreSQL 函数做全量刷新（递归 CTE，数据量小，通常 < 100ms）
- 通过 Odoo ORM 的 write/create/unlink 钩子触发，天然在事务内执行
- 如果闭包表尚未创建（函数不存在），静默跳过
"""
import logging
from odoo import api, models

_logger = logging.getLogger(__name__)


def _refresh_closure(cr, func_name, model_name):
    """Call a PostgreSQL refresh function if it exists.

    Safe to call even if closure tables haven't been initialized yet.
    Runs within the current Odoo transaction — no concurrency issues.
    """
    try:
        cr.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = %s)",
            (func_name,)
        )
        if cr.fetchone()[0]:
            cr.execute(f"SELECT {func_name}()")
            _logger.debug("Closure refreshed: %s (triggered by %s)", func_name, model_name)
    except Exception as e:
        # Never block the original write — closure is supplementary
        _logger.warning("Closure refresh failed (%s): %s", func_name, e)


class ResCompanyClosureSync(models.Model):
    """Auto-refresh res_company_closure on hierarchy changes."""
    _inherit = 'res.company'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _refresh_closure(self.env.cr, 'refresh_company_closure', 'res.company')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _refresh_closure(self.env.cr, 'refresh_company_closure', 'res.company')
        return result

    def unlink(self):
        result = super().unlink()
        _refresh_closure(self.env.cr, 'refresh_company_closure', 'res.company')
        return result


class HrDepartmentClosureSync(models.Model):
    """Auto-refresh hr_department_closure on hierarchy changes."""
    _inherit = 'hr.department'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _refresh_closure(self.env.cr, 'refresh_department_closure', 'hr.department')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _refresh_closure(self.env.cr, 'refresh_department_closure', 'hr.department')
        return result

    def unlink(self):
        result = super().unlink()
        _refresh_closure(self.env.cr, 'refresh_department_closure', 'hr.department')
        return result


class HrEmployeeClosureSync(models.Model):
    """Auto-refresh hr_employee_closure on hierarchy changes."""
    _inherit = 'hr.employee'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _refresh_closure(self.env.cr, 'refresh_employee_closure', 'hr.employee')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _refresh_closure(self.env.cr, 'refresh_employee_closure', 'hr.employee')
        return result

    def unlink(self):
        result = super().unlink()
        _refresh_closure(self.env.cr, 'refresh_employee_closure', 'hr.employee')
        return result


class ResPartnerClosureSync(models.Model):
    """Auto-refresh res_partner_closure on hierarchy changes."""
    _inherit = 'res.partner'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        if any('parent_id' in v for v in vals_list):
            _refresh_closure(self.env.cr, 'refresh_partner_closure', 'res.partner')
        return records

    def write(self, vals):
        result = super().write(vals)
        if 'parent_id' in vals:
            _refresh_closure(self.env.cr, 'refresh_partner_closure', 'res.partner')
        return result

    def unlink(self):
        result = super().unlink()
        _refresh_closure(self.env.cr, 'refresh_partner_closure', 'res.partner')
        return result
