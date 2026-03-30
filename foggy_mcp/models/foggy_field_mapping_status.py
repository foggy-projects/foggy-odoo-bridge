# -*- coding: utf-8 -*-
"""Basic permission field mapping status for the Community Edition."""
import logging

from odoo import api, fields, models
from odoo.tools.safe_eval import safe_eval

from ..services.permission_bridge import DIRECT_FIELD_MAP, HIERARCHY_FIELD_MAP
from ..services.tool_registry import QM_TO_ODOO_MODEL

_logger = logging.getLogger(__name__)


class FoggyFieldMappingStatus(models.Model):
    _name = 'foggy.field.mapping.status'
    _description = 'Permission Field Mapping Status'
    _order = 'status desc, odoo_model, rule_field'

    odoo_model = fields.Char(string='Odoo Model', required=True, readonly=True)
    qm_model = fields.Char(string='QM Model', readonly=True)
    rule_name = fields.Char(string='Rule Name', readonly=True)
    rule_field = fields.Char(string='ir.rule Field', required=True, readonly=True)
    qm_field = fields.Char(string='QM Field', readonly=True)
    status = fields.Selection([
        ('mapped', 'Mapped'),
        ('unmapped', 'Unmapped'),
    ], string='Status', required=True, readonly=True)
    status_display = fields.Char(
        string='Status Display', compute='_compute_status_display', store=False,
    )

    @api.depends('status', 'qm_field')
    def _compute_status_display(self):
        for rec in self:
            if rec.status == 'mapped':
                rec.status_display = '\u2705 \u5df2\u6620\u5c04'
            else:
                rec.status_display = '\u274c \u672a\u6620\u5c04'

    @api.model
    def refresh_mapping_status(self):
        """Scan all ir.rule records for QM-mapped models and check field mapping coverage.

        Returns:
            dict: {'total': int, 'mapped': int, 'unmapped': int}
        """
        all_field_map = {}
        all_field_map.update(DIRECT_FIELD_MAP)
        all_field_map.update(HIERARCHY_FIELD_MAP)

        records_to_create = []
        seen = set()  # (odoo_model, rule_name, field) dedup

        for qm_model, odoo_model in QM_TO_ODOO_MODEL.items():
            if odoo_model not in self.env:
                continue

            model_id = self.env['ir.model'].sudo().search(
                [('model', '=', odoo_model)], limit=1,
            )
            if not model_id:
                continue

            rules = self.env['ir.rule'].sudo().search([
                ('model_id', '=', model_id.id),
                ('perm_read', '=', True),
                ('active', '=', True),
            ])

            for rule in rules:
                rule_fields = self._extract_domain_fields(rule.domain_force)

                for field_name in rule_fields:
                    dedup_key = (odoo_model, rule.name, field_name)
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)

                    qm_field = all_field_map.get(field_name, '')
                    status = 'mapped' if qm_field else 'unmapped'

                    records_to_create.append({
                        'odoo_model': odoo_model,
                        'qm_model': qm_model,
                        'rule_name': rule.name or '',
                        'rule_field': field_name,
                        'qm_field': qm_field,
                        'status': status,
                    })

        # Replace all existing records
        self.sudo().search([]).unlink()
        if records_to_create:
            self.sudo().create(records_to_create)

        total = len(records_to_create)
        unmapped = sum(1 for r in records_to_create if r['status'] == 'unmapped')
        mapped = total - unmapped

        _logger.info(
            "Field mapping scan complete: %d total, %d mapped, %d unmapped",
            total, mapped, unmapped,
        )

        return {'total': total, 'mapped': mapped, 'unmapped': unmapped}

    # ── Domain field extraction ───────────────────────────────────

    @staticmethod
    def _extract_domain_fields(domain_force):
        """Extract field names referenced in a domain_force expression.

        Args:
            domain_force: String representation of an Odoo domain, e.g.
                "[('company_id', 'in', company_ids)]"

        Returns:
            set: Field names (e.g., {'company_id', 'user_id'})
        """
        if not domain_force:
            return set()

        field_names = set()
        try:
            eval_ctx = {
                'user': _Placeholder(),
                'uid': 1,
                'company_id': 1,
                'company_ids': [1],
                'time': _Placeholder(),
                'datetime': _Placeholder(),
                'True': True,
                'False': False,
            }
            domain = safe_eval(domain_force, eval_ctx)

            for element in domain:
                if isinstance(element, (list, tuple)) and len(element) == 3:
                    field = element[0]
                    if isinstance(field, int):
                        continue
                    if isinstance(field, str):
                        if '.' in field:
                            field = field.split('.')[0]
                        field_names.add(field)
        except Exception as e:
            _logger.debug(
                "Could not parse domain_force for field extraction: %s — %s",
                domain_force[:100], e,
            )

        return field_names


class _Placeholder:
    """Placeholder object for safe_eval context."""

    def __getattr__(self, name):
        return _Placeholder()

    def __int__(self):
        return 0

    def __bool__(self):
        return True

    @property
    def id(self):
        return 1

    @property
    def ids(self):
        return [1]

    @property
    def company_id(self):
        return _Placeholder()

    @property
    def company_ids(self):
        return _Placeholder()
