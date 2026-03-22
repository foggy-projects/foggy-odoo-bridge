# -*- coding: utf-8 -*-
"""Odoo 公司模型（res_company）"""
from ._helpers import odoo_model, dim, dim_time, measure_count, fk_join


def create_res_company_model():
    return odoo_model(
        name='OdooResCompanyQueryModel',
        table='res_company',
        alias='公司',
        dimensions={
            'id': dim('id', 'id', '公司ID'),
            'name': dim('name', 'name', '公司名称'),
            'email': dim('email', 'email', '邮箱'),
            'phone': dim('phone', 'phone', '电话'),
            'active': dim('active', 'active', '启用'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'companyCount': measure_count('companyCount', 'id', '公司数'),
        },
        dimension_joins=[
            fk_join('currency', 'res_currency', 'currency_id', caption='币种'),
            fk_join('parent', 'res_company', 'parent_id', caption='上级公司'),
        ],
    )
