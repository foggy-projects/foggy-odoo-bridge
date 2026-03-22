# -*- coding: utf-8 -*-
"""Odoo 合作伙伴模型（res_partner）"""
from ._helpers import (
    odoo_model, dim, dim_time, measure_count,
    fk_join, prop,
)


def create_res_partner_model():
    return odoo_model(
        name='OdooResPartnerQueryModel',
        table='res_partner',
        alias='合作伙伴',
        dimensions={
            'id': dim('id', 'id', '伙伴ID'),
            'name': dim('name', 'name', '名称'),
            'type': dim('type', 'type', '类型'),
            'email': dim('email', 'email', '邮箱'),
            'phone': dim('phone', 'phone', '电话'),
            'mobile': dim('mobile', 'mobile', '手机'),
            'city': dim('city', 'city', '城市'),
            'street': dim('street', 'street', '街道'),
            'zip': dim('zip', 'zip', '邮编'),
            'website': dim('website', 'website', '网站'),
            'vat': dim('vat', 'vat', '税号'),
            'ref': dim('ref', 'ref', '内部参考'),
            'isCompany': dim('isCompany', 'is_company', '是否公司'),
            'active': dim('active', 'active', '启用'),
            'customerRank': dim('customerRank', 'customer_rank', '客户等级'),
            'supplierRank': dim('supplierRank', 'supplier_rank', '供应商等级'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'partnerCount': measure_count('partnerCount', 'id', '伙伴数'),
        },
        dimension_joins=[
            fk_join('company', 'res_company', 'company_id', caption='公司'),
            fk_join('country', 'res_country', 'country_id', caption='国家',
                    properties=[prop('code', caption='国家代码')]),
            fk_join('state', 'res_country_state', 'state_id', caption='省/州',
                    properties=[prop('code', caption='省/州代码')]),
            fk_join('parentPartner', 'res_partner', 'parent_id', caption='上级公司'),
            fk_join('salesperson', 'res_users', 'user_id', caption='销售员',
                    caption_column='login'),
        ],
    )
