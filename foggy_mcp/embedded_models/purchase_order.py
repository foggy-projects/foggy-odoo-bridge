# -*- coding: utf-8 -*-
"""Odoo 采购订单模型（purchase_order）"""
from ._helpers import (
    odoo_model, dim, dim_time, measure_sum, measure_count,
    fk_join, prop,
)


def create_purchase_order_model():
    return odoo_model(
        name='OdooPurchaseOrderQueryModel',
        table='purchase_order',
        alias='采购订单',
        dimensions={
            'id': dim('id', 'id', '订单ID'),
            'name': dim('name', 'name', '订单编号'),
            'state': dim('state', 'state', '订单状态'),
            'dateOrder': dim_time('dateOrder', 'date_order', '下单日期'),
            'dateApprove': dim_time('dateApprove', 'date_approve', '批准日期'),
            'invoiceStatus': dim('invoiceStatus', 'invoice_status', '账单状态'),
            'origin': dim('origin', 'origin', '来源单据'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'amountUntaxed': measure_sum('amountUntaxed', 'amount_untaxed', '未税金额'),
            'amountTax': measure_sum('amountTax', 'amount_tax', '税额'),
            'amountTotal': measure_sum('amountTotal', 'amount_total', '总金额'),
            'orderCount': measure_count('orderCount', 'id', '订单数'),
        },
        dimension_joins=[
            fk_join('vendor', 'res_partner', 'partner_id', caption='供应商',
                    properties=[
                        prop('email', caption='邮箱'),
                        prop('phone', caption='电话'),
                        prop('city', caption='城市'),
                    ]),
            fk_join('buyer', 'res_users', 'user_id', caption='采购员',
                    caption_column='login'),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
            fk_join('currency', 'res_currency', 'currency_id', caption='币种'),
        ],
    )
