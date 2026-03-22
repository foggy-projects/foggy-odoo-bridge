# -*- coding: utf-8 -*-
"""Odoo 销售订单模型（sale_order + sale_order_line）"""
from ._helpers import (
    odoo_model, dim, dim_time, measure_sum, measure_count,
    fk_join, prop,
)


def create_sale_order_model():
    return odoo_model(
        name='OdooSaleOrderQueryModel',
        table='sale_order',
        alias='销售订单',
        dimensions={
            'id': dim('id', 'id', '订单ID'),
            'name': dim('name', 'name', '订单编号'),
            'state': dim('state', 'state', '订单状态'),
            'dateOrder': dim_time('dateOrder', 'date_order', '下单日期'),
            'invoiceStatus': dim('invoiceStatus', 'invoice_status', '开票状态'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'amountUntaxed': measure_sum('amountUntaxed', 'amount_untaxed', '未税金额'),
            'amountTax': measure_sum('amountTax', 'amount_tax', '税额'),
            'amountTotal': measure_sum('amountTotal', 'amount_total', '总金额'),
            'orderCount': measure_count('orderCount', 'id', '订单数'),
        },
        dimension_joins=[
            fk_join('partner', 'res_partner', 'partner_id', caption='客户',
                    properties=[
                        prop('email', caption='邮箱'),
                        prop('phone', caption='电话'),
                        prop('city', caption='城市'),
                        prop('is_company', name='isCompany', caption='是否公司', data_type='BOOLEAN'),
                    ]),
            fk_join('salesperson', 'res_users', 'user_id', caption='销售员',
                    caption_column='login'),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
            fk_join('salesTeam', 'crm_team', 'team_id', caption='销售团队'),
        ],
    )


def create_sale_order_line_model():
    return odoo_model(
        name='OdooSaleOrderLineQueryModel',
        table='sale_order_line',
        alias='销售订单行',
        dimensions={
            'id': dim('id', 'id', '行ID'),
            'name': dim('name', 'name', '描述'),
            'invoiceStatus': dim('invoiceStatus', 'invoice_status', '开票状态'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'productUomQty': measure_sum('productUomQty', 'product_uom_qty', '订购数量'),
            'qtyDelivered': measure_sum('qtyDelivered', 'qty_delivered', '已发货数量'),
            'qtyInvoiced': measure_sum('qtyInvoiced', 'qty_invoiced', '已开票数量'),
            'priceSubtotal': measure_sum('priceSubtotal', 'price_subtotal', '小计'),
            'priceTotal': measure_sum('priceTotal', 'price_total', '合计'),
            'lineCount': measure_count('lineCount', 'id', '行数'),
        },
        dimension_joins=[
            fk_join('order', 'sale_order', 'order_id', caption='销售订单'),
            fk_join('product', 'product_product', 'product_id', caption='产品'),
            fk_join('salesperson', 'res_users', 'salesman_id', caption='销售员',
                    caption_column='login'),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
        ],
    )
