# -*- coding: utf-8 -*-
"""Odoo 会计分录/发票模型（account_move）"""
from ._helpers import (
    odoo_model, dim, dim_day, dim_time, measure_sum, measure_count,
    fk_join, prop,
)


def create_account_move_model():
    return odoo_model(
        name='OdooAccountMoveQueryModel',
        table='account_move',
        alias='会计分录/发票',
        dimensions={
            'id': dim('id', 'id', '分录ID'),
            'name': dim('name', 'name', '编号'),
            'moveType': dim('moveType', 'move_type', '类型'),
            'state': dim('state', 'state', '状态'),
            'date': dim_day('date', 'date', '会计日期'),
            'invoiceDate': dim_day('invoiceDate', 'invoice_date', '发票日期'),
            'invoiceDateDue': dim_day('invoiceDateDue', 'invoice_date_due', '到期日'),
            'paymentState': dim('paymentState', 'payment_state', '付款状态'),
            'ref': dim('ref', 'ref', '参考'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'amountUntaxed': measure_sum('amountUntaxed', 'amount_untaxed', '未税金额'),
            'amountTax': measure_sum('amountTax', 'amount_tax', '税额'),
            'amountTotal': measure_sum('amountTotal', 'amount_total', '总金额'),
            'amountResidual': measure_sum('amountResidual', 'amount_residual', '未付金额'),
            'amountTotalSigned': measure_sum('amountTotalSigned', 'amount_total_signed', '总金额(签名)'),
            'amountResidualSigned': measure_sum('amountResidualSigned', 'amount_residual_signed', '未付金额(签名)'),
            'entryCount': measure_count('entryCount', 'id', '分录数'),
        },
        dimension_joins=[
            fk_join('partner', 'res_partner', 'partner_id', caption='合作伙伴',
                    properties=[
                        prop('email', caption='邮箱'),
                        prop('phone', caption='电话'),
                        prop('city', caption='城市'),
                        prop('is_company', name='isCompany', caption='是否公司', data_type='BOOLEAN'),
                    ]),
            fk_join('journal', 'account_journal', 'journal_id', caption='日记账',
                    properties=[
                        prop('code', caption='代码'),
                        prop('type', caption='类型'),
                    ]),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
            fk_join('currency', 'res_currency', 'currency_id', caption='币种'),
            fk_join('salesperson', 'res_users', 'invoice_user_id', caption='销售员',
                    caption_column='login'),
        ],
    )
