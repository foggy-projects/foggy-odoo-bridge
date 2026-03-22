# -*- coding: utf-8 -*-
"""Odoo 库存调拨模型（stock_picking）"""
from ._helpers import (
    odoo_model, dim, dim_time, measure_count,
    fk_join, prop,
)


def create_stock_picking_model():
    return odoo_model(
        name='OdooStockPickingQueryModel',
        table='stock_picking',
        alias='库存调拨',
        dimensions={
            'id': dim('id', 'id', '调拨ID'),
            'name': dim('name', 'name', '参考号'),
            'state': dim('state', 'state', '状态'),
            'origin': dim('origin', 'origin', '来源单据'),
            'scheduledDate': dim_time('scheduledDate', 'scheduled_date', '计划日期'),
            'dateDone': dim_time('dateDone', 'date_done', '完成日期'),
            'priority': dim('priority', 'priority', '优先级'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'transferCount': measure_count('transferCount', 'id', '调拨数'),
        },
        dimension_joins=[
            fk_join('partner', 'res_partner', 'partner_id', caption='联系人',
                    properties=[prop('email', caption='邮箱'), prop('city', caption='城市')]),
            fk_join('pickingType', 'stock_picking_type', 'picking_type_id', caption='操作类型',
                    properties=[prop('code', caption='代码')]),
            fk_join('locationSrc', 'stock_location', 'location_id', caption='源库位',
                    caption_column='complete_name'),
            fk_join('locationDest', 'stock_location', 'location_dest_id', caption='目标库位',
                    caption_column='complete_name'),
            fk_join('responsible', 'res_users', 'user_id', caption='负责人',
                    caption_column='login'),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
        ],
    )
