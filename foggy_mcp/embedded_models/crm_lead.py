# -*- coding: utf-8 -*-
"""Odoo CRM 线索/商机模型（crm_lead）"""
from ._helpers import (
    odoo_model, dim, dim_day, dim_time,
    measure_sum, measure_count, measure_avg,
    fk_join, prop,
)


def create_crm_lead_model():
    return odoo_model(
        name='OdooCrmLeadModel',
        table='crm_lead',
        alias='CRM 线索/商机',
        dimensions={
            'id': dim('id', 'id', '线索ID'),
            'name': dim('name', 'name', '线索名称'),
            'type': dim('type', 'type', '类型'),
            'priority': dim('priority', 'priority', '优先级'),
            'active': dim('active', 'active', '活跃'),
            'contactName': dim('contactName', 'contact_name', '联系人'),
            'partnerName': dim('partnerName', 'partner_name', '公司名称'),
            'emailFrom': dim('emailFrom', 'email_from', '邮箱'),
            'phone': dim('phone', 'phone', '电话'),
            'city': dim('city', 'city', '城市'),
            'dateDeadline': dim_day('dateDeadline', 'date_deadline', '预计成交日'),
            'dateOpen': dim_time('dateOpen', 'date_open', '分配日期'),
            'dateClosed': dim_time('dateClosed', 'date_closed', '关闭日期'),
            'createDate': dim_time('createDate', 'create_date', '创建时间'),
        },
        measures={
            'expectedRevenue': measure_sum('expectedRevenue', 'expected_revenue', '预期收入'),
            'proratedRevenue': measure_sum('proratedRevenue', 'prorated_revenue', '加权收入'),
            'probability': measure_avg('probability', 'probability', '成功概率'),
            'dayOpen': measure_avg('dayOpen', 'day_open', '分配天数'),
            'dayClose': measure_avg('dayClose', 'day_close', '关闭天数'),
            'leadCount': measure_count('leadCount', 'id', '线索数'),
        },
        dimension_joins=[
            fk_join('stage', 'crm_stage', 'stage_id', caption='阶段'),
            fk_join('partner', 'res_partner', 'partner_id', caption='客户',
                    properties=[
                        prop('email', caption='邮箱'),
                        prop('phone', caption='电话'),
                        prop('city', caption='城市'),
                        prop('is_company', name='isCompany', caption='是否公司', data_type='BOOLEAN'),
                    ]),
            fk_join('salesperson', 'res_users', 'user_id', caption='销售员',
                    caption_column='login'),
            fk_join('salesTeam', 'crm_team', 'team_id', caption='销售团队'),
            fk_join('company', 'res_company', 'company_id', caption='公司'),
            fk_join('country', 'res_country', 'country_id', caption='国家',
                    properties=[prop('code', caption='国家代码')]),
            fk_join('lostReason', 'crm_lost_reason', 'lost_reason_id', caption='丢失原因'),
            fk_join('campaign', 'utm_campaign', 'campaign_id', caption='营销活动'),
            fk_join('utmSource', 'utm_source', 'source_id', caption='来源'),
            fk_join('utmMedium', 'utm_medium', 'medium_id', caption='媒介'),
        ],
    )
