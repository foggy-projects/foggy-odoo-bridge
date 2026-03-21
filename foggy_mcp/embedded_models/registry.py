# -*- coding: utf-8 -*-
"""
Odoo 模型注册中心

将所有 Odoo TM 模型注册到 SemanticQueryService。
"""
import logging

_logger = logging.getLogger(__name__)

# 数据源名称常量（与 Java 侧 ODOO_DATA_SOURCE_NAME 一致）
ODOO_DATA_SOURCE_NAME = 'odoo'


def register_all_odoo_models(service):
    """将所有 Odoo 模型注册到语义查询服务。

    Args:
        service: SemanticQueryService 实例
    """
    from .sale_order import create_sale_order_model, create_sale_order_line_model
    from .purchase_order import create_purchase_order_model
    from .account_move import create_account_move_model
    from .stock_picking import create_stock_picking_model
    from .hr_employee import create_hr_employee_model
    from .res_partner import create_res_partner_model
    from .res_company import create_res_company_model
    from .crm_lead import create_crm_lead_model

    builders = [
        create_sale_order_model,
        create_sale_order_line_model,
        create_purchase_order_model,
        create_account_move_model,
        create_stock_picking_model,
        create_hr_employee_model,
        create_res_partner_model,
        create_res_company_model,
        create_crm_lead_model,
    ]

    count = 0
    for builder in builders:
        try:
            model = builder()
            service.register_model(model)
            count += 1
            _logger.debug("已注册 Odoo 模型：%s", model.name)
        except Exception as e:
            _logger.warning("注册模型失败（%s）：%s", builder.__name__, e)

    _logger.info("已注册 %d/%d 个 Odoo 模型", count, len(builders))
    return count
