# -*- coding: utf-8 -*-
"""
Odoo 内嵌引擎模型定义

将 Java 侧 9 个 Odoo TM 模型移植为 Python DbTableModelImpl，
供 EmbeddedBackend 直接在 Odoo 进程内使用。

注意：这些模型仅在内嵌模式下加载，网关模式使用 Java 侧的 TM/QM 文件。
"""
from .registry import register_all_odoo_models  # noqa: F401
