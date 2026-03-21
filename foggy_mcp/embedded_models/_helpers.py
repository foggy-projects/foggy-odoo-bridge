# -*- coding: utf-8 -*-
"""
模型定义辅助工具

提供常用的维度/度量/JOIN 构造快捷方式，减少 Odoo 模型定义的样板代码。
"""
from foggy.dataset_model.impl.model import (
    DbTableModelImpl,
    DbModelDimensionImpl,
    DbModelMeasureImpl,
    DimensionJoinDef,
    DimensionPropertyDef,
)
from foggy.dataset_model.definitions.base import (
    DimensionType,
    AggregationType,
    ColumnType,
)

from .registry import ODOO_DATA_SOURCE_NAME


def odoo_model(name, table, alias=None, description=None, dimensions=None,
               measures=None, dimension_joins=None):
    """创建 Odoo 模型的快捷方式。"""
    return DbTableModelImpl(
        name=name,
        alias=alias or name,
        description=description,
        source_table=table,
        source_datasource=ODOO_DATA_SOURCE_NAME,
        primary_key=['id'],
        dimensions=dimensions or {},
        measures=measures or {},
        dimension_joins=dimension_joins or [],
    )


def dim(name, column, alias=None, dim_type=DimensionType.REGULAR,
        data_type=ColumnType.INTEGER):
    """创建维度的快捷方式。"""
    return DbModelDimensionImpl(
        name=name,
        column=column,
        alias=alias or name,
        dimension_type=dim_type,
        data_type=data_type,
    )


def dim_time(name, column, alias=None):
    """创建时间维度。"""
    return dim(name, column, alias, dim_type=DimensionType.TIME,
               data_type=ColumnType.DATETIME)


def dim_day(name, column, alias=None):
    """创建日期维度。"""
    return dim(name, column, alias, dim_type=DimensionType.TIME,
               data_type=ColumnType.DATE)


def measure_sum(name, column, alias=None):
    """创建 SUM 度量。"""
    return DbModelMeasureImpl(
        name=name, column=column, alias=alias or name,
        aggregation=AggregationType.SUM,
    )


def measure_count(name, column='id', alias=None):
    """创建 COUNT DISTINCT 度量。"""
    return DbModelMeasureImpl(
        name=name, column=column, alias=alias or name,
        aggregation=AggregationType.COUNT_DISTINCT,
    )


def measure_avg(name, column, alias=None):
    """创建 AVG 度量。"""
    return DbModelMeasureImpl(
        name=name, column=column, alias=alias or name,
        aggregation=AggregationType.AVG,
    )


def fk_join(name, table, fk, caption_column='name', caption=None, pk='id',
            properties=None):
    """创建 FK JOIN（星型模式维度表关联）。"""
    return DimensionJoinDef(
        name=name,
        table_name=table,
        foreign_key=fk,
        primary_key=pk,
        caption_column=caption_column,
        caption=caption or name,
        properties=properties or [],
    )


def prop(column, name=None, caption=None, data_type='STRING'):
    """创建维度属性。"""
    return DimensionPropertyDef(
        column=column,
        name=name,
        caption=caption,
        data_type=data_type,
    )
