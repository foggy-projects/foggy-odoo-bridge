"""Regression tests for hierarchy operators inside $or / $and conditions.

Reproduces the bug: KeyError("Unknown operator: 'selfAndAncestorsOf'")
when hierarchy operators are nested inside $or / $and logical groups.

Root cause: _add_filter() was sending hierarchy operators to
_formula_registry.build_condition() instead of the closure-table path.
"""

import sys
import os
import pytest

# Ensure vendored foggy lib is importable
_lib_dir = os.path.join(os.path.dirname(__file__), "..", "foggy_mcp", "lib")
if _lib_dir not in sys.path:
    sys.path.insert(0, _lib_dir)

from foggy.dataset_model.definitions.base import ColumnType
from foggy.dataset_model.impl.model import (
    DbModelDimensionImpl,
    DbModelMeasureImpl,
    DbTableModelImpl,
    DimensionJoinDef,
)
from foggy.dataset_model.semantic.service import SemanticQueryService
from foggy.mcp_spi import SemanticQueryRequest


@pytest.fixture
def company_model() -> DbTableModelImpl:
    """Model mimicking Odoo res_partner with hierarchical company dimension."""
    model = DbTableModelImpl(
        name="PartnerModel",
        source_table="res_partner",
        dimensions={
            "partnerShare": DbModelDimensionImpl(
                name="partnerShare",
                column="partner_share",
                data_type=ColumnType.BOOLEAN,
            ),
            "company": DbModelDimensionImpl(
                name="company",
                column="company_id",
                data_type=ColumnType.INTEGER,
                is_hierarchical=True,
                hierarchy_table="res_company_closure",
                parent_column="parent_id",
                level_column="company_id",
            ),
        },
        measures={
            "partnerCount": DbModelMeasureImpl(
                name="partnerCount",
                column="id",
            )
        },
        dimension_joins=[
            DimensionJoinDef(
                name="company",
                table_name="res_company",
                foreign_key="company_id",
                primary_key="id",
                caption_column="name",
                alias="rc",
            )
        ],
    )
    return model


@pytest.fixture
def service(company_model: DbTableModelImpl) -> SemanticQueryService:
    svc = SemanticQueryService()
    svc.register_model(company_model)
    return svc


def _build(service: SemanticQueryService, request: SemanticQueryRequest):
    result = service._build_query(service.get_model("PartnerModel"), request)
    return result.sql, result.params


def _hier_alias(sql: str) -> str:
    marker = "LEFT JOIN res_company_closure AS "
    start = sql.index(marker) + len(marker)
    end = sql.index(" ON ", start)
    return sql[start:end]


# ---------- Top-level hierarchy operators ----------

class TestTopLevelHierarchyOperators:
    def test_self_and_descendants_of(self, service):
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{"field": "company$id", "op": "selfAndDescendantsOf", "value": 2}],
            ),
        )
        alias = _hier_alias(sql)
        assert "LEFT JOIN res_company_closure" in sql
        assert f't.company_id = {alias}.company_id' in sql
        assert f'{alias}.parent_id = ?' in sql
        assert f'{alias}.distance >= 0' in sql
        assert params == [2]

    def test_self_and_ancestors_of(self, service):
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{"field": "company$id", "op": "selfAndAncestorsOf", "value": 2}],
            ),
        )
        alias = _hier_alias(sql)
        assert "LEFT JOIN res_company_closure" in sql
        assert f't.company_id = {alias}.parent_id' in sql
        assert f'{alias}.company_id = ?' in sql
        assert f'{alias}.distance >= 0' in sql
        assert params == [2]


# ---------- $or with hierarchy operators ----------

class TestOrWithHierarchyOperators:
    def test_or_contains_self_and_ancestors_of(self, service):
        """Regression: selfAndAncestorsOf inside $or should not KeyError."""
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{
                    "$or": [
                        {"field": "partnerShare", "op": "=", "value": False},
                        {"field": "company$id", "op": "selfAndAncestorsOf", "value": 2},
                    ]
                }],
            ),
        )
        alias = _hier_alias(sql)
        assert " OR " in sql
        assert f'{alias}.distance >= 0' in sql
        assert f'{alias}.company_id = ?' in sql
        assert params == [False, 2]

    def test_or_is_null_plus_hierarchy(self, service):
        """$or with 'is null' + hierarchy operator should produce correct SQL."""
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{
                    "$or": [
                        {"field": "company$id", "op": "selfAndAncestorsOf", "value": 2},
                        {"field": "company$id", "op": "is null"},
                    ]
                }],
            ),
        )
        alias = _hier_alias(sql)
        assert " OR " in sql
        assert f't.company_id = {alias}.parent_id' in sql
        assert f'{alias}.company_id = ?' in sql
        assert 'rc.id IS NULL' in sql
        assert params == [2]

    def test_or_with_self_and_descendants_of(self, service):
        """selfAndDescendantsOf inside $or should work."""
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{
                    "$or": [
                        {"field": "partnerShare", "op": "=", "value": False},
                        {"field": "company$id", "op": "selfAndDescendantsOf", "value": 1},
                    ]
                }],
            ),
        )
        alias = _hier_alias(sql)
        assert " OR " in sql
        assert f'{alias}.distance >= 0' in sql
        assert f'{alias}.parent_id = ?' in sql


# ---------- Multi-value list ----------

class TestMultiValueList:
    def test_hierarchy_operator_with_multi_value_list(self, service):
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{"field": "company$id", "op": "selfAndDescendantsOf", "value": [2, 1]}],
            ),
        )
        alias = _hier_alias(sql)
        assert sql.count(f"{alias}.parent_id = ?") == 2
        assert sql.count(f"{alias}.distance >= 0") == 2
        assert " OR " in sql
        assert params == [2, 1]

    def test_or_with_multi_value_hierarchy(self, service):
        """$or containing hierarchy with list values."""
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{
                    "$or": [
                        {"field": "partnerShare", "op": "=", "value": False},
                        {"field": "company$id", "op": "selfAndAncestorsOf", "value": [2, 1]},
                        {"field": "company$id", "op": "is null"},
                    ]
                }],
            ),
        )
        alias = _hier_alias(sql)
        assert " OR " in sql
        assert sql.count(f"{alias}.company_id = ?") == 2
        assert params == [False, 2, 1]


# ---------- $and with hierarchy operators ----------

class TestAndWithHierarchyOperators:
    def test_and_with_hierarchy_operator(self, service):
        """$and with hierarchy operator should work."""
        sql, params = _build(
            service,
            SemanticQueryRequest(
                columns=["partnerCount"],
                slice=[{
                    "$and": [
                        {"field": "partnerShare", "op": "=", "value": False},
                        {"field": "company$id", "op": "selfAndAncestorsOf", "value": 2},
                    ]
                }],
            ),
        )
        alias = _hier_alias(sql)
        assert f'{alias}.company_id = ?' in sql
        assert f'{alias}.distance >= 0' in sql
        assert 't.partner_share = ?' in sql
        assert params == [False, 2]
