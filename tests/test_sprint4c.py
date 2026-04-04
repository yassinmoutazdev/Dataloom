# =============================================================================
# test_sprint4c.py  —  Dataloom v3.0  Sprint 4C
#
# Tests for:
#   4C-1  CTE engine (WITH … AS sub-intents)
#   4C-2  Correlated subqueries
#   4C-3  Remaining patterns: X5 (HAVING EXTRACT), W8 (CASE WHEN dates), W10 (interval filters)
# =============================================================================

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from validator import validate_intent, set_join_paths
from sql_builder import build_sql

SCHEMA = {
    "fact_orders":   ["order_id","customer_id","product_id","employee_id",
                      "unit_price","quantity","order_date","ship_date",
                      "status","region","freight","trip_ts","signup_ts"],
    "dim_customers": ["customer_id","name","city","country","email",
                      "signup_date","age"],
    "dim_products":  ["product_id","product_name","category","unit_price",
                      "category_id"],
    "dim_employees": ["employee_id","name","region","department"],
    "dim_categories":["category_id","category_name"],
    "spend_summary": ["customer_id","total_spend","last_order_date"],
}

@pytest.fixture(autouse=True)
def setup_joins():
    set_join_paths({
        "fact_orders":   {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products":  "fact_orders.product_id = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers": {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
        "dim_products":  {"fact_orders": "fact_orders.product_id = dim_products.product_id",
                          "dim_categories": "dim_products.category_id = dim_categories.category_id"},
        "dim_employees": {"fact_orders": "fact_orders.employee_id = dim_employees.employee_id"},
        "dim_categories":{"dim_products": "dim_products.category_id = dim_categories.category_id"},
        "spend_summary": {"dim_customers": "spend_summary.customer_id = dim_customers.customer_id"},
    })


def val(intent):
    ok, errs = validate_intent(intent, SCHEMA)
    return ok, errs


def sql(intent, db="postgresql"):
    ok, errs = validate_intent(intent, SCHEMA)
    assert ok, f"Validation failed: {errs}"
    return build_sql(intent, db)[0]


# =============================================================================
# 4C-1  CTE Engine
# =============================================================================

class TestCTEEngine:

    def test_single_cte_with_block(self):
        q = sql({
            "metrics": [{"metric":"customer_count","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "spend_summary", "group_by": [],
            "ctes": [{"name": "spend_summary", "intent": {
                "metrics": [{"metric":"total_spend","aggregation":"SUM","target_column":"unit_price"}],
                "fact_table": "fact_orders",
                "group_by": ["fact_orders.customer_id"],
            }}],
        })
        assert "WITH spend_summary AS" in q
        assert "SUM(fact_orders.unit_price)" in q

    def test_cte_limit_stripped_from_sub_intent(self):
        q = sql({
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "spend_summary", "group_by": [],
            "ctes": [{"name": "spend_summary", "intent": {
                "metrics": [{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
                "fact_table": "fact_orders",
                "group_by": ["fact_orders.customer_id"],
                "limit": 10,
            }}],
        })
        # LIMIT must not appear inside the CTE definition
        cte_block = q.split("SELECT")[1] if "WITH" in q else ""
        assert "WITH spend_summary AS" in q
        lines_before_main = q.split("SELECT cnt")[0] if "SELECT cnt" in q else q
        assert "LIMIT" not in lines_before_main or q.count("LIMIT") == 1

    def test_two_ctes_comma_separated(self):
        q = sql({
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "spend_summary", "group_by": [],
            "ctes": [
                {"name": "spend_summary", "intent": {
                    "metrics": [{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
                    "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                }},
                {"name": "top_customers", "intent": {
                    "metrics": [{"metric":"orders","aggregation":"COUNT","target_column":"order_id"}],
                    "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                }},
            ],
        })
        assert "spend_summary AS" in q
        assert "top_customers AS" in q

    def test_duplicate_cte_name_rejected(self):
        ok, errs = val({
            "metrics": [{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders", "group_by": [],
            "ctes": [
                {"name": "dup", "intent": {"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],"fact_table":"fact_orders","group_by":[]}},
                {"name": "dup", "intent": {"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],"fact_table":"fact_orders","group_by":[]}},
            ],
        })
        assert not ok
        assert any("duplicate" in e.lower() for e in errs)

    def test_missing_cte_name_rejected(self):
        ok, errs = val({
            "metrics": [{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders", "group_by": [],
            "ctes": [{"intent": {"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],"fact_table":"fact_orders","group_by":[]}}],
        })
        assert not ok
        assert any("name" in e for e in errs)

    def test_missing_sub_intent_rejected(self):
        ok, errs = val({
            "metrics": [{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders", "group_by": [],
            "ctes": [{"name": "broken"}],
        })
        assert not ok
        assert any("sub-intent" in e for e in errs)

    def test_cte_params_come_before_main_params(self):
        intent = {
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "spend_summary", "group_by": [],
            "filters": [{"column":"customer_id","operator":"=","value":"MAIN_PARAM"}],
            "ctes": [{"name": "spend_summary", "intent": {
                "metrics": [{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                "filters": [{"column":"status","operator":"=","value":"CTE_PARAM"}],
            }}],
        }
        ok, errs = validate_intent(intent, SCHEMA)
        assert ok, errs
        q, params = build_sql(intent)
        assert params[0] == "CTE_PARAM"
        assert params[1] == "MAIN_PARAM"


# =============================================================================
# 4C-2  Correlated subqueries
# =============================================================================

class TestCorrelatedFilter:

    def test_price_above_category_average(self):
        q = sql({
            "metrics": [{"metric":"product_count","aggregation":"COUNT","target_column":"product_id"}],
            "fact_table": "dim_products", "group_by": [],
            "correlated_filter": {
                "column": "dim_products.unit_price",
                "operator": ">",
                "subquery": {
                    "aggregation": "AVG", "target_column": "unit_price",
                    "fact_table": "dim_products", "where_col": "category",
                    "outer_ref": "dim_products.category"
                }
            },
        })
        assert "(SELECT AVG(" in q
        assert "WHERE" in q
        assert "dim_products.category" in q

    def test_correlated_filter_uses_fact_table_default(self):
        q = sql({
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"product_id"}],
            "fact_table": "dim_products", "group_by": [],
            "correlated_filter": {
                "column": "dim_products.unit_price",
                "operator": ">=",
                "subquery": {
                    "aggregation": "AVG", "target_column": "unit_price",
                    "where_col": "category", "outer_ref": "dim_products.category"
                }
            },
        })
        assert "dim_products" in q

    def test_missing_column_rejected(self):
        ok, errs = val({
            "metrics": [{"metric":"r","aggregation":"COUNT","target_column":"product_id"}],
            "fact_table": "dim_products", "group_by": [],
            "correlated_filter": {
                "operator": ">",
                "subquery": {"aggregation":"AVG","target_column":"unit_price","where_col":"category","outer_ref":"dim_products.category"}
            },
        })
        assert not ok
        assert any("column" in e for e in errs)

    def test_invalid_operator_rejected(self):
        ok, errs = val({
            "metrics": [{"metric":"r","aggregation":"COUNT","target_column":"product_id"}],
            "fact_table": "dim_products", "group_by": [],
            "correlated_filter": {
                "column": "dim_products.unit_price", "operator": "LIKE",
                "subquery": {"aggregation":"AVG","target_column":"unit_price","where_col":"category","outer_ref":"dim_products.category"}
            },
        })
        assert not ok
        assert any("operator" in e for e in errs)

    def test_missing_outer_ref_rejected(self):
        ok, errs = val({
            "metrics": [{"metric":"r","aggregation":"COUNT","target_column":"product_id"}],
            "fact_table": "dim_products", "group_by": [],
            "correlated_filter": {
                "column": "dim_products.unit_price", "operator": ">",
                "subquery": {"aggregation":"AVG","target_column":"unit_price","where_col":"category"}
            },
        })
        assert not ok
        assert any("outer_ref" in e for e in errs)


# =============================================================================
# 4C-3  Remaining patterns
# =============================================================================

class TestRemainingPatterns:

    # X5 — HAVING COUNT(DISTINCT EXTRACT(...))
    def test_x5_having_count_distinct_extract(self):
        q = sql({
            "metrics": [{"metric":"order_count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": ["fact_orders.customer_id"],
            "having": [{
                "aggregation": "COUNT",
                "target_column": "EXTRACT(month FROM fact_orders.order_date)",
                "distinct": True,
                "operator": "=",
                "value": 12
            }],
        })
        assert "COUNT(DISTINCT EXTRACT" in q
        assert "HAVING" in q

    def test_x5_extract_expression_passes_validator(self):
        ok, errs = val({
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": ["fact_orders.customer_id"],
            "having": [{
                "aggregation": "COUNT",
                "target_column": "EXTRACT(month FROM fact_orders.order_date)",
                "distinct": True, "operator": ">=", "value": 6
            }],
        })
        assert ok, f"X5 EXTRACT expression should pass validator, got: {errs}"

    # W8 — CASE WHEN with date-distance condition
    def test_w8_case_when_date_condition_passes(self):
        ok, errs = val({
            "metrics": [{"metric":"customer_count","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "computed_columns": [{
                "alias": "tenure_group",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                    {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'"},
                ],
                "else_value": "'Low'",
                "include_in_group_by": False,
            }],
        })
        assert ok, f"W8 CASE WHEN should pass validator, got: {errs}"

    def test_w8_case_when_renders_in_select(self):
        q = sql({
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "fact_orders", "group_by": [],
            "computed_columns": [{
                "alias": "tenure_group",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                ],
                "else_value": "'Low'",
                "include_in_group_by": False,
            }],
        })
        assert "CASE" in q
        assert "tenure_group" in q

    # W10 — Date interval filter value
    def test_w10_interval_filter_postgresql(self):
        intent = {
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders", "group_by": [],
            "filters": [{
                "column": "ship_date",
                "operator": "<=",
                "value": "order_date + INTERVAL '7 days'"
            }],
        }
        ok, errs = validate_intent(intent, SCHEMA)
        assert ok, errs
        q, params = build_sql(intent, "postgresql")
        assert any("INTERVAL" in str(p) for p in params) or "INTERVAL" in q

    def test_w10_interval_filter_mysql(self):
        intent = {
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders", "group_by": [],
            "filters": [{"column":"ship_date","operator":"<=","value":"DATE_ADD(order_date, INTERVAL 7 DAY)"}],
        }
        ok, errs = validate_intent(intent, SCHEMA)
        assert ok, errs
        q, params = build_sql(intent, "mysql")
        assert any("DATE_ADD" in str(p) for p in params) or "DATE_ADD" in q
