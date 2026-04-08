# =============================================================================
# test_sprint4b.py  —  Dataloom v3.0  Sprint 4B
#
# Tests for:
#   4B-1  RANK / ROW_NUMBER / DENSE_RANK  (regression against prior impl)
#   4B-2  LAG / LEAD offset functions
#   4B-3  Aggregate OVER with ROWS BETWEEN frame specs
#   4B-4  Scalar subquery (% of total)
#   4B-5  INTERSECT / EXCEPT set operations
# =============================================================================

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from validator import validate_intent, set_join_paths
from sql_builder import build_sql

# ── Shared schema ─────────────────────────────────────────────────────────────

# Five-table schema; dim_categories added to support multi-hop join tests.
SCHEMA = {
    "fact_orders":   ["order_id", "customer_id", "product_id", "employee_id",
                      "unit_price", "quantity", "order_date", "ship_date",
                      "status", "region", "freight"],
    "dim_customers": ["customer_id", "name", "city", "country", "email",
                      "signup_date", "age"],
    "dim_products":  ["product_id", "product_name", "category", "unit_price",
                      "category_id"],
    "dim_employees": ["employee_id", "name", "region", "department"],
    "dim_categories": ["category_id", "category_name"],
}

# Type hints used by dialect-specific paths in the builder.
SCHEMA_TYPES = {
    "fact_orders":   {"unit_price": "numeric", "quantity": "integer", "freight": "numeric"},
    "dim_customers": {"age": "integer"},
}


@pytest.fixture(autouse=True)
def setup_joins():
    """Register join paths for the five-table schema before every test."""
    set_join_paths({
        "fact_orders": {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products":  "fact_orders.product_id = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers": {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
        "dim_products":  {"fact_orders":  "fact_orders.product_id = dim_products.product_id",
                          "dim_categories": "dim_products.category_id = dim_categories.category_id"},
        "dim_employees": {"fact_orders":  "fact_orders.employee_id = dim_employees.employee_id"},
        "dim_categories": {"dim_products": "dim_products.category_id = dim_categories.category_id"},
    })


def val(intent):
    """Validate intent; returns (ok, errors)."""
    ok, errs = validate_intent(intent, SCHEMA, SCHEMA_TYPES)
    return ok, errs


def sql(intent, db="postgresql"):
    """Validate intent and build SQL; asserts validation passes."""
    ok, errs = validate_intent(intent, SCHEMA, SCHEMA_TYPES)
    assert ok, f"Validation failed: {errs}"
    return build_sql(intent, db)[0]


# =============================================================================
# 4B-1  RANK / ROW_NUMBER / DENSE_RANK  (regression)
# =============================================================================

class TestWindowFunctionsRankRegression:

    def test_rank_with_partition_by(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["dim_employees.region", "fact_orders.employee_id"],
            "joins": ["fact_orders.employee_id = dim_employees.employee_id"],
            "window_functions": [{
                "alias": "rev_rank", "function": "RANK",
                "partition_by": ["dim_employees.region"],
                "order_by": "total_revenue", "order_dir": "DESC",
            }],
        })
        assert "RANK() OVER" in q
        assert "PARTITION BY dim_employees.region" in q

    def test_row_number_no_partition(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["fact_orders.customer_id"],
            "window_functions": [{
                "alias": "rn", "function": "ROW_NUMBER",
                "partition_by": [], "order_by": "total_revenue", "order_dir": "DESC",
            }],
        })
        assert "ROW_NUMBER() OVER" in q
        assert "PARTITION BY" not in q

    def test_dense_rank(self):
        q = sql({
            "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["fact_orders.customer_id"],
            "window_functions": [{
                "alias": "dr", "function": "DENSE_RANK",
                "partition_by": [], "order_by": "rev", "order_dir": "ASC",
            }],
        })
        assert "DENSE_RANK() OVER" in q

    def test_order_by_resolves_metric_alias_to_expression(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["fact_orders.customer_id"],
            "window_functions": [{
                "alias": "rk", "function": "RANK",
                "partition_by": [], "order_by": "total_revenue", "order_dir": "DESC",
            }],
        })
        # The window ORDER BY resolves the metric alias to its full aggregate expression.
        assert "SUM(fact_orders.unit_price)" in q

    def test_missing_alias_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{"function": "RANK", "partition_by": [], "order_by": "r", "order_dir": "DESC"}],
        })
        assert not ok
        assert any("missing alias" in e for e in errs)

    def test_unknown_function_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{"alias": "x", "function": "MEDIAN",
                                  "partition_by": [], "order_by": "r", "order_dir": "DESC"}],
        })
        assert not ok
        assert any("unknown function" in e for e in errs)


# =============================================================================
# 4B-2  LAG / LEAD
# =============================================================================

class TestLagLead:

    def test_lag_three_arg_form(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "prev_rev", "function": "LAG",
                "target_column": "total_revenue", "offset": 1, "default": 0,
                "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
            }],
        })
        assert "LAG(" in q
        assert ", 1, 0)" in q

    def test_lead_three_arg_form(self):
        q = sql({
            "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "next_rev", "function": "LEAD",
                "target_column": "rev", "offset": 1, "default": "NULL",
                "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
            }],
        })
        assert "LEAD(" in q

    def test_lag_target_resolves_to_metric_expression(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "prev", "function": "LAG",
                "target_column": "total_revenue", "offset": 1, "default": 0,
                "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
            }],
        })
        # LAG target resolves to the full aggregate expression, not the alias string.
        assert "SUM(fact_orders.unit_price)" in q

    def test_lag_with_partition_by(self):
        q = sql({
            "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["dim_employees.region"],
            "joins": ["fact_orders.employee_id = dim_employees.employee_id"],
            "window_functions": [{
                "alias": "prev_region_rev", "function": "LAG",
                "target_column": "rev", "offset": 1, "default": 0,
                "partition_by": ["dim_employees.region"],
                "order_by": "order_date", "order_dir": "ASC",
            }],
        })
        assert "PARTITION BY dim_employees.region" in q

    def test_lag_asc_order_direction(self):
        q = sql({
            "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "prev", "function": "LAG",
                "target_column": "rev", "offset": 1, "default": 0,
                "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
            }],
        })
        assert "ORDER BY" in q
        assert "ASC" in q


# =============================================================================
# 4B-3  Aggregate OVER with frame specs
# =============================================================================

class TestAggregateOver:

    def test_sum_over_unbounded_running_total(self):
        q = sql({
            "metrics": [{"metric": "monthly_rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "running_total", "function": "SUM",
                "target_column": "monthly_rev", "partition_by": [],
                "order_by": "order_date", "order_dir": "ASC",
                "frame_spec": "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            }],
        })
        assert "SUM(" in q
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in q

    def test_avg_over_rolling_3_row_window(self):
        q = sql({
            "metrics": [{"metric": "monthly_rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "rolling_3m", "function": "AVG",
                "target_column": "monthly_rev", "partition_by": [],
                "order_by": "order_date", "order_dir": "ASC",
                "frame_spec": "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
            }],
        })
        assert "AVG(" in q
        assert "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" in q

    def test_sum_over_with_partition_by(self):
        q = sql({
            "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["dim_products.category"],
            "joins": ["fact_orders.product_id = dim_products.product_id"],
            "window_functions": [{
                "alias": "cumulative_cat", "function": "SUM",
                "target_column": "rev",
                "partition_by": ["dim_products.category"],
                "order_by": "order_date", "order_dir": "ASC",
                "frame_spec": "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            }],
        })
        assert "PARTITION BY dim_products.category" in q

    def test_invalid_frame_spec_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "x", "function": "SUM", "target_column": "unit_price",
                "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
                "frame_spec": "INVALID FRAME SPEC",
            }],
        })
        assert not ok
        assert any("frame_spec" in e for e in errs)

    def test_range_between_frame_spec_accepted(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "window_functions": [{
                "alias": "x", "function": "SUM", "target_column": "unit_price",
                "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
                "frame_spec": "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            }],
        })
        assert ok, errs


# =============================================================================
# 4B-4  Scalar subquery
# =============================================================================

class TestScalarSubquery:

    def test_percentage_of_total(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["dim_products.category"],
            "joins": ["fact_orders.product_id = dim_products.product_id"],
            "scalar_subquery": {
                "alias": "pct_total",
                "numerator_metric": "total_revenue",
                "multiply_by": 100,
                "denominator": {"aggregation": "SUM", "target_column": "unit_price", "fact_table": "fact_orders"},
            },
        })
        assert "NULLIF(" in q
        assert "pct_total" in q
        assert "100.0" in q

    def test_raw_ratio_no_multiply(self):
        q = sql({
            "metrics": [{"metric": "total_revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "scalar_subquery": {
                "alias": "revenue_share",
                "numerator_metric": "total_revenue",
                "multiply_by": 1,
                "denominator": {"aggregation": "SUM", "target_column": "unit_price", "fact_table": "fact_orders"},
            },
        })
        assert "revenue_share" in q
        assert "100.0" not in q

    def test_missing_alias_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "scalar_subquery": {
                "numerator_metric": "r", "multiply_by": 100,
                "denominator": {"aggregation": "SUM", "target_column": "unit_price"},
            },
        })
        assert not ok
        assert any("scalar_subquery" in e and "alias" in e for e in errs)

    def test_missing_denominator_aggregation_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "scalar_subquery": {
                "alias": "pct", "numerator_metric": "r", "multiply_by": 100,
                "denominator": {"target_column": "unit_price"},
            },
        })
        assert not ok
        assert any("aggregation" in e for e in errs)


# =============================================================================
# 4B-5  INTERSECT / EXCEPT
# =============================================================================

class TestSetOperations:

    def test_intersect_two_sub_intents(self):
        q = sql({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "set_operation": {
                "operator": "INTERSECT",
                "left":  {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders",
                          "filters": [{"column": "status", "operator": "=", "value": "shipped"}],
                          "group_by": []},
                "right": {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders",
                          "filters": [{"column": "region", "operator": "=", "value": "US"}],
                          "group_by": []},
            },
        })
        assert "INTERSECT" in q

    def test_except_two_sub_intents(self):
        q = sql({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "set_operation": {
                "operator": "EXCEPT",
                "left":  {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders", "group_by": []},
                "right": {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders",
                          "filters": [{"column": "status", "operator": "=", "value": "cancelled"}],
                          "group_by": []},
            },
        })
        assert "EXCEPT" in q

    def test_limit_stripped_from_sub_intents_before_set_op(self):
        q = sql({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "set_operation": {
                "operator": "INTERSECT",
                "left":  {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders", "group_by": [], "limit": 10},
                "right": {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders", "group_by": [], "limit": 10},
            },
        })
        # LIMIT inside a set-operation operand would produce invalid SQL.
        parts = q.split("INTERSECT")
        assert "LIMIT" not in parts[0]

    def test_invalid_operator_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "set_operation": {
                "operator": "MINUS",
                "left":  {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders", "group_by": []},
                "right": {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders", "group_by": []},
            },
        })
        assert not ok
        assert any("INTERSECT" in e or "operator" in e for e in errs)

    def test_missing_left_sub_intent_rejected(self):
        ok, errs = val({
            "metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders", "group_by": [],
            "set_operation": {
                "operator": "INTERSECT",
                "right": {"metrics": [{"metric": "cid", "aggregation": "COUNT", "target_column": "customer_id"}],
                          "fact_table": "fact_orders", "group_by": []},
            },
        })
        assert not ok
