"""
test_validator_basic.py  —  Dataloom v3.0
Unit tests for the validator module.

Covers the exported constant sets (used by callers to build valid intents)
and the core ``validate_intent`` function in both its minimal and advanced
forms. All tests run in isolation against a fixed in-memory schema.

Test classes:
    TestValidatorConstants    — completeness checks on exported constant sets
    TestValidateIntentBasic   — happy-path and rejection for required fields
    TestValidateIntentAdvanced — join types, HAVING, and operator validation
"""

import pytest
from validator import (
    validate_intent,
    set_join_paths,
    VALID_AGGREGATIONS,
    VALID_OPERATORS,
    VALID_HAVING_OPS,
    VALID_ORDER_DIRS,
    VALID_TIME_RANGES,
    VALID_TIME_BUCKETS,
    VALID_JOIN_TYPES,
    VALID_DATE_OPS,
    VALID_WINDOW_FUNCTIONS,
    VALID_FRAME_UNITS,
)


@pytest.fixture(scope="function")
def sample_schema():
    """Four-table schema used throughout this file."""
    return {
        "fact_orders": [
            "order_id", "customer_id", "product_id", "employee_id",
            "order_date", "ship_date", "quantity", "unit_price",
            "freight", "status", "region",
        ],
        "dim_customers": ["customer_id", "name", "email", "city", "country", "age"],
        "dim_products":  ["product_id", "product_name", "category", "price"],
        "dim_employees": ["employee_id", "name", "department", "region"],
    }


@pytest.fixture(scope="function")
def sample_join_paths():
    """Join conditions for the four-table schema."""
    return {
        "fact_orders": {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products":  "fact_orders.product_id = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers": {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
        "dim_products":  {"fact_orders": "fact_orders.product_id = dim_products.product_id"},
        "dim_employees": {"fact_orders": "fact_orders.employee_id = dim_employees.employee_id"},
    }


@pytest.fixture(autouse=True)
def setup_test_environment(sample_join_paths):
    """Apply join paths before each test and clear them on teardown."""
    set_join_paths(sample_join_paths)
    yield
    set_join_paths({})


# =============================================================================
# Constant sets
# =============================================================================

class TestValidatorConstants:

    def test_valid_aggregations_are_complete(self):
        expected = {"SUM", "COUNT", "AVG", "MAX", "MIN", "NTILE", "PERCENTILE_CONT"}
        assert expected.issubset(VALID_AGGREGATIONS)

    def test_valid_operators_are_complete(self):
        expected = {"=", ">", "<", ">=", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL"}
        assert expected.issubset(VALID_OPERATORS)

    def test_valid_having_ops_are_complete(self):
        expected = {"=", ">", "<", ">=", "<="}
        assert expected.issubset(VALID_HAVING_OPS)

    def test_valid_order_dirs_are_complete(self):
        expected = {"DESC", "ASC"}
        assert expected.issubset(VALID_ORDER_DIRS)

    def test_valid_join_types_are_complete(self):
        expected = {"INNER", "LEFT", "RIGHT", "FULL"}
        assert expected.issubset(VALID_JOIN_TYPES)

    def test_valid_window_functions_are_complete(self):
        """Sprint 4B window functions must all be registered."""
        expected = {
            "RANK", "ROW_NUMBER", "DENSE_RANK",
            "LAG", "LEAD",
            "SUM", "AVG", "COUNT", "MAX", "MIN",
        }
        assert expected.issubset(VALID_WINDOW_FUNCTIONS)


# =============================================================================
# Basic intent validation
# =============================================================================

class TestValidateIntentBasic:

    def test_minimal_valid_intent_passes(self, sample_schema):
        intent = {
            "metrics": [{"metric": "revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": [], "joins": [], "filters": [], "computed_columns": [],
            "time_filter": None, "time_bucket": None, "time_bucket_column": None,
            "having": [], "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert ok, f"Validation failed: {errors}"
        assert len(errors) == 0

    def test_missing_required_fields_rejected(self, sample_schema):
        ok, errors = validate_intent({}, sample_schema)
        assert not ok
        assert len(errors) > 0

    def test_invalid_fact_table_rejected(self, sample_schema):
        intent = {
            "metrics": [{"metric": "revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "nonexistent_table",
            "group_by": [], "joins": [], "filters": [], "computed_columns": [],
            "time_filter": None, "time_bucket": None, "time_bucket_column": None,
            "having": [], "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok
        assert len(errors) > 0

    def test_invalid_metric_aggregation_rejected(self, sample_schema):
        intent = {
            "metrics": [{"metric": "revenue", "aggregation": "INVALID", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": [], "joins": [], "filters": [], "computed_columns": [],
            "time_filter": None, "time_bucket": None, "time_bucket_column": None,
            "having": [], "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok
        assert len(errors) > 0

    def test_invalid_filter_operator_rejected(self, sample_schema):
        intent = {
            "metrics": [{"metric": "revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": [], "joins": [],
            "filters": [{"column": "status", "operator": "INVALID", "value": "shipped"}],
            "computed_columns": [], "time_filter": None, "time_bucket": None,
            "time_bucket_column": None, "having": [], "limit": 10,
            "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok
        assert len(errors) > 0


# =============================================================================
# Advanced intent validation
# =============================================================================

class TestValidateIntentAdvanced:

    def test_valid_left_join_type_passes(self, sample_schema):
        intent = {
            "metrics": [{"metric": "count", "aggregation": "COUNT", "target_column": "order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [{"type": "LEFT", "condition": "fact_orders.customer_id = dim_customers.customer_id"}],
            "filters": [], "computed_columns": [], "time_filter": None,
            "time_bucket": None, "time_bucket_column": None,
            "having": [], "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert ok, f"Validation failed: {errors}"
        assert len(errors) == 0

    def test_invalid_join_type_is_rejected(self, sample_schema):
        """Invalid join types must be explicitly rejected by the validator.

        The validator pre-checks join type before normalize_joins() runs,
        so an unknown type like "INVALID" produces a validation error.
        This is the fail-fast behaviour introduced in Issue 4.
        """
        intent = {
            "metrics": [{"metric": "count", "aggregation": "COUNT", "target_column": "order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [{"type": "INVALID", "condition": "fact_orders.customer_id = dim_customers.customer_id"}],
            "filters": [], "computed_columns": [], "time_filter": None,
            "time_bucket": None, "time_bucket_column": None,
            "having": [], "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail for unknown join type"
        assert any("join type" in e.lower() or "invalid" in e.lower() for e in errors), (
            f"Expected a join-type error message, got: {errors}"
        )

    def test_valid_having_form_a_passes(self, sample_schema):
        intent = {
            "metrics": [{"metric": "revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["customer_id"],
            "joins": [], "filters": [], "computed_columns": [], "time_filter": None,
            "time_bucket": None, "time_bucket_column": None,
            "having": [{"aggregation": "COUNT", "target_column": "order_id",
                        "distinct": True, "operator": ">", "value": 3}],
            "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert ok, f"Validation failed: {errors}"
        assert len(errors) == 0

    def test_invalid_having_operator_rejected(self, sample_schema):
        intent = {
            "metrics": [{"metric": "revenue", "aggregation": "SUM", "target_column": "unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["customer_id"],
            "joins": [], "filters": [], "computed_columns": [], "time_filter": None,
            "time_bucket": None, "time_bucket_column": None,
            "having": [{"aggregation": "COUNT", "target_column": "order_id",
                        "distinct": True, "operator": "INVALID", "value": 3}],
            "limit": 10, "order_by": None, "order_dir": "DESC",
        }
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok
        assert len(errors) > 0
