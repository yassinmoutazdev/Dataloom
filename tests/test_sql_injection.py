"""
Security tests for SQL injection prevention.

Tests for vulnerabilities identified in the Master Review Prompt:
- GAP-1: else_value validation to block subqueries and DDL
- GAP-3: then value validation to block subqueries and DDL  
- GAP-4: is_expression keyword blocking
"""
import copy
import pytest
from validator import validate_intent
from sql_builder import build_sql


@pytest.fixture(scope="module")
def schema_map():
    """Sample schema map for security testing."""
    return {
        "fact_orders": [
            "order_id","customer_id","product_id","employee_id",
            "order_date","ship_date","quantity","unit_price","freight","status","region",
        ],
        "dim_customers": ["customer_id","name","email","city","country","age","signup_date"],
        "dim_products":  ["product_id","product_name","category","subcategory","cost","price","stock_level"],
        "dim_categories":["category_id","category_name","dept_id"],
        "dim_employees": ["employee_id","name","department","region"],
    }


@pytest.fixture(scope="module")
def schema_types():
    """Sample schema types for security testing."""
    return {
        "fact_orders": {
            "unit_price":"numeric","freight":"numeric","quantity":"integer",
            "order_date":"timestamp","ship_date":"timestamp",
        },
        "dim_products": {"price":"numeric","cost":"numeric","stock_level":"integer"},
    }


def base_intent(**overrides):
    """Create a base intent with common defaults."""
    intent = {
        "metrics":          [{"metric":"total_revenue","aggregation":"SUM",
                              "target_column":"unit_price","distinct":False}],
        "fact_table":       "fact_orders",
        "group_by":         [],
        "joins":            [],
        "filters":          [],
        "computed_columns": [],
        "time_filter":      None,
        "time_bucket":      None,
        "time_bucket_column":None,
        "having":           [],
        "limit":            10,
        "order_by":         None,
        "order_dir":        "DESC",
    }
    intent.update(overrides)
    return intent


class TestElseValueValidation:
    """GAP-1: else_value content validation."""

    def test_subquery_in_else_value_is_rejected(self, schema_map, schema_types):
        """GAP-1-A: Subquery in else_value is rejected."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
            "else_value": "(SELECT password FROM users LIMIT 1)",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_ddl_keyword_in_else_value_is_rejected(self, schema_map, schema_types):
        """GAP-1-B: DDL keyword in else_value is rejected."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
            "else_value": "'; DROP TABLE orders; --",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_valid_quoted_string_in_else_value_passes(self, schema_map, schema_types):
        """GAP-1-C: Valid quoted string in else_value passes."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
            "else_value": "'Low'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "ELSE 'Low'" in sql

    def test_numeric_else_value_passes(self, schema_map, schema_types):
        """GAP-1-D: Numeric else_value passes."""
        intent = base_intent(computed_columns=[{
            "alias": "score",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "1"}],
            "else_value": "0",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "ELSE 0" in sql

    def test_null_else_value_passes(self, schema_map, schema_types):
        """GAP-1-E: NULL else_value passes."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
            "else_value": "NULL",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "ELSE NULL" in sql


class TestThenValueValidation:
    """GAP-3: CASE WHEN then content validation."""

    def test_subquery_in_then_is_rejected(self, schema_map, schema_types):
        """GAP-3-A: Subquery in then is rejected."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "SUM(fact_orders.unit_price) > 1000",
                "then": "(SELECT api_key FROM config)",
            }],
            "else_value": "'Low'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_ddl_keyword_in_then_is_rejected(self, schema_map, schema_types):
        """GAP-3-B: DDL keyword in then is rejected."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "1=1",
                "then": "'; DROP TABLE users; --",
            }],
            "else_value": "'Other'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_bare_column_name_in_then_is_rejected(self, schema_map, schema_types):
        """GAP-3-C: Bare column name in then is rejected (not a literal)."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "1=1",
                "then": "email",   # not a quoted string or number
            }],
            "else_value": "'Other'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_valid_quoted_string_in_then_passes(self, schema_map, schema_types):
        """GAP-3-D: Valid quoted string in then passes."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [
                {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'Premium'"},
                {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Standard'"},
            ],
            "else_value": "'Basic'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "THEN 'Premium'" in sql
        assert "THEN 'Standard'" in sql

    def test_numeric_then_passes(self, schema_map, schema_types):
        """GAP-3-E: Numeric then passes."""
        intent = base_intent(computed_columns=[{
            "alias": "rank_score",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "3"}],
            "else_value": "1",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "THEN 3" in sql


class TestConditionKeywordBlocking:
    """GAP-4: is_expression / condition keyword blocking."""

    def test_select_in_case_when_condition_is_blocked(self, schema_map, schema_types):
        """GAP-4-A: SELECT in CASE WHEN condition is blocked."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "EXISTS (SELECT 1 FROM pg_tables)",
                "then": "'High'",
            }],
            "else_value": "'Low'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_drop_in_case_when_condition_is_blocked(self, schema_map, schema_types):
        """GAP-4-B: DROP in CASE WHEN condition is blocked."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "1=1; DROP TABLE orders",
                "then": "'High'",
            }],
            "else_value": "'Low'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_sql_comment_in_condition_is_blocked(self, schema_map, schema_types):
        """GAP-4-C: SQL comment (--) in condition is blocked."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "1=1 -- bypass",
                "then": "'High'",
            }],
            "else_value": "'Low'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_valid_aggregation_condition_passes(self, schema_map, schema_types):
        """GAP-4-D: Valid aggregation condition passes."""
        intent = base_intent(computed_columns=[{
            "alias": "tier",
            "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"}],
            "else_value": "'Low'",
        }])
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "CASE" in sql
        assert "WHEN" in sql