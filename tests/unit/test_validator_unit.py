"""
Unit tests for the validator module.

Tests individual validation functions in isolation to ensure they work correctly.
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
    VALID_FRAME_UNITS
)


@pytest.fixture(scope="function")
def sample_schema():
    """Sample schema for unit testing."""
    return {
        "fact_orders": [
            "order_id", "customer_id", "product_id", "employee_id",
            "order_date", "ship_date", "quantity", "unit_price", 
            "freight", "status", "region"
        ],
        "dim_customers": [
            "customer_id", "name", "email", "city", "country", "age"
        ],
        "dim_products": [
            "product_id", "product_name", "category", "price"
        ],
        "dim_employees": [
            "employee_id", "name", "department", "region"
        ]
    }


@pytest.fixture(scope="function")
def sample_join_paths():
    """Sample join paths for unit testing."""
    return {
        "fact_orders": {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products": "fact_orders.product_id = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers": {
            "fact_orders": "fact_orders.customer_id = dim_customers.customer_id"
        },
        "dim_products": {
            "fact_orders": "fact_orders.product_id = dim_products.product_id"
        },
        "dim_employees": {
            "fact_orders": "fact_orders.employee_id = dim_employees.employee_id"
        }
    }


@pytest.fixture(autouse=True)
def setup_test_environment(sample_join_paths):
    """Set up the test environment with join paths."""
    set_join_paths(sample_join_paths)
    yield
    # Reset join paths after each test
    set_join_paths({})


class TestValidatorConstants:
    """Test validation constants."""

    def test_valid_aggregations(self):
        """Test that valid aggregations include expected values."""
        expected = {"SUM", "COUNT", "AVG", "MAX", "MIN", "NTILE", "PERCENTILE_CONT"}
        assert expected.issubset(VALID_AGGREGATIONS)

    def test_valid_operators(self):
        """Test that valid operators include expected values."""
        expected = {"=", ">", "<", ">=", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL"}
        assert expected.issubset(VALID_OPERATORS)

    def test_valid_having_ops(self):
        """Test that valid HAVING operators include expected values."""
        expected = {"=", ">", "<", ">=", "<="}
        assert expected.issubset(VALID_HAVING_OPS)

    def test_valid_order_dirs(self):
        """Test that valid order directions include expected values."""
        expected = {"DESC", "ASC"}
        assert expected.issubset(VALID_ORDER_DIRS)

    def test_valid_join_types(self):
        """Test that valid join types include expected values."""
        expected = {"INNER", "LEFT", "RIGHT", "FULL"}
        assert expected.issubset(VALID_JOIN_TYPES)

    def test_valid_window_functions(self):
        """Test that valid window functions include expected values."""
        expected = {
            "RANK", "ROW_NUMBER", "DENSE_RANK",
            "LAG", "LEAD",
            "SUM", "AVG", "COUNT", "MAX", "MIN",
        }
        assert expected.issubset(VALID_WINDOW_FUNCTIONS)


class TestValidateIntentBasic:
    """Test basic intent validation."""

    def test_minimal_valid_intent(self, sample_schema):
        """Test that a minimal valid intent passes validation."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert ok, f"Validation failed: {errors}"
        assert len(errors) == 0

    def test_missing_required_fields(self, sample_schema):
        """Test that missing required fields are caught."""
        intent = {
            # Missing required fields like metrics, fact_table
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail"
        assert len(errors) > 0

    def test_invalid_fact_table(self, sample_schema):
        """Test that invalid fact table is caught."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "nonexistent_table",  # Invalid table
            "group_by": [],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail"
        assert len(errors) > 0

    def test_invalid_metric_aggregation(self, sample_schema):
        """Test that invalid metric aggregation is caught."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"INVALID","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail"
        assert len(errors) > 0

    def test_invalid_filter_operator(self, sample_schema):
        """Test that invalid filter operator is caught."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [],
            "filters": [{"column":"status","operator":"INVALID","value":"shipped"}],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail"
        assert len(errors) > 0


class TestValidateIntentAdvanced:
    """Test advanced intent validation."""

    def test_valid_join_type(self, sample_schema):
        """Test that valid join types pass validation."""
        intent = {
            "metrics": [{"metric":"count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [{"type":"LEFT","condition":"fact_orders.customer_id = dim_customers.customer_id"}],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert ok, f"Validation failed: {errors}"
        assert len(errors) == 0

    def test_invalid_join_type(self, sample_schema):
        """Test that invalid join types are caught."""
        intent = {
            "metrics": [{"metric":"count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [{"type":"INVALID","condition":"fact_orders.customer_id = dim_customers.customer_id"}],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail"
        assert len(errors) > 0

    def test_valid_having_clause(self, sample_schema):
        """Test that valid HAVING clauses pass validation."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["customer_id"],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [{"aggregation":"COUNT","target_column":"order_id","distinct":True,"operator":">","value":3}],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert ok, f"Validation failed: {errors}"
        assert len(errors) == 0

    def test_invalid_having_operator(self, sample_schema):
        """Test that invalid HAVING operators are caught."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["customer_id"],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [{"aggregation":"COUNT","target_column":"order_id","distinct":True,"operator":"INVALID","value":3}],
            "limit": 10,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        ok, errors = validate_intent(intent, sample_schema)
        assert not ok, "Expected validation to fail"
        assert len(errors) > 0