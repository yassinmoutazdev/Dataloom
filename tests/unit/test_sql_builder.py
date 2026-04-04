"""
Unit tests for the sql_builder module.

Tests individual SQL generation functions in isolation to ensure they work correctly.
"""
import pytest
from sql_builder import build_sql


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


class TestBuildSqlBasic:
    """Test basic SQL building functionality."""

    def test_build_simple_sum_query(self, sample_schema):
        """Test building a simple SUM query."""
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
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "SUM(fact_orders.unit_price)" in sql
        assert "fact_orders" in sql
        assert "LIMIT 10" in sql
        assert len(params) == 0  # No parameters for this query

    def test_build_count_with_filter(self, sample_schema):
        """Test building a COUNT query with a filter."""
        intent = {
            "metrics": [{"metric":"count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [],
            "filters": [{"column":"status","operator":"=","value":"shipped"}],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "COUNT(*)" in sql
        assert "WHERE" in sql
        assert "fact_orders.status = %s" in sql
        assert len(params) == 1
        assert params[0] == "shipped"

    def test_build_group_by_query(self, sample_schema):
        """Test building a query with GROUP BY."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["region"],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "SUM(fact_orders.unit_price)" in sql
        assert "GROUP BY" in sql
        assert "region" in sql

    def test_build_join_query(self, sample_schema):
        """Test building a query with JOINs."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["dim_products.category"],
            "joins": ["fact_orders.product_id = dim_products.product_id"],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "SUM(fact_orders.unit_price)" in sql
        assert "JOIN" in sql
        assert "dim_products" in sql
        assert "fact_orders.product_id = dim_products.product_id" in sql


class TestBuildSqlAdvanced:
    """Test advanced SQL building functionality."""

    def test_build_order_by_limit_query(self, sample_schema):
        """Test building a query with ORDER BY and LIMIT."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["region"],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": 5,
            "order_by": "revenue",
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "ORDER BY" in sql
        assert "DESC" in sql
        assert "LIMIT 5" in sql

    def test_build_having_query(self, sample_schema):
        """Test building a query with HAVING clause."""
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
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "HAVING" in sql
        assert "COUNT(DISTINCT fact_orders.order_id)" in sql
        assert "> 3" in sql
        assert len(params) == 0

    def test_build_left_join_anti_join_query(self, sample_schema):
        """Test building a LEFT JOIN anti-join query."""
        intent = {
            "metrics": [{"metric":"count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [{"condition":"fact_orders.customer_id = dim_customers.customer_id","type":"LEFT"}],
            "filters": [{"column":"dim_customers.customer_id","operator":"IS NULL"}],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "postgresql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        assert "LEFT JOIN" in sql
        assert "IS NULL" in sql

    def test_build_mysql_dialect_query(self, sample_schema):
        """Test building a query with MySQL dialect."""
        intent = {
            "metrics": [{"metric":"count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "mysql")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        # MySQL-specific syntax might be different, but basic structure should be similar

    def test_build_sqlite_dialect_query(self, sample_schema):
        """Test building a query with SQLite dialect."""
        intent = {
            "metrics": [{"metric":"count","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table": "fact_orders",
            "group_by": [],
            "joins": [],
            "filters": [],
            "computed_columns": [],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [],
            "limit": None,
            "order_by": None,
            "order_dir": "DESC",
        }
        
        sql, params = build_sql(intent, "sqlite")
        
        # Check that the SQL contains expected elements
        assert "SELECT" in sql
        # SQLite-specific syntax might be different, but basic structure should be similar