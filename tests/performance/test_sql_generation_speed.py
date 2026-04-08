"""
Dataloom SQL generation performance tests.

Guards against latency regressions by benchmarking build_sql across four
representative intent shapes: simple aggregate, multi-join complex query,
window function, and CTE. Uses pytest-benchmark so results appear in CI
artifacts alongside pass/fail status.

Public functions: none (all entry points are pytest benchmark fixtures)
"""
import pytest
from validator import validate_intent, set_join_paths
from sql_builder import build_sql


@pytest.fixture(scope="module")
def schema():
    """Shared schema for performance testing."""
    return {
        "fact_orders":    ["order_id","customer_id","product_id","employee_id",
                           "unit_price","quantity","order_date","ship_date",
                           "status","region","freight","trip_ts"],
        "dim_customers":  ["customer_id","name","city","country","email","age"],
        "dim_products":   ["product_id","product_name","category","unit_price","category_id"],
        "dim_employees":  ["employee_id","name","region","department"],
        "dim_categories": ["category_id","category_name"],
        "spend_summary":  ["customer_id","total_spend","last_order_date"],
    }


@pytest.fixture(scope="module", autouse=True)
def setup_join_paths():
    """Set up join paths for performance testing."""
    set_join_paths({
        "fact_orders": {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products":  "fact_orders.product_id  = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers":  {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
        "dim_products":   {"fact_orders": "fact_orders.product_id  = dim_products.product_id",
                           "dim_categories": "dim_products.category_id = dim_categories.category_id"},
        "dim_employees":  {"fact_orders": "fact_orders.employee_id = dim_employees.employee_id"},
        "dim_categories": {"dim_products": "dim_products.category_id = dim_categories.category_id"},
        "spend_summary":  {},
    })
    yield


class TestSqlGenerationSpeed:
    """Performance tests for SQL generation speed."""

    def test_simple_query_generation_speed(self, benchmark, schema):
        """Test the speed of generating a simple query."""
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
        
        def generate_sql():
            ok, errors = validate_intent(intent, schema)
            assert ok, f"Validation failed: {'; '.join(errors)}"
            return build_sql(intent, "postgresql")
        
        sql, params = benchmark(generate_sql)
        
        assert "SELECT" in sql
        assert "SUM(fact_orders.unit_price)" in sql

    def test_complex_query_generation_speed(self, benchmark, schema):
        """Test the speed of generating a complex query."""
        intent = {
            "metrics": [
                {"metric":"revenue","aggregation":"SUM","target_column":"unit_price"},
                {"metric":"count","aggregation":"COUNT","target_column":"order_id"}
            ],
            "fact_table": "fact_orders",
            "group_by": ["dim_products.category", "fact_orders.region"],
            "joins": [
                "fact_orders.product_id = dim_products.product_id",
                "fact_orders.customer_id = dim_customers.customer_id"
            ],
            "filters": [
                {"column":"status","operator":"=","value":"shipped"},
                {"column":"dim_customers.country","operator":"=","value":"US"}
            ],
            "computed_columns": [
                {
                    "alias": "spending_tier",
                    "when_clauses": [
                        {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                        {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'"},
                    ],
                    "else_value": "'Low'",
                    "include_in_group_by": False,
                }
            ],
            "time_filter": None,
            "time_bucket": None,
            "time_bucket_column": None,
            "having": [{"aggregation":"COUNT","target_column":"order_id","distinct":True,"operator":">","value":3}],
            "limit": 100,
            "order_by": "revenue",
            "order_dir": "DESC",
        }
        
        def generate_sql():
            ok, errors = validate_intent(intent, schema)
            assert ok, f"Validation failed: {'; '.join(errors)}"
            return build_sql(intent, "postgresql")
        
        sql, params = benchmark(generate_sql)
        
        assert "SELECT" in sql
        assert "SUM(fact_orders.unit_price)" in sql
        assert "COUNT(*)" in sql
        assert "JOIN" in sql
        assert "WHERE" in sql
        assert "GROUP BY" in sql
        assert "HAVING" in sql

    def test_window_function_query_speed(self, benchmark, schema):
        """Test the speed of generating a query with window functions."""
        intent = {
            "metrics": [{"metric":"revenue","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table": "fact_orders",
            "group_by": ["fact_orders.customer_id"],
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
            "window_functions": [
                {
                    "alias":"rank",
                    "function":"RANK",
                    "partition_by":["fact_orders.customer_id"],
                    "order_by":"revenue",
                    "order_dir":"DESC"
                },
                {
                    "alias":"running_total",
                    "function":"SUM",
                    "target_column":"unit_price",
                    "partition_by":[],
                    "order_by":"order_date",
                    "order_dir":"ASC",
                    "frame_spec":"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                }
            ]
        }
        
        def generate_sql():
            ok, errors = validate_intent(intent, schema)
            assert ok, f"Validation failed: {'; '.join(errors)}"
            return build_sql(intent, "postgresql")
        
        sql, params = benchmark(generate_sql)
        
        assert "SELECT" in sql
        assert "RANK() OVER" in sql
        assert "SUM(" in sql
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql

    def test_cte_query_generation_speed(self, benchmark, schema):
        """Test the speed of generating a query with CTEs."""
        intent = {
            "metrics": [{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table": "spend_summary",
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
            "ctes": [
                {
                    "name":"spend_summary",
                    "intent":{
                        "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
                        "fact_table":"fact_orders",
                        "group_by":["fact_orders.customer_id"]
                    }
                },
                {
                    "name":"high_spenders",
                    "intent":{
                        "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
                        "fact_table":"fact_orders",
                        "group_by":["fact_orders.customer_id"],
                        "filters":[{"column":"unit_price","operator":">","value":100}]
                    }
                }
            ]
        }
        
        def generate_sql():
            ok, errors = validate_intent(intent, schema)
            assert ok, f"Validation failed: {'; '.join(errors)}"
            return build_sql(intent, "postgresql")
        
        sql, params = benchmark(generate_sql)
        
        assert "SELECT" in sql
        assert "WITH spend_summary AS" in sql
        assert "high_spenders AS" in sql