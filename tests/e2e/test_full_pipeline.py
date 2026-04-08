"""
Dataloom end-to-end pipeline tests.

Validates the complete flow from intent validation through SQL generation for
all supported feature sprints (Baseline, 4B, 4C). Mirrors eval_harness.py
coverage in pytest form so CI catches regressions on every merge.

Public functions: run_test_case
"""
import pytest
from validator import validate_intent, set_join_paths
from sql_builder import build_sql


@pytest.fixture(scope="module")
def schema():
    """Shared schema for end-to-end testing."""
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
    """Set up join paths for end-to-end testing."""
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


def run_test_case(intent, schema_map, must_contain=None, must_not_contain=None,
                  expect_valid=True, db="postgresql"):
    """Validate an intent and assert SQL output constraints.

    Runs validate_intent followed by build_sql, then checks that the
    generated SQL contains or excludes the specified substrings. Comparisons
    are case-insensitive.

    Args:
        intent: Parsed query intent dict passed to validate_intent and build_sql.
        schema_map: Table-to-columns mapping used by the validator.
        must_contain: Substrings that must appear in the generated SQL.
        must_not_contain: Substrings that must not appear in the generated SQL.
        expect_valid: When False, asserts that validation fails and returns early
            without attempting SQL generation.
        db: SQL dialect passed to build_sql. Defaults to "postgresql".

    Returns:
        Tuple of (sql, params) when expect_valid is True, otherwise None.

    Raises:
        pytest.Failed: If validation or SQL content assertions are not met.
    """
    ok, errors = validate_intent(intent, schema_map)

    if not expect_valid:
        assert not ok, f"Expected validation failure, got success"
        return

    assert ok, f"Validation failed: {'; '.join(errors)}"

    sql, params = build_sql(intent, db)

    for needle in (must_contain or []):
        assert needle.upper() in sql.upper(), f"Missing '{needle}' in SQL:\n{sql}"

    for needle in (must_not_contain or []):
        assert needle.upper() not in sql.upper(), f"Should NOT contain '{needle}' in SQL:\n{sql}"

    return sql, params


class TestBaselineFunctionality:
    """Baseline end-to-end tests."""

    def test_basic_sum_group_by(self, schema):
        """BL-01: Basic SUM + GROUP BY."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":["dim_products.category"],
            "joins":["fact_orders.product_id = dim_products.product_id"]
        }

        run_test_case(
            intent, schema,
            must_contain=["SUM(fact_orders.unit_price)","GROUP BY","dim_products.category"]
        )

    def test_count_with_filter(self, schema):
        """BL-02: COUNT with filter."""
        intent = {
            "metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "filters":[{"column":"status","operator":"=","value":"shipped"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["COUNT(","WHERE","fact_orders.status"]
        )

    def test_order_by_desc_limit(self, schema):
        """BL-03: ORDER BY DESC LIMIT."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":["fact_orders.region"],
            "order_by":"rev",
            "order_dir":"DESC",
            "limit":5
        }

        run_test_case(
            intent, schema,
            must_contain=["ORDER BY","DESC","LIMIT 5"]
        )

    def test_left_join_anti_join_is_null(self, schema):
        """BL-04: LEFT JOIN anti-join IS NULL."""
        intent = {
            "metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "joins":[{"condition":"fact_orders.customer_id = dim_customers.customer_id","type":"LEFT"}],
            "filters":[{"column":"dim_customers.customer_id","operator":"IS NULL"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["LEFT JOIN","IS NULL"]
        )

    def test_having_count_distinct(self, schema):
        """BL-05: HAVING COUNT DISTINCT."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":["fact_orders.customer_id"],
            "having":[{"aggregation":"COUNT","target_column":"order_id",
                       "distinct":True,"operator":">","value":3}]
        }

        run_test_case(
            intent, schema,
            must_contain=["HAVING","COUNT(DISTINCT"]
        )


class TestSprint4BFunctionality:
    """Sprint 4B end-to-end tests."""

    def test_rank_over_with_partition_by(self, schema):
        """4B-01: RANK() OVER with PARTITION BY."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":["dim_products.category","fact_orders.customer_id"],
            "joins":["fact_orders.product_id = dim_products.product_id"],
            "window_functions":[{"alias":"rk","function":"RANK",
                                 "partition_by":["dim_products.category"],
                                 "order_by":"rev","order_dir":"DESC"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["RANK() OVER","PARTITION BY dim_products.category"]
        )

    def test_row_number_no_partition(self, schema):
        """4B-02: ROW_NUMBER no partition."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":["fact_orders.customer_id"],
            "window_functions":[{"alias":"rn","function":"ROW_NUMBER",
                                 "partition_by":[],"order_by":"rev","order_dir":"DESC"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["ROW_NUMBER() OVER"],
            must_not_contain=["PARTITION BY"]
        )

    def test_lag_3_arg_form(self, schema):
        """4B-03: LAG 3-arg form."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "window_functions":[{"alias":"prev","function":"LAG","target_column":"rev",
                                 "offset":1,"default":0,"partition_by":[],
                                 "order_by":"order_date","order_dir":"ASC"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["LAG(","1, 0)"]
        )

    def test_lead_function(self, schema):
        """4B-04: LEAD."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "window_functions":[{"alias":"nxt","function":"LEAD","target_column":"rev",
                                 "offset":1,"default":"NULL","partition_by":[],
                                 "order_by":"order_date","order_dir":"ASC"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["LEAD("]
        )

    def test_avg_over_rolling_frame(self, schema):
        """4B-05: AVG OVER rolling 3-row frame."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "window_functions":[{"alias":"roll","function":"AVG","target_column":"unit_price",
                                 "partition_by":[],"order_by":"order_date","order_dir":"ASC",
                                 "frame_spec":"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["AVG(","ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"]
        )

    def test_sum_over_running_total(self, schema):
        """4B-06: SUM OVER running total UNBOUNDED."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "window_functions":[{"alias":"run","function":"SUM","target_column":"unit_price",
                                 "partition_by":[],"order_by":"order_date","order_dir":"ASC",
                                 "frame_spec":"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"}]
        }

        run_test_case(
            intent, schema,
            must_contain=["ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"]
        )

    def test_scalar_subquery_percent_of_total(self, schema):
        """4B-07: Scalar subquery % of total."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":["fact_orders.region"],
            "scalar_subquery":{"alias":"pct","numerator_metric":"rev","multiply_by":100,
                               "denominator":{"aggregation":"SUM","target_column":"unit_price",
                                              "fact_table":"fact_orders"}}
        }

        run_test_case(
            intent, schema,
            must_contain=["NULLIF(","100.0","pct"]
        )

    def test_intersect_set_operation(self, schema):
        """4B-08: INTERSECT set operation."""
        intent = {
            "metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "set_operation":{"operator":"INTERSECT",
                "left": {"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                         "fact_table":"fact_orders","group_by":[],
                         "filters":[{"column":"status","operator":"=","value":"shipped"}]},
                "right":{"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                         "fact_table":"fact_orders","group_by":[],
                         "filters":[{"column":"region","operator":"=","value":"US"}]}}
        }

        run_test_case(
            intent, schema,
            must_contain=["INTERSECT"]
        )

    def test_except_set_operation(self, schema):
        """4B-09: EXCEPT set operation."""
        intent = {
            "metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "set_operation":{"operator":"EXCEPT",
                "left": {"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                         "fact_table":"fact_orders","group_by":[]},
                "right":{"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                         "fact_table":"fact_orders","group_by":[],
                         "filters":[{"column":"status","operator":"=","value":"cancelled"}]}}
        }

        run_test_case(
            intent, schema,
            must_contain=["EXCEPT"]
        )

    def test_bad_frame_spec_rejected(self, schema):
        """4B-10: Bad frame_spec → validation rejects."""
        intent = {
            "metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "window_functions":[{"alias":"x","function":"SUM","target_column":"unit_price",
                                 "partition_by":[],"order_by":"order_date","order_dir":"ASC",
                                 "frame_spec":"INVALID FRAME"}]
        }

        run_test_case(intent, schema, expect_valid=False)


class TestSprint4CFunctionality:
    """Sprint 4C end-to-end tests."""

    def test_cte_with_block(self, schema):
        """4C-01: CTE WITH block."""
        intent = {
            "metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
            "fact_table":"spend_summary",
            "group_by":[],
            "ctes":[{"name":"spend_summary","intent":{
                "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
                "fact_table":"fact_orders",
                "group_by":["fact_orders.customer_id"]
            }}]
        }

        run_test_case(
            intent, schema,
            must_contain=["WITH spend_summary AS","SUM(fact_orders.unit_price)"]
        )

    def test_correlated_subquery_price_greater_than_category_avg(self, schema):
        """4C-04: Correlated subquery price > category avg."""
        intent = {
            "metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id"}],
            "fact_table":"dim_products",
            "group_by":[],
            "correlated_filter":{"column":"dim_products.unit_price","operator":">",
                "subquery":{"aggregation":"AVG","target_column":"unit_price",
                            "fact_table":"dim_products","where_col":"category",
                            "outer_ref":"dim_products.category"}}
        }

        run_test_case(
            intent, schema,
            must_contain=["(SELECT AVG(","WHERE","dim_products.category"]
        )

    def test_having_count_distinct_extract(self, schema):
        """4C-06: HAVING COUNT DISTINCT EXTRACT (X5)."""
        intent = {
            "metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
            "fact_table":"fact_orders",
            "group_by":["fact_orders.customer_id"],
            "having":[{"aggregation":"COUNT",
                       "target_column":"EXTRACT(month FROM fact_orders.order_date)",
                       "distinct":True,"operator":"=","value":12}]
        }

        run_test_case(
            intent, schema,
            must_contain=["COUNT(DISTINCT EXTRACT","HAVING"]
        )

    def test_scalar_subquery_multiply_by_float_ok(self, schema):
        """4C-10: Scalar subquery multiply_by float OK."""
        intent = {
            "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
            "fact_table":"fact_orders",
            "group_by":[],
            "scalar_subquery":{"alias":"pct","numerator_metric":"rev","multiply_by":100.0,
                               "denominator":{"aggregation":"SUM","target_column":"unit_price",
                                              "fact_table":"fact_orders"}}
        }

        run_test_case(
            intent, schema,
            must_contain=["100.0","NULLIF("]
        )
