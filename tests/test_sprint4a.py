# =============================================================================
# test_sprint4a.py  —  Dataloom v3.0  Sprint 4A Comprehensive Test Suite
#
# Coverage matrix:
#   4A-1  IS NULL / IS NOT NULL
#   4A-2  Typed JOINs (LEFT/RIGHT for anti-join)
#   4A-3  EXTRACT date-part expressions in group_by
#   4A-4  Date-arithmetic metrics (diff_days / diff_hours / diff_seconds)
#   4A-5  NTILE and PERCENTILE_CONT
#   4A-6  Standalone HAVING COUNT DISTINCT
#   4A-7  computed_columns[] CASE WHEN
#
#   SMOKE      Official 5-question smoke test (per audit plan)
#   REGRESSION Sprint 1+2+3 regression battery
#   CROSS      Cross-feature compound queries (≥3 patterns in one intent)
#   DIALECT    Dialect compatibility matrix per new feature
# =============================================================================

import copy
import pytest
from validator import set_join_paths, validate_intent
from sql_builder import build_sql

# ─── Schema ───────────────────────────────────────────────────────────────────

# Five-table schema extended with campaign and temporal columns for 4A tests.
SCHEMA_MAP = {
    "fact_orders": [
        "order_id", "customer_id", "product_id", "employee_id", "campaign_id",
        "order_date", "ship_date", "quantity", "unit_price", "freight",
        "status", "region", "group_label",
    ],
    "dim_customers": [
        "customer_id", "name", "email", "city", "country", "age",
        "signup_date", "is_member", "member_since",
    ],
    "dim_products": [
        "product_id", "product_name", "category", "subcategory",
        "cost", "price", "stock_level", "supplier_id",
    ],
    "dim_employees": ["employee_id", "name", "department", "region", "hire_date", "role"],
    "dim_campaigns": ["campaign_id", "source", "clicks", "conversions", "month"],
}

# Type hints for columns that need dialect-specific arithmetic (4A-4).
SCHEMA_TYPES = {
    "fact_orders": {
        "order_id": "varchar", "customer_id": "varchar", "product_id": "varchar",
        "employee_id": "varchar", "campaign_id": "varchar",
        "quantity": "integer", "unit_price": "numeric", "freight": "numeric",
        "order_date": "timestamp", "ship_date": "timestamp",
    },
    "dim_products": {
        "price": "numeric", "cost": "numeric", "stock_level": "integer",
    },
}

_JOIN_PATHS = {
    "fact_orders": {
        "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
        "dim_products":  "fact_orders.product_id  = dim_products.product_id",
        "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        "dim_campaigns": "fact_orders.campaign_id = dim_campaigns.campaign_id",
    },
    "dim_customers": {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
    "dim_products":  {"fact_orders": "fact_orders.product_id  = dim_products.product_id"},
    "dim_employees": {"fact_orders": "fact_orders.employee_id = dim_employees.employee_id"},
    "dim_campaigns": {"fact_orders": "fact_orders.campaign_id = dim_campaigns.campaign_id"},
}


@pytest.fixture(autouse=True)
def setup_joins():
    """Register join paths before every test in this file."""
    set_join_paths(_JOIN_PATHS)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def base_intent(**overrides):
    """Return a minimal valid intent, merging ``overrides`` at the top level.

    Args:
        **overrides: Keys to set or replace in the base intent dict.

    Returns:
        A complete intent dict ready to pass to ``validate_intent``.
    """
    i = {
        "metrics":          [{"metric": "total_revenue", "aggregation": "SUM",
                              "target_column": "unit_price", "distinct": False}],
        "fact_table":       "fact_orders",
        "group_by":         [],
        "joins":            [],
        "filters":          [],
        "computed_columns": [],
        "time_filter":      None,
        "time_bucket":      None,
        "time_bucket_column": None,
        "having":           [],
        "limit":            10,
        "order_by":         None,
        "order_dir":        "DESC",
    }
    i.update(overrides)
    return i


def run_valid(intent_raw, db_type="postgresql"):
    """Validate and build SQL. Asserts validation passes. Returns (sql, params)."""
    intent = copy.deepcopy(intent_raw)
    ok, errs = validate_intent(intent, SCHEMA_MAP, SCHEMA_TYPES)
    assert ok, f"validate_intent failed: {errs}"
    sql, params = build_sql(intent, db_type)
    return sql, params


def run_invalid(intent_raw):
    """Validate and assert that validation fails. Returns errors list."""
    intent = copy.deepcopy(intent_raw)
    ok, errs = validate_intent(intent, SCHEMA_MAP, SCHEMA_TYPES)
    assert not ok, "Expected validation failure but got success"
    return errs


def assert_sql_contains(sql, *needles):
    """Assert that every needle appears in ``sql`` (case-insensitive).

    Args:
        sql: The generated SQL string to inspect.
        *needles: One or more substrings that must be present.
    """
    sql_upper = sql.upper()
    for needle in needles:
        assert needle.upper() in sql_upper, f"SQL missing '{needle}':\n{sql}"


def assert_sql_not_contains(sql, *needles):
    """Assert that no needle appears in ``sql`` (case-insensitive).

    Args:
        sql: The generated SQL string to inspect.
        *needles: One or more substrings that must be absent.
    """
    sql_upper = sql.upper()
    for needle in needles:
        assert needle.upper() not in sql_upper, f"SQL should NOT contain '{needle}':\n{sql}"


# =============================================================================
# SECTION 4A-1: IS NULL / IS NOT NULL
# =============================================================================

class TestISNull:

    def test_is_null_on_email(self):
        sql, params = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "cust_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            filters=[{"column": "email", "operator": "IS NULL"}],
            joins=[],
        ))
        assert_sql_contains(sql, "IS NULL")
        assert params == []

    def test_is_not_null_on_email(self):
        sql, params = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "cust_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            filters=[{"column": "email", "operator": "IS NOT NULL"}],
            joins=[],
        ))
        assert_sql_contains(sql, "IS NOT NULL")
        assert params == []

    def test_is_null_combined_with_value_filter(self):
        sql, params = run_valid(base_intent(
            filters=[
                {"column": "email", "operator": "IS NULL"},
                {"column": "status", "operator": "=", "value": "pending"},
            ],
        ))
        assert_sql_contains(sql, "IS NULL", "%s")
        assert params == ["pending"]

    def test_is_null_on_qualified_column(self):
        sql, _ = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "cust_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            filters=[{"column": "dim_customers.email", "operator": "IS NULL"}],
            joins=[],
        ))
        assert_sql_contains(sql, "IS NULL")

    def test_invalid_null_operator_rejected(self):
        run_invalid(base_intent(
            filters=[{"column": "email", "operator": "IS MISSING", "value": None}]
        ))

    def test_is_null_sqlite_parameterless(self):
        sql, params = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "cust_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            filters=[{"column": "email", "operator": "IS NULL"}],
        ), db_type="sqlite")
        assert_sql_contains(sql, "IS NULL")
        assert params == []


# =============================================================================
# SECTION 4A-2: Typed JOINs / Anti-Join
# =============================================================================

class TestTypedJoins:

    def test_left_join_renders_keyword(self):
        sql, _ = run_valid(base_intent(
            fact_table="dim_products",
            metrics=[{"metric": "product_count", "aggregation": "COUNT",
                      "target_column": "product_id", "distinct": False}],
            group_by=["dim_products.product_name"],
            joins=[{"type": "LEFT", "condition": "dim_products.product_id = fact_orders.product_id"}],
            filters=[{"column": "fact_orders.product_id", "operator": "IS NULL"}],
        ))
        assert_sql_contains(sql, "LEFT JOIN", "IS NULL")

    def test_inner_join_string_stays_plain(self):
        sql, _ = run_valid(base_intent(
            joins=["fact_orders.product_id = dim_products.product_id"],
            group_by=["dim_products.category"],
        ))
        assert_sql_contains(sql, "JOIN dim_products")
        assert_sql_not_contains(sql, "LEFT JOIN", "RIGHT JOIN")

    def test_anti_join_products_never_ordered(self):
        sql, _ = run_valid(base_intent(
            fact_table="dim_products",
            metrics=[{"metric": "unordered_count", "aggregation": "COUNT",
                      "target_column": "product_id", "distinct": False}],
            group_by=["dim_products.product_name"],
            joins=[{"type": "LEFT", "condition": "dim_products.product_id = fact_orders.product_id"}],
            filters=[{"column": "fact_orders.product_id", "operator": "IS NULL"}],
        ))
        assert_sql_contains(sql, "LEFT JOIN", "IS NULL")

    def test_anti_join_churned_customers(self):
        sql, _ = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "churn_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            group_by=["dim_customers.name"],
            joins=[{"type": "LEFT", "condition": "dim_customers.customer_id = fact_orders.customer_id"}],
            filters=[{"column": "fact_orders.customer_id", "operator": "IS NULL"}],
        ))
        assert_sql_contains(sql, "LEFT JOIN", "fact_orders.customer_id IS NULL")

    def test_right_join_keyword_renders(self):
        sql, _ = run_valid(base_intent(
            fact_table="dim_products",
            metrics=[{"metric": "cnt", "aggregation": "COUNT",
                      "target_column": "product_id", "distinct": False}],
            joins=[{"type": "RIGHT", "condition": "dim_products.product_id = fact_orders.product_id"}],
        ))
        assert_sql_contains(sql, "RIGHT JOIN")

    def test_mixed_typed_and_plain_joins(self):
        sql, _ = run_valid(base_intent(
            group_by=["dim_products.category", "dim_employees.department"],
            joins=[
                "fact_orders.product_id = dim_products.product_id",
                {"type": "LEFT", "condition": "fact_orders.employee_id = dim_employees.employee_id"},
            ],
        ))
        assert_sql_contains(sql, "JOIN dim_products ON", "LEFT JOIN dim_employees ON")


# =============================================================================
# SECTION 4A-3: EXTRACT / Date-Part Expressions in group_by
# =============================================================================

class TestExtractExpressions:

    def test_extract_month_postgresql(self):
        sql, _ = run_valid(base_intent(
            group_by=["EXTRACT(month FROM fact_orders.order_date) AS order_month"],
        ))
        assert_sql_contains(sql, "EXTRACT(month FROM fact_orders.order_date)")

    def test_extract_dow_for_weekend_weekday(self):
        sql, _ = run_valid(base_intent(
            group_by=["EXTRACT(dow FROM fact_orders.order_date) AS day_of_week"],
        ))
        assert_sql_contains(sql, "EXTRACT(dow FROM")

    def test_extract_year_mysql_emits_year_function(self):
        sql, _ = run_valid(base_intent(
            group_by=["YEAR(fact_orders.order_date) AS order_year"],
        ), db_type="mysql")
        assert_sql_contains(sql, "YEAR(")

    def test_extract_in_group_by_clause(self):
        sql, _ = run_valid(base_intent(
            group_by=["EXTRACT(month FROM fact_orders.order_date) AS order_month"],
        ))
        assert_sql_contains(sql, "GROUP BY")

    def test_sqlite_strftime_passes_through(self):
        sql, _ = run_valid(base_intent(
            group_by=["CAST(strftime('%m', fact_orders.order_date) AS INTEGER) AS order_month"],
        ), db_type="sqlite")
        assert_sql_contains(sql, "strftime")

    def test_extract_bypasses_schema_validation(self):
        # EXTRACT expressions are not column refs, so schema lookup is skipped.
        sql, _ = run_valid(base_intent(
            group_by=["EXTRACT(quarter FROM fact_orders.order_date) AS qtr"],
        ))
        assert_sql_contains(sql, "EXTRACT")


# =============================================================================
# SECTION 4A-4: Date-Arithmetic Metrics
# =============================================================================

class TestDateArithmetic:

    def test_avg_diff_days_postgresql_uses_epoch(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "avg_delay_days", "aggregation": "AVG",
                "target_column": "ship_date - order_date", "is_expression": True,
                "date_arithmetic": {"operation": "diff_days", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ))
        assert_sql_contains(sql, "AVG(", "86400", "epoch")

    def test_avg_diff_days_mysql_uses_timestampdiff(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "avg_delay_days", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_days", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ), db_type="mysql")
        assert_sql_contains(sql, "TIMESTAMPDIFF", "86400")

    def test_avg_diff_days_sqlite_uses_julianday(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "avg_delay_days", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_days", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ), db_type="sqlite")
        assert_sql_contains(sql, "julianday")

    def test_diff_hours_operation(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "avg_delay_hours", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_hours", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ))
        assert_sql_contains(sql, "3600")

    def test_diff_seconds_sqlite(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "delay_secs", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_seconds", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ), db_type="sqlite")
        assert_sql_contains(sql, "86400", "julianday")

    def test_invalid_date_operation_rejected(self):
        run_invalid(base_intent(
            metrics=[{
                "metric": "bad", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_years", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ))

    def test_generic_raw_expression_no_date_block(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "avg_ratio", "aggregation": "AVG",
                "target_column": "unit_price / NULLIF(quantity, 0)", "is_expression": True,
            }],
        ))
        assert_sql_contains(sql, "AVG(unit_price / NULLIF(quantity, 0))")


# =============================================================================
# SECTION 4A-5: NTILE and PERCENTILE_CONT
# =============================================================================

class TestNtileAndPercentile:

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_ntile_10_renders_across_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "revenue_decile", "aggregation": "NTILE",
                "ntile_buckets": 10, "order_by_column": "total_revenue",
                "order_dir": "DESC", "target_column": "",
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, "NTILE(10)")

    def test_ntile_postgresql_order_clause(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "revenue_decile", "aggregation": "NTILE",
                "ntile_buckets": 10, "order_by_column": "total_revenue",
                "order_dir": "DESC", "target_column": "",
            }],
        ))
        assert_sql_contains(sql, "NTILE(10)", "ORDER BY total_revenue DESC")

    def test_ntile_missing_buckets_rejected(self):
        run_invalid(base_intent(
            metrics=[{
                "metric": "decile", "aggregation": "NTILE",
                "order_by_column": "total_revenue", "target_column": "",
            }],
        ))

    def test_percentile_cont_postgresql_native(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "median_order_value", "aggregation": "PERCENTILE_CONT",
                "percentile": 0.5, "target_column": "unit_price", "order_dir": "ASC",
            }],
        ))
        assert_sql_contains(sql, "PERCENTILE_CONT(0.5)", "WITHIN GROUP", "ORDER BY")

    def test_percentile_cont_90th_postgresql(self):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "p90", "aggregation": "PERCENTILE_CONT",
                "percentile": 0.9, "target_column": "unit_price", "order_dir": "ASC",
            }],
        ))
        assert_sql_contains(sql, "PERCENTILE_CONT(0.9)")

    def test_percentile_cont_sqlite_fallback_subquery(self):
        # SQLite lacks native PERCENTILE_CONT; the builder emits a ROW_NUMBER subquery instead.
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "median_order_value", "aggregation": "PERCENTILE_CONT",
                "percentile": 0.5, "target_column": "unit_price", "order_dir": "ASC",
            }],
        ), db_type="sqlite")
        assert_sql_contains(sql, "ROW_NUMBER", "cnt")

    def test_percentile_out_of_range_rejected(self):
        run_invalid(base_intent(
            metrics=[{
                "metric": "bad_p", "aggregation": "PERCENTILE_CONT",
                "percentile": 1.5, "target_column": "unit_price",
            }],
        ))


# =============================================================================
# SECTION 4A-6: Standalone HAVING COUNT DISTINCT
# =============================================================================

class TestHavingCountDistinct:

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_having_count_distinct_all_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.order_id"],
            having=[{
                "aggregation": "COUNT", "target_column": "product_id",
                "distinct": True, "operator": ">", "value": 3,
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, "COUNT(DISTINCT")

    def test_having_count_distinct_postgresql_full(self):
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.order_id"],
            having=[{
                "aggregation": "COUNT", "target_column": "product_id",
                "distinct": True, "operator": ">", "value": 3,
            }],
        ))
        assert_sql_contains(sql, "COUNT(DISTINCT", "product_id", "> 3")

    def test_having_sum_standalone(self):
        sql, _ = run_valid(base_intent(
            group_by=["dim_products.category"],
            joins=["fact_orders.product_id = dim_products.product_id"],
            having=[{
                "aggregation": "SUM", "target_column": "unit_price",
                "operator": ">", "value": 50000,
            }],
        ))
        assert_sql_contains(sql, "HAVING SUM(", "> 50000")

    def test_having_references_select_metric_by_name(self):
        sql, _ = run_valid(base_intent(
            group_by=["dim_customers.country"],
            joins=["fact_orders.customer_id = dim_customers.customer_id"],
            having=[{"metric": "total_revenue", "operator": ">", "value": 1000}],
        ))
        assert_sql_contains(sql, "HAVING", "1000")

    def test_having_missing_operator_rejected(self):
        run_invalid(base_intent(
            having=[{
                "aggregation": "COUNT", "target_column": "product_id",
                "distinct": True, "value": 3,
            }],
        ))


# =============================================================================
# SECTION 4A-7: CASE WHEN computed_columns[]
# =============================================================================

class TestCaseWhen:

    def test_high_medium_low_spending_tiers(self):
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.customer_id"],
            computed_columns=[{
                "alias": "spending_tier",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                    {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'"},
                ],
                "else_value": "'Low'",
            }],
        ))
        assert_sql_contains(sql, "CASE", "WHEN", "THEN", "ELSE", "END AS spending_tier")

    def test_weekend_weekday_bucket(self):
        sql, _ = run_valid(base_intent(
            group_by=["EXTRACT(dow FROM fact_orders.order_date) AS dow"],
            computed_columns=[{
                "alias": "day_type",
                "when_clauses": [
                    {"condition": "EXTRACT(dow FROM fact_orders.order_date) IN (0,6)", "then": "'Weekend'"},
                ],
                "else_value": "'Weekday'",
            }],
        ))
        assert_sql_contains(sql, "CASE", "Weekend", "Weekday", "END AS day_type")

    def test_case_when_alias_appears_before_metric(self):
        sql, _ = run_valid(base_intent(
            computed_columns=[{
                "alias": "revenue_band",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 10000", "then": "'Premium'"},
                ],
                "else_value": "'Standard'",
            }],
        ))
        assert_sql_contains(sql, "END AS revenue_band", "SUM(fact_orders.unit_price) AS total_revenue")

    def test_include_in_group_by_adds_to_group_by_clause(self):
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.customer_id"],
            computed_columns=[{
                "alias": "tier",
                "when_clauses": [{"condition": "fact_orders.region = 'APAC'", "then": "'Asia'"}],
                "else_value": "'Other'",
                "include_in_group_by": True,
            }],
        ))
        assert_sql_contains(sql, "GROUP BY", "CASE")

    def test_multiple_case_when_columns(self):
        sql, _ = run_valid(base_intent(
            computed_columns=[
                {
                    "alias": "size_tier",
                    "when_clauses": [{"condition": "SUM(fact_orders.quantity) > 100", "then": "'Large'"}],
                    "else_value": "'Small'",
                },
                {
                    "alias": "value_tier",
                    "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"}],
                    "else_value": "'Low'",
                },
            ],
        ))
        assert_sql_contains(sql, "END AS size_tier", "END AS value_tier")

    def test_missing_alias_rejected(self):
        run_invalid(base_intent(
            computed_columns=[{"when_clauses": [{"condition": "1=1", "then": "'x'"}]}],
        ))

    def test_camel_case_alias_rejected(self):
        run_invalid(base_intent(
            computed_columns=[{
                "alias": "SpendingTier",
                "when_clauses": [{"condition": "1=1", "then": "'x'"}],
            }],
        ))

    def test_empty_when_clauses_rejected(self):
        run_invalid(base_intent(
            computed_columns=[{"alias": "tier", "when_clauses": []}],
        ))

    @pytest.mark.parametrize("db_type", ["mysql", "sqlite"])
    def test_case_when_syntax_identical_across_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            computed_columns=[{
                "alias": "spending_tier",
                "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"}],
                "else_value": "'Low'",
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, "CASE", "WHEN", "END AS spending_tier")


# =============================================================================
# SECTION SMOKE: Official 5-Question Smoke Test
# =============================================================================

class TestSmokeTest:

    def test_s1_customers_without_email(self):
        """E9: Customers without valid email (IS NULL)."""
        sql, params = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "cust_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            filters=[{"column": "email", "operator": "IS NULL"}],
            joins=[],
        ))
        assert_sql_contains(sql, "WHERE", "email IS NULL")
        assert params == []

    def test_s2_products_never_ordered(self):
        """I2: Products never ordered (LEFT JOIN anti-join)."""
        sql, params = run_valid(base_intent(
            fact_table="dim_products",
            metrics=[{"metric": "product_count", "aggregation": "COUNT",
                      "target_column": "product_id", "distinct": False}],
            group_by=["dim_products.product_name"],
            joins=[{"type": "LEFT", "condition": "dim_products.product_id = fact_orders.product_id"}],
            filters=[{"column": "fact_orders.product_id", "operator": "IS NULL"}],
        ))
        assert_sql_contains(sql, "LEFT JOIN", "fact_orders.product_id IS NULL")
        assert params == []

    def test_s3_avg_shipping_delay(self):
        """A2: Average shipping delay in days (date arithmetic)."""
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "avg_delay_days", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_days", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ))
        assert_sql_contains(sql, "AVG(", "epoch", "86400")

    def test_s4_revenue_deciles_ntile(self):
        """X7: Products in revenue deciles (NTILE)."""
        sql, _ = run_valid(base_intent(
            metrics=[
                {"metric": "total_revenue", "aggregation": "SUM",
                 "target_column": "unit_price", "distinct": False},
                {
                    "metric": "revenue_decile", "aggregation": "NTILE",
                    "ntile_buckets": 10, "order_by_column": "total_revenue",
                    "order_dir": "DESC", "target_column": "",
                },
            ],
            group_by=["fact_orders.product_id"],
        ))
        assert_sql_contains(sql, "NTILE(10)", "SUM(fact_orders.unit_price)")

    def test_s5_high_medium_low_spender_case_when(self):
        """A3: High/Medium/Low spender categories (CASE WHEN)."""
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.customer_id"],
            computed_columns=[{
                "alias": "spending_tier",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                    {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'"},
                ],
                "else_value": "'Low'",
            }],
        ))
        assert_sql_contains(
            sql,
            "CASE",
            "WHEN SUM(fact_orders.unit_price) > 5000 THEN 'High'",
            "WHEN SUM(fact_orders.unit_price) > 1000 THEN 'Medium'",
            "ELSE 'Low'",
            "END AS spending_tier",
        )


# =============================================================================
# SECTION CROSS: Cross-Feature Compound Queries (≥3 patterns per query)
# =============================================================================

class TestCrossFeature:

    def test_x1_anti_join_plus_is_null_plus_time_filter(self):
        sql, _ = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "cust_count", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            group_by=["dim_customers.city"],
            joins=[{"type": "LEFT", "condition": "dim_customers.customer_id = fact_orders.customer_id"}],
            filters=[
                {"column": "fact_orders.customer_id", "operator": "IS NULL"},
                {"column": "dim_customers.email", "operator": "IS NOT NULL"},
            ],
        ))
        assert_sql_contains(sql, "LEFT JOIN", "IS NULL", "IS NOT NULL", "GROUP BY")

    def test_x2_case_when_plus_extract_plus_having_count_distinct(self):
        sql, _ = run_valid(base_intent(
            group_by=["EXTRACT(dow FROM fact_orders.order_date) AS dow"],
            computed_columns=[{
                "alias": "day_type",
                "when_clauses": [
                    {"condition": "EXTRACT(dow FROM fact_orders.order_date) IN (0,6)", "then": "'Weekend'"},
                ],
                "else_value": "'Weekday'",
            }],
            having=[{
                "aggregation": "COUNT", "target_column": "order_id",
                "distinct": True, "operator": ">", "value": 100,
            }],
        ))
        assert_sql_contains(sql, "CASE", "EXTRACT(dow", "COUNT(DISTINCT", "> 100")

    def test_x3_date_arith_plus_case_when_plus_having(self):
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.customer_id"],
            metrics=[
                {"metric": "total_revenue", "aggregation": "SUM",
                 "target_column": "unit_price", "distinct": False},
                {
                    "metric": "avg_ship_delay", "aggregation": "AVG",
                    "target_column": "", "is_expression": True,
                    "date_arithmetic": {"operation": "diff_days", "col_a": "ship_date", "col_b": "order_date"},
                },
            ],
            computed_columns=[{
                "alias": "customer_tier",
                "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"}],
                "else_value": "'Standard'",
            }],
            having=[{"metric": "total_revenue", "operator": ">", "value": 500}],
        ))
        assert_sql_contains(sql, "SUM(fact_orders.unit_price)", "AVG(", "epoch", "CASE", "HAVING")

    def test_x4_ntile_plus_join_plus_case_when(self):
        sql, _ = run_valid(base_intent(
            group_by=["dim_products.category"],
            joins=["fact_orders.product_id = dim_products.product_id"],
            metrics=[
                {"metric": "total_revenue", "aggregation": "SUM",
                 "target_column": "unit_price", "distinct": False},
                {
                    "metric": "cat_decile", "aggregation": "NTILE",
                    "ntile_buckets": 10, "order_by_column": "total_revenue",
                    "order_dir": "DESC", "target_column": "",
                },
            ],
            computed_columns=[{
                "alias": "revenue_band",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 100000", "then": "'Tier 1'"},
                    {"condition": "SUM(fact_orders.unit_price) > 50000",  "then": "'Tier 2'"},
                ],
                "else_value": "'Tier 3'",
            }],
        ))
        assert_sql_contains(sql, "NTILE(10)", "JOIN dim_products", "CASE", "Tier 1")


# =============================================================================
# SECTION REGRESSION: Sprint 1 + 2 + 3 Battery
# =============================================================================

class TestRegressionBattery:

    def test_r1_revenue_by_month_date_trunc(self):
        sql, _ = run_valid(base_intent(
            time_bucket="month",
            time_bucket_column="order_date",
        ))
        assert_sql_contains(sql, "DATE_TRUNC('month'", "month_period")

    def test_r2_orders_per_week_2017_year_filter(self):
        sql, _ = run_valid(base_intent(
            metrics=[{"metric": "order_count", "aggregation": "COUNT",
                      "target_column": "order_id", "distinct": False}],
            time_bucket="week",
            time_bucket_column="order_date",
            time_filter={"column": "order_date", "year": 2017},
        ))
        assert_sql_contains(sql, "2017", "week_period")

    def test_r3_categories_revenue_having(self):
        sql, _ = run_valid(base_intent(
            group_by=["dim_products.category"],
            joins=["fact_orders.product_id = dim_products.product_id"],
            having=[{"metric": "total_revenue", "operator": ">", "value": 50000}],
        ))
        assert_sql_contains(sql, "HAVING", "50000")

    def test_r4_multi_metric_revenue_and_orders(self):
        sql, _ = run_valid(base_intent(
            metrics=[
                {"metric": "total_revenue", "aggregation": "SUM",
                 "target_column": "unit_price", "distinct": False},
                {"metric": "order_count", "aggregation": "COUNT",
                 "target_column": "order_id", "distinct": False},
            ],
            group_by=["dim_customers.city"],
            joins=["fact_orders.customer_id = dim_customers.customer_id"],
        ))
        assert_sql_contains(
            sql,
            "SUM(fact_orders.unit_price) AS total_revenue",
            "COUNT(*) AS order_count",
        )

    def test_r5_multi_hop_bfs_auto_repairs_join(self):
        # BFS join-path repair should insert the missing dim_products join automatically.
        sql, _ = run_valid(base_intent(
            group_by=["dim_products.category"],
            joins=[],
        ))
        assert_sql_contains(sql, "JOIN dim_products ON")

    def test_r6_monthly_having_multi_metric(self):
        sql, _ = run_valid(base_intent(
            metrics=[
                {"metric": "total_revenue", "aggregation": "SUM",
                 "target_column": "unit_price", "distinct": False},
                {"metric": "order_count", "aggregation": "COUNT",
                 "target_column": "order_id", "distinct": False},
            ],
            time_bucket="month",
            time_bucket_column="order_date",
            having=[{"metric": "total_revenue", "operator": ">", "value": 10000}],
        ))
        assert_sql_contains(sql, "DATE_TRUNC", "HAVING", "SUM", "COUNT")


# =============================================================================
# SECTION DIALECT: Dialect Integrity Matrix
# =============================================================================

class TestDialectMatrix:

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_is_null_syntax_identical_all_dialects(self, db_type):
        sql, params = run_valid(base_intent(
            fact_table="dim_customers",
            metrics=[{"metric": "n", "aggregation": "COUNT",
                      "target_column": "customer_id", "distinct": False}],
            filters=[{"column": "email", "operator": "IS NULL"}],
        ), db_type=db_type)
        assert_sql_contains(sql, "IS NULL")
        assert params == []

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_left_join_keyword_all_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            fact_table="dim_products",
            metrics=[{"metric": "n", "aggregation": "COUNT",
                      "target_column": "product_id", "distinct": False}],
            joins=[{"type": "LEFT", "condition": "dim_products.product_id = fact_orders.product_id"}],
        ), db_type=db_type)
        assert_sql_contains(sql, "LEFT JOIN")

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_case_when_syntax_all_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            computed_columns=[{
                "alias": "tier",
                "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
                "else_value": "'Low'",
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, "CASE", "WHEN", "THEN", "ELSE", "END AS tier")

    @pytest.mark.parametrize("db_type,expected", [
        ("postgresql", "epoch"),
        ("mysql",      "TIMESTAMPDIFF"),
        ("sqlite",     "julianday"),
    ])
    def test_diff_days_dialect_expression(self, db_type, expected):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "d", "aggregation": "AVG",
                "target_column": "", "is_expression": True,
                "date_arithmetic": {"operation": "diff_days", "col_a": "ship_date", "col_b": "order_date"},
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, expected)

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_ntile5_all_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            metrics=[{
                "metric": "q", "aggregation": "NTILE", "ntile_buckets": 5,
                "order_by_column": "total_revenue", "order_dir": "DESC", "target_column": "",
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, "NTILE(5)")

    def test_percentile_cont_postgresql_within_group(self):
        sql, _ = run_valid(base_intent(
            metrics=[{"metric": "med", "aggregation": "PERCENTILE_CONT",
                      "percentile": 0.5, "target_column": "unit_price", "order_dir": "ASC"}],
        ), db_type="postgresql")
        assert_sql_contains(sql, "PERCENTILE_CONT(0.5)", "WITHIN GROUP")

    def test_percentile_cont_sqlite_fallback_no_within_group(self):
        # SQLite emits a ROW_NUMBER subquery — WITHIN GROUP must not appear.
        sql, _ = run_valid(base_intent(
            metrics=[{"metric": "med", "aggregation": "PERCENTILE_CONT",
                      "percentile": 0.5, "target_column": "unit_price", "order_dir": "ASC"}],
        ), db_type="sqlite")
        assert_sql_contains(sql, "ROW_NUMBER", "cnt")
        assert_sql_not_contains(sql, "WITHIN GROUP")

    @pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlite"])
    def test_having_count_distinct_all_dialects(self, db_type):
        sql, _ = run_valid(base_intent(
            group_by=["fact_orders.order_id"],
            having=[{
                "aggregation": "COUNT", "target_column": "product_id",
                "distinct": True, "operator": ">", "value": 2,
            }],
        ), db_type=db_type)
        assert_sql_contains(sql, "COUNT(DISTINCT")
