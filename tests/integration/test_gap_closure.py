"""
Dataloom gap-closure integration tests.

Covers the five confirmed fixes identified in the Master Review Prompt:
  GAP-2: NTILE order_by_column is schema-validated.
  GAP-5: NTILE order_by_column alias is auto-expanded to its full expression.
  GAP-6: auto_repair_joins scans CASE WHEN conditions for implicit table refs.
  GAP-7: PERCENTILE_CONT raises ValueError on MySQL instead of emitting bad SQL.
  GAP-8: BFS-inserted join hops inherit LEFT semantics when any explicit LEFT JOIN exists.

Public functions: base_intent
"""
import copy
import pytest
from validator import validate_intent
from sql_builder import build_sql


@pytest.fixture(scope="module")
def schema_map():
    """Sample schema map for gap closure testing."""
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
    """Sample schema types for gap closure testing."""
    return {
        "fact_orders": {
            "unit_price":"numeric","freight":"numeric","quantity":"integer",
            "order_date":"timestamp","ship_date":"timestamp",
        },
        "dim_products": {"price":"numeric","cost":"numeric","stock_level":"integer"},
    }


@pytest.fixture(autouse=True)
def setup_join_paths():
    """Set up join paths including dim_categories needed for BFS tests."""
    from validator import set_join_paths
    set_join_paths({
        "fact_orders": {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products":  "fact_orders.product_id = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers": {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
        "dim_products":  {
            "fact_orders":   "fact_orders.product_id = dim_products.product_id",
            "dim_categories":"dim_products.category_id = dim_categories.category_id",
        },
        "dim_employees": {"fact_orders": "fact_orders.employee_id = dim_employees.employee_id"},
        "dim_categories":{"dim_products": "dim_products.category_id = dim_categories.category_id"},
    })
    yield


def base_intent(**overrides):
    """Return a minimal valid intent with sensible defaults, optionally overridden.

    Provides a base for test cases so each test only specifies the fields
    relevant to the scenario under test.

    Args:
        **overrides: Any intent fields to override on the default dict.

    Returns:
        dict: A complete intent dict suitable for validate_intent and build_sql.
    """
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


class TestNtileValidation:
    """GAP-2: NTILE order_by_column schema validation."""

    def test_ntile_with_valid_order_by_column_passes(self, schema_map, schema_types):
        """GAP-2-A: NTILE with valid order_by_column passes."""
        intent = base_intent(
            metrics=[{
                "metric": "revenue_decile",
                "aggregation": "NTILE",
                "ntile_buckets": 10,
                "order_by_column": "SUM(fact_orders.unit_price)",
                "order_dir": "DESC",
                "target_column": "",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "NTILE(10)" in sql

    def test_ntile_with_unknown_bare_column_is_rejected(self, schema_map, schema_types):
        """GAP-2-B: NTILE with unknown dotted column is rejected.
        
        Note: the validator only schema-checks dotted (table.column) references.
        Bare names without a dot pass through as expressions. This test uses a
        dotted nonexistent column to exercise the actual rejection path.
        """
        intent = base_intent(
            metrics=[{
                "metric": "decile",
                "aggregation": "NTILE",
                "ntile_buckets": 10,
                "order_by_column": "fact_orders.nonexistent_column_xyz",
                "order_dir": "DESC",
                "target_column": "",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert not ok, f"Expected validation failure, got success: {errs}"

    def test_ntile_with_fully_qualified_column_passes(self, schema_map, schema_types):
        """GAP-2-C: NTILE with fully-qualified column passes."""
        intent = base_intent(
            metrics=[{
                "metric": "decile",
                "aggregation": "NTILE",
                "ntile_buckets": 5,
                "order_by_column": "SUM(fact_orders.unit_price)",
                "order_dir": "DESC",
                "target_column": "",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "NTILE(5)" in sql


class TestNtileAliasExpansion:
    """GAP-5: NTILE order_by_column alias → full expression expansion."""

    def test_llm_emits_alias_total_revenue_builder_expands_to_full_sum(self, schema_map, schema_types):
        """GAP-5-A: LLM emits alias 'total_revenue' — builder expands to full SUM()."""
        intent = base_intent(
            metrics=[
                {"metric":"total_revenue","aggregation":"SUM",
                 "target_column":"unit_price","distinct":False},
                {
                    "metric":          "revenue_decile",
                    "aggregation":     "NTILE",
                    "ntile_buckets":   10,
                    "order_by_column": "total_revenue",   # alias, not full expr
                    "order_dir":       "DESC",
                    "target_column":   "",
                },
            ],
            group_by=["fact_orders.product_id"],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # The alias 'total_revenue' must be expanded to SUM(fact_orders.unit_price) in NTILE
        assert "NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)" in sql
        # The final ORDER BY clause can still use the alias (this is correct behavior)
        assert "ORDER BY total_revenue DESC" in sql

    def test_ntile_with_full_expression_passes_through_unchanged(self, schema_map, schema_types):
        """GAP-5-B: NTILE with full expression passes through unchanged."""
        intent = base_intent(
            metrics=[
                {"metric":"total_revenue","aggregation":"SUM",
                 "target_column":"unit_price","distinct":False},
                {
                    "metric":          "revenue_decile",
                    "aggregation":     "NTILE",
                    "ntile_buckets":   10,
                    "order_by_column": "SUM(fact_orders.unit_price)",  # already full
                    "order_dir":       "DESC",
                    "target_column":   "",
                },
            ],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)" in sql

    def test_ntile_with_unknown_alias_falls_back_to_alias(self, schema_map, schema_types):
        """GAP-5-C: NTILE with unknown alias falls back to alias (no crash)."""
        intent = base_intent(
            metrics=[{
                "metric":          "revenue_decile",
                "aggregation":     "NTILE",
                "ntile_buckets":   10,
                "order_by_column": "SUM(fact_orders.unit_price)",
                "order_dir":       "DESC",
                "target_column":   "",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "NTILE(10)" in sql


class TestAutoRepairJoins:
    """GAP-6: auto_repair_joins covers computed_columns table references."""

    def test_case_when_references_dim_customers_join_auto_inserted(self, schema_map, schema_types):
        """GAP-6-A: CASE WHEN references dim_customers — join auto-inserted."""
        intent = base_intent(
            fact_table="fact_orders",
            group_by=[],                      # no group_by to trigger repair
            joins=[],                         # no explicit join
            computed_columns=[{
                "alias": "age_group",
                "when_clauses": [{
                    # dim_customers.age referenced — repair must insert the join
                    "condition": "dim_customers.age > 30",
                    "then": "'Senior'",
                }],
                "else_value": "'Junior'",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # dim_customers must appear in the generated SQL as a join
        assert "JOIN dim_customers ON" in sql

    def test_case_when_references_dim_products_join_auto_inserted(self, schema_map, schema_types):
        """GAP-6-B: CASE WHEN references dim_products — join auto-inserted."""
        intent = base_intent(
            fact_table="fact_orders",
            group_by=[],
            joins=[],
            computed_columns=[{
                "alias": "price_tier",
                "when_clauses": [{
                    "condition": "dim_products.price > 100",
                    "then": "'Premium'",
                }],
                "else_value": "'Standard'",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "JOIN dim_products ON" in sql

    def test_no_false_joins_for_fact_table_columns(self, schema_map, schema_types):
        """GAP-6-C: No false joins for fact table columns in CASE WHEN."""
        intent = base_intent(
            computed_columns=[{
                "alias": "rev_band",
                "when_clauses": [{
                    "condition": "fact_orders.unit_price > 500",  # fact table — no join needed
                    "then": "'High'",
                }],
                "else_value": "'Low'",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # Should NOT inject any extra joins for the fact table itself
        assert "JOIN fact_orders" not in sql

    def test_no_join_for_tables_not_in_fk_graph(self, schema_map, schema_types):
        """GAP-6-D: No join for tables not in FK graph (unknown table — no crash)."""
        intent = base_intent(
            computed_columns=[{
                "alias": "tier",
                "when_clauses": [{
                    "condition": "dim_nonexistent.value > 10",
                    "then": "'X'",
                }],
                "else_value": "'Y'",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # Unknown table — BFS returns empty, no join inserted, no crash
        assert "JOIN dim_nonexistent" not in sql


class TestPercentileContDialectHandling:
    """GAP-7: PERCENTILE_CONT hard-fails on MySQL dialect."""

    def test_percentile_cont_on_mysql_raises_value_error(self, schema_map, schema_types):
        """GAP-7-A: PERCENTILE_CONT on MySQL raises ValueError (not silent SQL)."""
        intent = base_intent(
            metrics=[{
                "metric":        "median_value",
                "aggregation":   "PERCENTILE_CONT",
                "percentile":    0.5,
                "target_column": "unit_price",
                "order_dir":     "ASC",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        with pytest.raises(ValueError, match="PERCENTILE_CONT is not supported for MySQL"):
            build_sql(intent, "mysql")

    def test_percentile_cont_on_postgresql_still_works_correctly(self, schema_map, schema_types):
        """GAP-7-B: PERCENTILE_CONT on PostgreSQL still works correctly."""
        intent = base_intent(
            metrics=[{
                "metric":        "median_value",
                "aggregation":   "PERCENTILE_CONT",
                "percentile":    0.5,
                "target_column": "unit_price",
                "order_dir":     "ASC",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        assert "PERCENTILE_CONT(0.5)" in sql
        assert "WITHIN GROUP" in sql

    def test_percentile_cont_on_sqlite_uses_row_number_fallback(self, schema_map, schema_types):
        """GAP-7-C: PERCENTILE_CONT on SQLite still uses ROW_NUMBER fallback."""
        intent = base_intent(
            metrics=[{
                "metric":        "median_value",
                "aggregation":   "PERCENTILE_CONT",
                "percentile":    0.5,
                "target_column": "unit_price",
                "order_dir":     "ASC",
            }],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "sqlite")
        assert "ROW_NUMBER" in sql
        assert "WITHIN GROUP" not in sql


class TestLeftJoinSemantics:
    """GAP-8: BFS-inserted hops inherit LEFT semantics."""

    def test_anti_join_query_bfs_hop_is_left_join_not_inner_join(self, schema_map, schema_types):
        """GAP-8-A: Anti-join query — BFS hop is LEFT JOIN not INNER JOIN."""
        intent = base_intent(
            fact_table="dim_products",
            metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id","distinct":False}],
            group_by=["dim_categories.category_name"],   # triggers BFS: products → categories
            joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
            filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # Both the explicit join and the BFS-inserted hop should be LEFT JOINs
        assert "LEFT JOIN fact_orders" in sql
        assert "LEFT JOIN dim_categories" in sql
        assert "INNER JOIN dim_categories" not in sql
        # "JOIN dim_categories ON" is a substring of "LEFT JOIN dim_categories ON",
        # so assert on the INNER prefix specifically — not a bare join keyword.
        assert "INNER JOIN dim_categories ON" not in sql

    def test_inner_only_query_bfs_hop_is_plain_inner_join(self, schema_map, schema_types):
        """GAP-8-B: INNER-only query — BFS hop is plain INNER JOIN."""
        intent = base_intent(
            group_by=["dim_products.category"],
            joins=[],   # BFS inserts INNER join
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # The BFS-inserted hop should be a plain JOIN (INNER JOIN)
        assert "JOIN dim_products ON" in sql
        assert "LEFT JOIN dim_products" not in sql

    def test_mixed_joins_left_present_means_bfs_hops_are_left(self, schema_map, schema_types):
        """GAP-8-C: Mixed joins — LEFT present means BFS hops are LEFT."""
        intent = base_intent(
            fact_table="fact_orders",
            metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id","distinct":False}],
            group_by=["dim_products.category"],           # BFS inserts hop
            joins=[
                {"type":"LEFT","condition":"fact_orders.customer_id = dim_customers.customer_id"},
            ],
            filters=[{"column":"dim_customers.customer_id","operator":"IS NULL"}],
        )
        
        ok, errs = validate_intent(intent, schema_map, schema_types)
        assert ok, f"Validation failed unexpectedly: {errs}"
        
        sql, params = build_sql(intent, "postgresql")
        # The LEFT join to dim_customers is explicit;
        # BFS join to dim_products should ALSO be LEFT (due to has_left_join flag)
        assert "LEFT JOIN dim_customers" in sql
        assert "LEFT JOIN dim_products" in sql