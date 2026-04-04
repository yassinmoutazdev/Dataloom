"""
Tests for CASE WHEN deduplication functionality.

Reproduces the exact defect observed in Stress Test Q2:
  LLM emits the CASE WHEN expression in BOTH group_by[] AND computed_columns[]
  with include_in_group_by: true  →  expression appeared twice in SELECT
  and twice in GROUP BY.

Fix location:
  sql_builder.py  build_sql()  — deduplication guard strips raw CASE entries
                                 from group_by[] when a computed_column with
                                 include_in_group_by=True already covers them
"""
import copy
import pytest
from validator import validate_intent
from sql_builder import build_sql


@pytest.fixture(scope="module")
def schema_map():
    """Sample schema map for testing CASE WHEN deduplication."""
    return {
        "fact_orders": ["order_id","customer_id","product_id","order_date",
                        "unit_price","quantity","ship_date","region"],
        "dim_customers": ["customer_id","name","email","city","country","age"],
        "dim_products":  ["product_id","product_name","category","price"],
    }


@pytest.fixture(scope="module")
def schema_types():
    """Sample schema types for testing CASE WHEN deduplication."""
    return {
        "fact_orders": {
            "unit_price":"numeric","quantity":"integer",
            "order_date":"timestamp","ship_date":"timestamp",
        }
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
        "time_bucket_column": None,
        "having":           [],
        "limit":            10,
        "order_by":         None,
        "order_dir":        "DESC",
    }
    intent.update(overrides)
    return intent


def run_test_case(intent, schema_map, schema_types, db_type="postgresql",
                  must_contain=None, must_not_contain=None, count_occurrences=None,
                  expect_fail=False):
    """Helper function to run a test case."""
    try:
        ok, errs = validate_intent(intent, schema_map, schema_types)
        if expect_fail:
            if ok:
                pytest.fail(f"Expected validation failure, got success")
            return
        if not ok:
            pytest.fail(f"validate_intent failed: {'; '.join(errs)}")

        sql, params = build_sql(intent, db_type)

        for needle in (must_contain or []):
            if needle.upper() not in sql.upper():
                pytest.fail(f"Missing '{needle}' in SQL:\n{sql}")

        for needle in (must_not_contain or []):
            if needle.upper() in sql.upper():
                pytest.fail(f"Should NOT contain '{needle}' in SQL:\n{sql}")

        # count_occurrences: {substring: expected_count}
        for needle, expected in (count_occurrences or {}).items():
            actual = sql.upper().count(needle.upper())
            if actual != expected:
                pytest.fail(
                    f"'{needle}' appears {actual}x, expected {expected}x\nSQL:\n{sql}"
                )
                
    except Exception as e:
        pytest.fail(f"Exception occurred: {e}")


# The exact defect from Stress Test Q2
WEEKEND_CC = [{
    "alias": "day_type",
    "when_clauses": [
        {"condition": "EXTRACT(dow FROM fact_orders.order_date) IN (0, 6)", "then": "'Weekend'"},
    ],
    "else_value": "'Weekday'",
    "include_in_group_by": True,
}]

# Raw CASE WHEN string the LLM incorrectly put into group_by[]
BAD_GROUP_BY_ENTRY = (
    "CASE WHEN EXTRACT(dow FROM fact_orders.order_date) IN (0, 6) "
    "THEN 'Weekend' ELSE 'Weekday' END"
)


class TestCaseWhenDuplication:
    """Test cases for CASE WHEN duplication fix."""

    def test_case_when_in_both_group_by_and_computed_columns(self, schema_map, schema_types):
        """DEDUP-1: CASE WHEN in group_by[] AND computed_columns — no duplication in SELECT."""
        intent = base_intent(
            group_by=[BAD_GROUP_BY_ENTRY],
            computed_columns=WEEKEND_CC,
            metrics=[
                {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
                {"metric":"distinct_customers","aggregation":"COUNT","target_column":"customer_id",
                 "distinct":True},
            ],
            having=[{"aggregation":"COUNT","target_column":"customer_id","distinct":True,
                     "operator":">","value":50}],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            count_occurrences={"END AS day_type": 1},
            must_not_contain=["THEN 'Weekend'\nFROM", "SELECT CASE WHEN EXTRACT"],
        )

    def test_group_by_contains_case_exactly_once(self, schema_map, schema_types):
        """DEDUP-2: GROUP BY contains CASE exactly once (not twice)."""
        intent = base_intent(
            group_by=[BAD_GROUP_BY_ENTRY],
            computed_columns=WEEKEND_CC,
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            count_occurrences={"GROUP BY": 1},
            must_contain=["GROUP BY"],
        )

    def test_result_has_alias_not_bare_case(self, schema_map, schema_types):
        """DEDUP-3: Result has alias AS day_type, not bare unlabeled CASE column."""
        intent = base_intent(
            group_by=[BAD_GROUP_BY_ENTRY],
            computed_columns=WEEKEND_CC,
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["END AS day_type"],
            must_not_contain=["END,\n       CASE"],
        )

    def test_multiple_plain_group_by_cols_preserved(self, schema_map, schema_types):
        """DEDUP-4: Multiple plain group_by cols preserved, only CASE stripped."""
        intent = base_intent(
            group_by=[
                "fact_orders.region",
                BAD_GROUP_BY_ENTRY,
            ],
            computed_columns=WEEKEND_CC,
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["fact_orders.region", "END AS day_type"],
            count_occurrences={"END AS day_type": 1},
        )

    def test_no_computed_columns_group_by_case_stays(self, schema_map, schema_types):
        """DEDUP-5: No computed_columns → group_by CASE stays (no false stripping)."""
        intent = base_intent(
            group_by=[BAD_GROUP_BY_ENTRY],
            computed_columns=[],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["CASE WHEN"],
        )

    def test_include_in_group_by_false_not_stripped(self, schema_map, schema_types):
        """DEDUP-6: include_in_group_by=False → CASE in group_by NOT stripped."""
        intent = base_intent(
            group_by=[BAD_GROUP_BY_ENTRY],
            computed_columns=[{
                "alias": "day_type",
                "when_clauses": [
                    {"condition": "EXTRACT(dow FROM fact_orders.order_date) IN (0, 6)", "then": "'Weekend'"},
                ],
                "else_value": "'Weekday'",
                "include_in_group_by": False,
            }],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["CASE WHEN"],
        )

    def test_correct_intent_no_case_in_group_by(self, schema_map, schema_types):
        """DEDUP-7: Correct intent (no CASE in group_by) still works perfectly."""
        intent = base_intent(
            group_by=[],
            computed_columns=WEEKEND_CC,
            metrics=[
                {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
                {"metric":"distinct_customers","aggregation":"COUNT","target_column":"customer_id",
                 "distinct":True},
            ],
            having=[{"aggregation":"COUNT","target_column":"customer_id","distinct":True,
                     "operator":">","value":50}],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["END AS day_type", "HAVING COUNT(DISTINCT", "> 50"],
            count_occurrences={"END AS day_type": 1},
        )

    def test_case_stripped_from_group_by_clause(self, schema_map, schema_types):
        """DEDUP-8: CASE stripped from GROUP BY clause as well (not just SELECT)."""
        intent = base_intent(
            group_by=[
                "fact_orders.region",
                BAD_GROUP_BY_ENTRY,
            ],
            computed_columns=WEEKEND_CC,
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["GROUP BY fact_orders.region"],
        )


class TestRegressionCorrectIntentUnaffected:
    """Regression tests to ensure correct intent path remains unaffected."""

    def test_standard_case_when_metric_label(self, schema_map, schema_types):
        """REG-1: Standard CASE WHEN metric label (no include_in_group_by)."""
        intent = base_intent(
            group_by=["fact_orders.customer_id"],
            computed_columns=[{
                "alias": "spending_tier",
                "when_clauses": [
                    {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                    {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'"},
                ],
                "else_value": "'Low'",
                "include_in_group_by": False,
            }],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["END AS spending_tier"],
            count_occurrences={"END AS spending_tier": 1},
        )

    def test_anti_join_unaffected(self, schema_map, schema_types):
        """REG-2: Anti-join unaffected."""
        intent = base_intent(
            fact_table="dim_products",
            metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id","distinct":False}],
            group_by=["dim_products.product_name"],
            joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
            filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
            computed_columns=[],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["LEFT JOIN", "IS NULL"],
        )

    def test_ntile_alias_expansion_still_works(self, schema_map, schema_types):
        """REG-3: NTILE alias expansion still works."""
        intent = base_intent(
            metrics=[
                {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
                {"metric":"decile","aggregation":"NTILE","ntile_buckets":10,
                 "order_by_column":"total_revenue","order_dir":"DESC","target_column":""},
            ],
            group_by=["fact_orders.product_id"],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)"],
        )

    def test_multi_hop_bfs_unaffected(self, schema_map, schema_types):
        """REG-4: Multi-hop BFS unaffected."""
        intent = base_intent(
            group_by=["dim_products.category"],
            joins=[]
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["JOIN dim_products ON"],
        )

    def test_having_count_distinct_standalone(self, schema_map, schema_types):
        """REG-5: HAVING COUNT DISTINCT standalone."""
        intent = base_intent(
            group_by=["fact_orders.order_id"],
            having=[{"aggregation":"COUNT","target_column":"product_id","distinct":True,
                     "operator":">","value":3}],
        )
        
        run_test_case(
            intent, schema_map, schema_types,
            must_contain=["COUNT(DISTINCT", "> 3"],
        )