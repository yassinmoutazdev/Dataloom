# =============================================================================
# test_eval_harness.py  —  Dataloom v3.0
#
# Regression harness: validates generated SQL patterns for canonical
# questions across three test suites (Baseline, 4B, 4C).
# Zero database connection required — validates intent + SQL only.
# =============================================================================

import os
import sys
import types

# Bootstrap: mock LLM providers so we can import intent_parser without them.
# This keeps the harness runnable in CI environments without Ollama or OpenAI.
for mod in ("ollama", "openai"):
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from validator   import validate_intent, set_join_paths
from sql_builder import build_sql

# ── Shared schema ─────────────────────────────────────────────────────────────

# Six-table schema; spend_summary is used as a CTE target in 4C cases.
SCHEMA = {
    "fact_orders":    ["order_id", "customer_id", "product_id", "employee_id",
                       "unit_price", "quantity", "order_date", "ship_date",
                       "status", "region", "freight", "trip_ts"],
    "dim_customers":  ["customer_id", "name", "city", "country", "email", "age"],
    "dim_products":   ["product_id", "product_name", "category", "unit_price", "category_id"],
    "dim_employees":  ["employee_id", "name", "region", "department"],
    "dim_categories": ["category_id", "category_name"],
    "spend_summary":  ["customer_id", "total_spend", "last_order_date"],
}

_JOIN_PATHS = {
    "fact_orders": {
        "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
        "dim_products":  "fact_orders.product_id  = dim_products.product_id",
        "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
    },
    "dim_customers":  {"fact_orders": "fact_orders.customer_id = dim_customers.customer_id"},
    "dim_products":   {"fact_orders":  "fact_orders.product_id  = dim_products.product_id",
                       "dim_categories": "dim_products.category_id = dim_categories.category_id"},
    "dim_employees":  {"fact_orders": "fact_orders.employee_id = dim_employees.employee_id"},
    "dim_categories": {"dim_products": "dim_products.category_id = dim_categories.category_id"},
    # spend_summary has no pre-defined join paths — it is always materialized via CTE.
    "spend_summary":  {},
}


@pytest.fixture(autouse=True)
def setup_joins():
    """Register join paths before every test in this file."""
    set_join_paths(_JOIN_PATHS)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_case(tc: dict):
    """Run a single harness test case: validate intent, build SQL, assert patterns.

    Handles both positive cases (``expect_valid=True``, the default) and
    negative cases (``expect_valid=False``). For positive cases, asserts that
    every pattern in ``must_contain`` is present and every pattern in
    ``must_not_contain`` is absent from the generated SQL.

    Args:
        tc: Test-case dict with the following keys:
            ``id``              — short identifier shown in failure messages.
            ``description``     — human-readable description of what is tested.
            ``intent``          — the raw intent dict to validate and build.
            ``expect_valid``    — whether validation should succeed (default True).
            ``db``              — dialect to build SQL for (default "postgresql").
            ``must_contain``    — list of SQL substrings that must appear.
            ``must_not_contain``— list of SQL substrings that must not appear.

    Returns:
        The generated SQL string, or ``None`` when ``expect_valid`` is False.
    """
    intent       = dict(tc["intent"])
    expect_valid = tc.get("expect_valid", True)
    db           = tc.get("db", "postgresql")

    ok, errors = validate_intent(intent, SCHEMA)

    if not expect_valid:
        assert not ok, (
            f"[{tc['id']}] Expected validation to FAIL but it passed. "
            f"Description: {tc['description']}"
        )
        return None

    assert ok, (
        f"[{tc['id']}] Validation failed unexpectedly: {'; '.join(errors)}\n"
        f"Description: {tc['description']}"
    )

    sql_text, _ = build_sql(intent, db)
    sql_upper   = sql_text.upper()

    missing   = [p for p in tc.get("must_contain", [])     if p.upper() not in sql_upper]
    forbidden = [p for p in tc.get("must_not_contain", []) if p.upper() in sql_upper]

    assert not missing, (
        f"[{tc['id']}] SQL missing expected patterns: {missing}\nSQL:\n{sql_text}"
    )
    assert not forbidden, (
        f"[{tc['id']}] SQL contains forbidden patterns: {forbidden}\nSQL:\n{sql_text}"
    )
    return sql_text


# ── Test case definitions (mirrors the original ALL_SUITES structure) ─────────

# Baseline suite: core SQL features from Sprints 1–3.
SUITE_BASELINE = [
    {"id": "BL-01", "description": "Basic SUM + GROUP BY",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": ["dim_products.category"],
                "joins": ["fact_orders.product_id = dim_products.product_id"]},
     "must_contain": ["SUM(fact_orders.unit_price)", "GROUP BY", "dim_products.category"]},

    {"id": "BL-02", "description": "COUNT with filter",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "order_id"}],
                "fact_table": "fact_orders", "group_by": [],
                "filters": [{"column": "status", "operator": "=", "value": "shipped"}]},
     "must_contain": ["COUNT(", "WHERE", "fact_orders.status"]},

    {"id": "BL-03", "description": "ORDER BY DESC LIMIT",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.region"],
                "order_by": "rev", "order_dir": "DESC", "limit": 5},
     "must_contain": ["ORDER BY", "DESC", "LIMIT 5"]},

    {"id": "BL-04", "description": "LEFT JOIN anti-join IS NULL",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "order_id"}],
                "fact_table": "fact_orders", "group_by": [],
                "joins": [{"condition": "fact_orders.customer_id = dim_customers.customer_id", "type": "LEFT"}],
                "filters": [{"column": "dim_customers.customer_id", "operator": "IS NULL"}]},
     "must_contain": ["LEFT JOIN", "IS NULL"]},

    {"id": "BL-05", "description": "HAVING COUNT DISTINCT",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                "having": [{"aggregation": "COUNT", "target_column": "order_id",
                            "distinct": True, "operator": ">", "value": 3}]},
     "must_contain": ["HAVING", "COUNT(DISTINCT"]},
]

# Sprint 4B suite: window functions, scalar subqueries, and set operations.
SUITE_4B = [
    {"id": "4B-01", "description": "RANK() OVER with PARTITION BY",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders",
                "group_by": ["dim_products.category", "fact_orders.customer_id"],
                "joins": ["fact_orders.product_id = dim_products.product_id"],
                "window_functions": [{"alias": "rk", "function": "RANK",
                                      "partition_by": ["dim_products.category"],
                                      "order_by": "rev", "order_dir": "DESC"}]},
     "must_contain": ["RANK() OVER", "PARTITION BY dim_products.category"]},

    {"id": "4B-02", "description": "ROW_NUMBER no partition",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                "window_functions": [{"alias": "rn", "function": "ROW_NUMBER",
                                      "partition_by": [], "order_by": "rev", "order_dir": "DESC"}]},
     "must_contain": ["ROW_NUMBER() OVER"],
     "must_not_contain": ["PARTITION BY"]},

    {"id": "4B-03", "description": "LAG 3-arg form",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "window_functions": [{"alias": "prev", "function": "LAG", "target_column": "rev",
                                      "offset": 1, "default": 0, "partition_by": [],
                                      "order_by": "order_date", "order_dir": "ASC"}]},
     "must_contain": ["LAG(", "1, 0)"]},

    {"id": "4B-04", "description": "LEAD",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "window_functions": [{"alias": "nxt", "function": "LEAD", "target_column": "rev",
                                      "offset": 1, "default": "NULL", "partition_by": [],
                                      "order_by": "order_date", "order_dir": "ASC"}]},
     "must_contain": ["LEAD("]},

    {"id": "4B-05", "description": "AVG OVER rolling 3-row frame",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "window_functions": [{"alias": "roll", "function": "AVG", "target_column": "unit_price",
                                      "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
                                      "frame_spec": "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"}]},
     "must_contain": ["AVG(", "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"]},

    {"id": "4B-06", "description": "SUM OVER running total UNBOUNDED",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "window_functions": [{"alias": "run", "function": "SUM", "target_column": "unit_price",
                                      "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
                                      "frame_spec": "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"}]},
     "must_contain": ["ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"]},

    {"id": "4B-07", "description": "Scalar subquery % of total",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.region"],
                "scalar_subquery": {"alias": "pct", "numerator_metric": "rev", "multiply_by": 100,
                                    "denominator": {"aggregation": "SUM", "target_column": "unit_price",
                                                    "fact_table": "fact_orders"}}},
     "must_contain": ["NULLIF(", "100.0", "pct"]},

    {"id": "4B-08", "description": "INTERSECT set operation",
     "intent": {"metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "set_operation": {"operator": "INTERSECT",
                    "left":  {"metrics": [{"metric": "c", "aggregation": "COUNT", "target_column": "customer_id"}],
                              "fact_table": "fact_orders", "group_by": [],
                              "filters": [{"column": "status", "operator": "=", "value": "shipped"}]},
                    "right": {"metrics": [{"metric": "c", "aggregation": "COUNT", "target_column": "customer_id"}],
                              "fact_table": "fact_orders", "group_by": [],
                              "filters": [{"column": "region", "operator": "=", "value": "US"}]}}},
     "must_contain": ["INTERSECT"]},

    {"id": "4B-09", "description": "EXCEPT set operation",
     "intent": {"metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "set_operation": {"operator": "EXCEPT",
                    "left":  {"metrics": [{"metric": "c", "aggregation": "COUNT", "target_column": "customer_id"}],
                              "fact_table": "fact_orders", "group_by": []},
                    "right": {"metrics": [{"metric": "c", "aggregation": "COUNT", "target_column": "customer_id"}],
                              "fact_table": "fact_orders", "group_by": [],
                              "filters": [{"column": "status", "operator": "=", "value": "cancelled"}]}}},
     "must_contain": ["EXCEPT"]},

    {"id": "4B-10", "description": "Bad frame_spec → validation rejects",
     "intent": {"metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "window_functions": [{"alias": "x", "function": "SUM", "target_column": "unit_price",
                                      "partition_by": [], "order_by": "order_date", "order_dir": "ASC",
                                      "frame_spec": "INVALID FRAME"}]},
     "must_contain": [], "expect_valid": False},
]

# Sprint 4C suite: CTEs, correlated subqueries, EXTRACT, and interval filters.
SUITE_4C = [
    {"id": "4C-01", "description": "CTE WITH block",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "customer_id"}],
                "fact_table": "spend_summary", "group_by": [],
                "ctes": [{"name": "spend_summary", "intent": {
                    "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                    "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                }}]},
     "must_contain": ["WITH spend_summary AS", "SUM(fact_orders.unit_price)"]},

    {"id": "4C-02", "description": "CTE LIMIT stripped from sub-intent",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "customer_id"}],
                "fact_table": "spend_summary", "group_by": [],
                "ctes": [{"name": "spend_summary", "intent": {
                    "metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                    "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"], "limit": 10,
                }}]},
     "must_contain": ["WITH spend_summary AS"]},

    {"id": "4C-03", "description": "Duplicate CTE name → rejected",
     "intent": {"metrics": [{"metric": "r", "aggregation": "COUNT", "target_column": "order_id"}],
                "fact_table": "fact_orders", "group_by": [],
                "ctes": [
                    {"name": "dup", "intent": {"metrics": [{"metric": "r", "aggregation": "COUNT", "target_column": "order_id"}],
                                               "fact_table": "fact_orders", "group_by": []}},
                    {"name": "dup", "intent": {"metrics": [{"metric": "r", "aggregation": "COUNT", "target_column": "order_id"}],
                                               "fact_table": "fact_orders", "group_by": []}},
                ]},
     "must_contain": [], "expect_valid": False},

    {"id": "4C-04", "description": "Correlated subquery price > category avg",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "product_id"}],
                "fact_table": "dim_products", "group_by": [],
                "correlated_filter": {"column": "dim_products.unit_price", "operator": ">",
                    "subquery": {"aggregation": "AVG", "target_column": "unit_price",
                                 "fact_table": "dim_products", "where_col": "category",
                                 "outer_ref": "dim_products.category"}}},
     "must_contain": ["(SELECT AVG(", "WHERE", "dim_products.category"]},

    {"id": "4C-05", "description": "Correlated: missing outer_ref → rejected",
     "intent": {"metrics": [{"metric": "r", "aggregation": "COUNT", "target_column": "product_id"}],
                "fact_table": "dim_products", "group_by": [],
                "correlated_filter": {"column": "dim_products.unit_price", "operator": ">",
                    "subquery": {"aggregation": "AVG", "target_column": "unit_price", "where_col": "category"}}},
     "must_contain": [], "expect_valid": False},

    {"id": "4C-06", "description": "HAVING COUNT DISTINCT EXTRACT (X5)",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "order_id"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                "having": [{"aggregation": "COUNT",
                            "target_column": "EXTRACT(month FROM fact_orders.order_date)",
                            "distinct": True, "operator": "=", "value": 12}]},
     "must_contain": ["COUNT(DISTINCT EXTRACT", "HAVING"]},

    {"id": "4C-07", "description": "HAVING EXTRACT injection blocked",
     "intent": {"metrics": [{"metric": "cnt", "aggregation": "COUNT", "target_column": "order_id"}],
                "fact_table": "fact_orders", "group_by": ["fact_orders.customer_id"],
                "having": [{"aggregation": "COUNT",
                            "target_column": "EXTRACT(month FROM order_date); DROP TABLE users",
                            "distinct": True, "operator": "=", "value": 12}]},
     "must_contain": [], "expect_valid": False},

    {"id": "4C-08", "description": "Scalar subquery alias injection blocked",
     "intent": {"metrics": [{"metric": "r", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "scalar_subquery": {"alias": "pct; DROP TABLE users", "numerator_metric": "r",
                                    "multiply_by": 100,
                                    "denominator": {"aggregation": "SUM", "target_column": "unit_price"}}},
     "must_contain": [], "expect_valid": False},

    {"id": "4C-09", "description": "Correlated: unqualified outer_ref → rejected",
     "intent": {"metrics": [{"metric": "r", "aggregation": "COUNT", "target_column": "product_id"}],
                "fact_table": "dim_products", "group_by": [],
                "correlated_filter": {"column": "dim_products.unit_price", "operator": ">",
                    "subquery": {"aggregation": "AVG", "target_column": "unit_price",
                                 "fact_table": "dim_products", "where_col": "category",
                                 # outer_ref must be table-qualified to prevent ambiguity
                                 "outer_ref": "category"}}},
     "must_contain": [], "expect_valid": False},

    {"id": "4C-10", "description": "Scalar subquery multiply_by float OK",
     "intent": {"metrics": [{"metric": "rev", "aggregation": "SUM", "target_column": "unit_price"}],
                "fact_table": "fact_orders", "group_by": [],
                "scalar_subquery": {"alias": "pct", "numerator_metric": "rev", "multiply_by": 100.0,
                                    "denominator": {"aggregation": "SUM", "target_column": "unit_price",
                                                    "fact_table": "fact_orders"}}},
     "must_contain": ["100.0", "NULLIF("]},
]


# ── Parametrized test functions ───────────────────────────────────────────────

def _make_test_id(tc: dict) -> str:
    """Format a pytest test ID from a test-case dict."""
    return f"{tc['id']}: {tc['description']}"


@pytest.mark.parametrize("tc", SUITE_BASELINE, ids=[_make_test_id(t) for t in SUITE_BASELINE])
def test_baseline(tc):
    """Run all baseline (Sprint 1–3) regression cases."""
    _run_case(tc)


@pytest.mark.parametrize("tc", SUITE_4B, ids=[_make_test_id(t) for t in SUITE_4B])
def test_sprint4b(tc):
    """Run all Sprint 4B (window functions, scalar subqueries, set ops) cases."""
    _run_case(tc)


@pytest.mark.parametrize("tc", SUITE_4C, ids=[_make_test_id(t) for t in SUITE_4C])
def test_sprint4c(tc):
    """Run all Sprint 4C (CTEs, correlated subqueries, EXTRACT, intervals) cases."""
    _run_case(tc)
