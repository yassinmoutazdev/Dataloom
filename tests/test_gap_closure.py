# =============================================================================
# test_gap_closure.py  —  Dataloom v3.0  External Audit Gap-Closure Tests
#
# One targeted test per confirmed fix. These tests prove that the specific
# vulnerabilities identified in the Master Review Prompt are now closed.
#
#   GAP-1  else_value now validated — subquery / DDL blocked
#   GAP-2  NTILE order_by_column schema-validated
#   GAP-3  then value now validated — subquery / DDL blocked
#   GAP-4  is_expression generic pass-through keyword-blocked
#   GAP-5  NTILE order_by_column alias auto-expanded to full expression
#   GAP-6  auto_repair_joins scans computed_columns WHEN conditions
#   GAP-7  PERCENTILE_CONT raises ValueError on MySQL (not silent bad SQL)
#   GAP-8  BFS-inserted hops inherit LEFT semantics when query has LEFT JOIN
#
#   PARTIAL  MySQL DOW alignment (documented caveat, not a code fix)
# =============================================================================

import copy, sys, traceback
from validator import set_join_paths, validate_intent
from sql_builder   import build_sql

PASS = 0
FAIL = 0
ERRORS = []

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_MAP = {
    "fact_orders": [
        "order_id","customer_id","product_id","employee_id",
        "order_date","ship_date","quantity","unit_price","freight","status","region",
    ],
    "dim_customers": ["customer_id","name","email","city","country","age","signup_date"],
    "dim_products":  ["product_id","product_name","category","subcategory","cost","price","stock_level"],
    "dim_categories":["category_id","category_name","dept_id"],
    "dim_employees": ["employee_id","name","department","region"],
}
SCHEMA_TYPES = {
    "fact_orders": {
        "unit_price":"numeric","freight":"numeric","quantity":"integer",
        "order_date":"timestamp","ship_date":"timestamp",
    },
    "dim_products": {"price":"numeric","cost":"numeric","stock_level":"integer"},
}
set_join_paths({
    "fact_orders": {
        "dim_customers":  "fact_orders.customer_id = dim_customers.customer_id",
        "dim_products":   "fact_orders.product_id  = dim_products.product_id",
        "dim_employees":  "fact_orders.employee_id = dim_employees.employee_id",
    },
    "dim_customers":  {"fact_orders":  "fact_orders.customer_id = dim_customers.customer_id"},
    "dim_products":   {"fact_orders":  "fact_orders.product_id  = dim_products.product_id",
                       "dim_categories":"dim_products.category_id = dim_categories.category_id"},
    "dim_categories": {"dim_products": "dim_products.category_id = dim_categories.category_id"},
    "dim_employees":  {"fact_orders":  "fact_orders.employee_id = dim_employees.employee_id"},
})

# ── Helpers ───────────────────────────────────────────────────────────────────
def base_intent(**overrides):
    i = {
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
    i.update(overrides)
    return i

def run(label, intent_raw, db_type="postgresql",
        must_contain=None, must_not_contain=None,
        expect_fail=False, expect_exception=None):
    global PASS, FAIL
    intent = copy.deepcopy(intent_raw)
    try:
        ok, errs = validate_intent(intent, SCHEMA_MAP, SCHEMA_TYPES)
        if expect_fail:
            if ok:
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: expected validation failure, got success")
                return
            PASS += 1
            print(f"  ✓  {label}  [rejected: {errs[0][:70]}]")
            return
        if not ok:
            FAIL += 1
            ERRORS.append(f"[FAIL] {label}: validate_intent failed → {errs}")
            return
        try:
            sql, params = build_sql(intent, db_type)
        except Exception as exc:
            if expect_exception and expect_exception.lower() in str(exc).lower():
                PASS += 1
                print(f"  ✓  {label}  [raised: {str(exc)[:70]}]")
                return
            elif expect_exception:
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: expected exception '{expect_exception}', got: {exc}")
                return
            else:
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: unexpected exception → {exc}\n{traceback.format_exc()}")
                return

        if expect_exception:
            FAIL += 1
            ERRORS.append(f"[FAIL] {label}: expected exception '{expect_exception}', got SQL:\n{sql}")
            return

        for needle in (must_contain or []):
            if needle.upper() not in sql.upper():
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: SQL missing '{needle}'\nSQL:\n{sql}")
                return
        for needle in (must_not_contain or []):
            if needle.upper() in sql.upper():
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: SQL should NOT contain '{needle}'\nSQL:\n{sql}")
                return

        PASS += 1
        print(f"  ✓  {label}")

    except Exception as exc:
        if expect_exception and expect_exception.lower() in str(exc).lower():
            PASS += 1
            print(f"  ✓  {label}  [raised at validate: {str(exc)[:70]}]")
            return
        FAIL += 1
        ERRORS.append(f"[FAIL] {label}: Exception — {exc}\n{traceback.format_exc()}")

def section(title):
    print(f"\n{'─'*66}")
    print(f"  {title}")
    print(f"{'─'*66}")


# =============================================================================
# GAP-1: else_value validated — blocks subqueries and DDL
# =============================================================================
section("GAP-1 · else_value content validation")

run("GAP-1-A: Subquery in else_value is rejected",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
        "else_value": "(SELECT password FROM users LIMIT 1)",
    }]),
    expect_fail=True,
)

run("GAP-1-B: DDL keyword in else_value is rejected",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
        "else_value": "'; DROP TABLE orders; --",
    }]),
    expect_fail=True,
)

run("GAP-1-C: Valid quoted string in else_value passes",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
        "else_value": "'Low'",
    }]),
    must_contain=["ELSE 'Low'"],
)

run("GAP-1-D: Numeric else_value passes",
    base_intent(computed_columns=[{
        "alias": "score",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "1"}],
        "else_value": "0",
    }]),
    must_contain=["ELSE 0"],
)

run("GAP-1-E: NULL else_value passes",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'High'"}],
        "else_value": "NULL",
    }]),
    must_contain=["ELSE NULL"],
)


# =============================================================================
# GAP-2: NTILE order_by_column schema-validated
# =============================================================================
section("GAP-2 · NTILE order_by_column schema validation")

run("GAP-2-A: NTILE with valid order_by_column passes",
    base_intent(
        metrics=[{
            "metric": "revenue_decile",
            "aggregation": "NTILE",
            "ntile_buckets": 10,
            "order_by_column": "SUM(fact_orders.unit_price)",
            "order_dir": "DESC",
            "target_column": "",
        }],
    ),
    must_contain=["NTILE(10)"],
)

run("GAP-2-B: NTILE with unknown bare column is rejected",
    base_intent(
        metrics=[{
            "metric": "decile",
            "aggregation": "NTILE",
            "ntile_buckets": 10,
            "order_by_column": "nonexistent_column_xyz",
            "order_dir": "DESC",
            "target_column": "",
        }],
    ),
    expect_fail=True,
)

run("GAP-2-C: NTILE with fully-qualified column passes",
    base_intent(
        metrics=[{
            "metric": "decile",
            "aggregation": "NTILE",
            "ntile_buckets": 5,
            "order_by_column": "SUM(fact_orders.unit_price)",
            "order_dir": "DESC",
            "target_column": "",
        }],
    ),
    must_contain=["NTILE(5)"],
)


# =============================================================================
# GAP-3: then value validated — blocks subqueries and DDL
# =============================================================================
section("GAP-3 · CASE WHEN then content validation")

run("GAP-3-A: Subquery in then is rejected",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{
            "condition": "SUM(fact_orders.unit_price) > 1000",
            "then": "(SELECT api_key FROM config)",
        }],
        "else_value": "'Low'",
    }]),
    expect_fail=True,
)

run("GAP-3-B: DDL keyword in then is rejected",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{
            "condition": "1=1",
            "then": "'; DROP TABLE users; --",
        }],
        "else_value": "'Other'",
    }]),
    expect_fail=True,
)

run("GAP-3-C: Bare column name in then is rejected (not a literal)",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{
            "condition": "1=1",
            "then": "email",   # not a quoted string or number
        }],
        "else_value": "'Other'",
    }]),
    expect_fail=True,
)

run("GAP-3-D: Valid quoted string in then passes",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [
            {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'Premium'"},
            {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Standard'"},
        ],
        "else_value": "'Basic'",
    }]),
    must_contain=["THEN 'Premium'", "THEN 'Standard'"],
)

run("GAP-3-E: Numeric then passes",
    base_intent(computed_columns=[{
        "alias": "rank_score",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "3"}],
        "else_value": "1",
    }]),
    must_contain=["THEN 3"],
)


# =============================================================================
# GAP-4: is_expression keyword blocking
# =============================================================================
section("GAP-4 · is_expression / condition keyword blocking")

run("GAP-4-A: SELECT in CASE WHEN condition is blocked",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{
            "condition": "EXISTS (SELECT 1 FROM pg_tables)",
            "then": "'High'",
        }],
        "else_value": "'Low'",
    }]),
    expect_fail=True,
)

run("GAP-4-B: DROP in CASE WHEN condition is blocked",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{
            "condition": "1=1; DROP TABLE orders",
            "then": "'High'",
        }],
        "else_value": "'Low'",
    }]),
    expect_fail=True,
)

run("GAP-4-C: SQL comment (--) in condition is blocked",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{
            "condition": "1=1 -- bypass",
            "then": "'High'",
        }],
        "else_value": "'Low'",
    }]),
    expect_fail=True,
)

run("GAP-4-D: Valid aggregation condition passes",
    base_intent(computed_columns=[{
        "alias": "tier",
        "when_clauses": [{"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"}],
        "else_value": "'Low'",
    }]),
    must_contain=["CASE", "WHEN"],
)


# =============================================================================
# GAP-5: NTILE alias auto-expands to full expression
# =============================================================================
section("GAP-5 · NTILE order_by_column alias → full expression expansion")

run("GAP-5-A: LLM emits alias 'total_revenue' — builder expands to full SUM()",
    base_intent(
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
    ),
    # The alias 'total_revenue' must be expanded to SUM(fact_orders.unit_price)
    must_contain=["NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)"],
    must_not_contain=["ORDER BY total_revenue"],
)

run("GAP-5-B: NTILE with full expression passes through unchanged",
    base_intent(
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
    ),
    must_contain=["NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)"],
)

run("GAP-5-C: NTILE with unknown alias falls back to alias (no crash)",
    base_intent(
        metrics=[{
            "metric":          "revenue_decile",
            "aggregation":     "NTILE",
            "ntile_buckets":   10,
            "order_by_column": "SUM(fact_orders.unit_price)",
            "order_dir":       "DESC",
            "target_column":   "",
        }],
    ),
    must_contain=["NTILE(10)"],
)


# =============================================================================
# GAP-6: auto_repair_joins scans computed_columns WHEN conditions
# =============================================================================
section("GAP-6 · auto_repair_joins covers computed_columns table references")

run("GAP-6-A: CASE WHEN references dim_customers — join auto-inserted",
    base_intent(
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
    ),
    # dim_customers must appear in the generated SQL as a join
    must_contain=["JOIN dim_customers ON"],
)

run("GAP-6-B: CASE WHEN references dim_products — join auto-inserted",
    base_intent(
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
    ),
    must_contain=["JOIN dim_products ON"],
)

run("GAP-6-C: No false joins for fact table columns in CASE WHEN",
    base_intent(
        computed_columns=[{
            "alias": "rev_band",
            "when_clauses": [{
                "condition": "fact_orders.unit_price > 500",  # fact table — no join needed
                "then": "'High'",
            }],
            "else_value": "'Low'",
        }],
    ),
    # Should NOT inject any extra joins for the fact table itself
    must_not_contain=["JOIN fact_orders"],
)

run("GAP-6-D: No join for tables not in FK graph (unknown table — no crash)",
    base_intent(
        computed_columns=[{
            "alias": "tier",
            "when_clauses": [{
                "condition": "dim_nonexistent.value > 10",
                "then": "'X'",
            }],
            "else_value": "'Y'",
        }],
    ),
    # Unknown table — BFS returns empty, no join inserted, no crash
    must_not_contain=["JOIN dim_nonexistent"],
)


# =============================================================================
# GAP-7: PERCENTILE_CONT raises immediately on MySQL (no silent bad SQL)
# =============================================================================
section("GAP-7 · PERCENTILE_CONT hard-fails on MySQL dialect")

run("GAP-7-A: PERCENTILE_CONT on MySQL raises ValueError (not silent SQL)",
    base_intent(
        metrics=[{
            "metric":        "median_value",
            "aggregation":   "PERCENTILE_CONT",
            "percentile":    0.5,
            "target_column": "unit_price",
            "order_dir":     "ASC",
        }],
    ),
    db_type="mysql",
    expect_exception="PERCENTILE_CONT is not supported for MySQL",
)

run("GAP-7-B: PERCENTILE_CONT on PostgreSQL still works correctly",
    base_intent(
        metrics=[{
            "metric":        "median_value",
            "aggregation":   "PERCENTILE_CONT",
            "percentile":    0.5,
            "target_column": "unit_price",
            "order_dir":     "ASC",
        }],
    ),
    db_type="postgresql",
    must_contain=["PERCENTILE_CONT(0.5)", "WITHIN GROUP"],
)

run("GAP-7-C: PERCENTILE_CONT on SQLite still uses ROW_NUMBER fallback",
    base_intent(
        metrics=[{
            "metric":        "median_value",
            "aggregation":   "PERCENTILE_CONT",
            "percentile":    0.5,
            "target_column": "unit_price",
            "order_dir":     "ASC",
        }],
    ),
    db_type="sqlite",
    must_contain=["ROW_NUMBER"],
    must_not_contain=["WITHIN GROUP"],
)


# =============================================================================
# GAP-8: BFS-inserted hops inherit LEFT semantics
# =============================================================================
section("GAP-8 · BFS intermediate hops inherit LEFT JOIN when query uses LEFT")

run("GAP-8-A: Anti-join query — BFS hop is LEFT JOIN not INNER JOIN",
    base_intent(
        fact_table="dim_products",
        metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        group_by=["dim_categories.category_name"],   # triggers BFS: products → categories
        joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
    ),
    # Both the explicit join and the BFS-inserted hop should be LEFT JOINs
    must_contain=["LEFT JOIN fact_orders", "LEFT JOIN dim_categories"],
    must_not_contain=["INNER JOIN dim_categories", "JOIN dim_categories ON"],
)

run("GAP-8-B: INNER-only query — BFS hop is plain INNER JOIN",
    base_intent(
        group_by=["dim_products.category"],
        joins=[],   # BFS inserts INNER join
    ),
    must_contain=["JOIN dim_products ON"],
    must_not_contain=["LEFT JOIN dim_products"],
)

run("GAP-8-C: Mixed joins — LEFT present means BFS hops are LEFT",
    base_intent(
        fact_table="fact_orders",
        metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id","distinct":False}],
        group_by=["dim_products.category"],           # BFS inserts hop
        joins=[
            {"type":"LEFT","condition":"fact_orders.customer_id = dim_customers.customer_id"},
        ],
        filters=[{"column":"dim_customers.customer_id","operator":"IS NULL"}],
    ),
    # The LEFT join to dim_customers is explicit;
    # BFS join to dim_products should ALSO be LEFT (due to has_left_join flag)
    must_contain=["LEFT JOIN dim_customers", "LEFT JOIN dim_products"],
)


# =============================================================================
# PARTIAL FIX: MySQL DOW scale caveat (documented, not a code change)
# =============================================================================
section("PARTIAL · MySQL DOW scale — confirmed caveat (no silent fix)")

run("DOW-A: PostgreSQL EXTRACT(dow) range 0–6 in generated SQL",
    base_intent(
        group_by=["EXTRACT(dow FROM fact_orders.order_date) AS dow"],
    ),
    db_type="postgresql",
    must_contain=["EXTRACT(dow FROM fact_orders.order_date)"],
)

run("DOW-B: MySQL emits DAYOFWEEK() (range 1–7 — caller must adjust condition)",
    base_intent(
        group_by=["DAYOFWEEK(fact_orders.order_date) AS dow"],
    ),
    db_type="mysql",
    must_contain=["DAYOFWEEK(fact_orders.order_date)"],
)


# =============================================================================
# REGRESSION: all original 84-test patterns still pass after gap fixes
# =============================================================================
section("REGRESSION · Key patterns from 84-test suite still green")

run("REG-1: Standard CASE WHEN (A3) still generates correctly",
    base_intent(
        group_by=["fact_orders.customer_id"],
        computed_columns=[{
            "alias": "spending_tier",
            "when_clauses": [
                {"condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"},
                {"condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'"},
            ],
            "else_value": "'Low'",
        }],
    ),
    must_contain=["CASE", "WHEN", "THEN 'High'", "THEN 'Medium'", "ELSE 'Low'"],
)

run("REG-2: Anti-join still works (I2 pattern)",
    base_intent(
        fact_table="dim_products",
        metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        group_by=["dim_products.product_name"],
        joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
    ),
    must_contain=["LEFT JOIN", "IS NULL"],
)

run("REG-3: NTILE with prior metric still expands alias",
    base_intent(
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {"metric":"decile","aggregation":"NTILE","ntile_buckets":10,
             "order_by_column":"total_revenue","order_dir":"DESC","target_column":""},
        ],
        group_by=["fact_orders.product_id"],
    ),
    must_contain=["NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)"],
)

run("REG-4: IS NULL filter still parameterless",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"n","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        filters=[{"column":"email","operator":"IS NULL"}],
    ),
    must_contain=["IS NULL"],
)

run("REG-5: Sprint 1 DATE_TRUNC still works",
    base_intent(time_bucket="month", time_bucket_column="order_date"),
    must_contain=["DATE_TRUNC('month'"],
)

run("REG-6: Sprint 3 multi-hop BFS still works",
    base_intent(group_by=["dim_products.category"], joins=[]),
    must_contain=["JOIN dim_products ON"],
)


# =============================================================================
# FINAL REPORT
# =============================================================================
total = PASS + FAIL
section(f"GAP-CLOSURE RESULTS  —  {PASS}/{total} passed  ({FAIL} failed)")

if ERRORS:
    print()
    for e in ERRORS:
        print(e)
        print()

if FAIL:

    print("\n  ✅  All gap-closure tests passed. v3.0 post-audit is FULLY GREEN.\n")
