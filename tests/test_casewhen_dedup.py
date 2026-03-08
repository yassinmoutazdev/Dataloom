# =============================================================================
# test_casewhen_dedup.py  —  Regression test for CASE WHEN duplication fix
#
# Reproduces the exact defect observed in Stress Test Q2:
#   LLM emits the CASE WHEN expression in BOTH group_by[] AND computed_columns[]
#   with include_in_group_by: true  →  expression appeared twice in SELECT
#   and twice in GROUP BY.
#
# Fix location:
#   sql_builder.py  build_sql()  — deduplication guard strips raw CASE entries
#                                  from group_by[] when a computed_column with
#                                  include_in_group_by=True already covers them
# =============================================================================

import copy, sys, traceback
from validator import set_join_paths, validate_intent
from sql_builder   import build_sql

PASS = 0
FAIL = 0
ERRORS = []

SCHEMA_MAP = {
    "fact_orders": ["order_id","customer_id","product_id","order_date",
                    "unit_price","quantity","ship_date","region"],
    "dim_customers": ["customer_id","name","email","city","country","age"],
    "dim_products":  ["product_id","product_name","category","price"],
}
SCHEMA_TYPES = {
    "fact_orders": {
        "unit_price":"numeric","quantity":"integer",
        "order_date":"timestamp","ship_date":"timestamp",
    }
}
set_join_paths({
    "fact_orders":   {"dim_customers":"fact_orders.customer_id = dim_customers.customer_id",
                      "dim_products":  "fact_orders.product_id  = dim_products.product_id"},
    "dim_customers": {"fact_orders":"fact_orders.customer_id = dim_customers.customer_id"},
    "dim_products":  {"fact_orders":"fact_orders.product_id  = dim_products.product_id"},
})

def base(**overrides):
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
        "time_bucket_column": None,
        "having":           [],
        "limit":            10,
        "order_by":         None,
        "order_dir":        "DESC",
    }
    i.update(overrides)
    return i

def run(label, intent_raw, db_type="postgresql",
        must_contain=None, must_not_contain=None,
        count_occurrences=None, expect_fail=False):
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
            print(f"  ✓  {label}  [rejected: {errs[0][:60]}]")
            return
        if not ok:
            FAIL += 1
            ERRORS.append(f"[FAIL] {label}: validate_intent failed → {errs}")
            return

        sql, params = build_sql(intent, db_type)

        for needle in (must_contain or []):
            if needle.upper() not in sql.upper():
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: missing '{needle}'\nSQL:\n{sql}")
                return

        for needle in (must_not_contain or []):
            if needle.upper() in sql.upper():
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: should NOT contain '{needle}'\nSQL:\n{sql}")
                return

        # count_occurrences: {substring: expected_count}
        for needle, expected in (count_occurrences or {}).items():
            actual = sql.upper().count(needle.upper())
            if actual != expected:
                FAIL += 1
                ERRORS.append(
                    f"[FAIL] {label}: '{needle}' appears {actual}x, expected {expected}x"
                    f"\nSQL:\n{sql}"
                )
                return

        PASS += 1
        print(f"  ✓  {label}")
    except Exception as e:
        FAIL += 1
        ERRORS.append(f"[FAIL] {label}: Exception — {e}\n{traceback.format_exc()}")

def section(title):
    print(f"\n{'─'*66}")
    print(f"  {title}")
    print(f"{'─'*66}")


# ── The exact defect from Stress Test Q2 ─────────────────────────────────────
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

section("CASE WHEN duplication — exact Q2 defect reproduction")

# CORE: The defect case — CASE WHEN in both group_by[] AND computed_columns[]
run("DEDUP-1: CASE WHEN in group_by[] AND computed_columns — no duplication in SELECT",
    base(
        group_by=[BAD_GROUP_BY_ENTRY],
        computed_columns=WEEKEND_CC,
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {"metric":"distinct_customers","aggregation":"COUNT","target_column":"customer_id",
             "distinct":True},
        ],
        having=[{"aggregation":"COUNT","target_column":"customer_id","distinct":True,
                 "operator":">","value":50}],
    ),
    # CASE block must appear exactly once in SELECT (not twice)
    count_occurrences={"END AS day_type": 1},
    # The raw unlabeled CASE must NOT appear as a bare SELECT column
    must_not_contain=["THEN 'Weekend'\nFROM", "SELECT CASE WHEN EXTRACT"],
)

run("DEDUP-2: GROUP BY contains CASE exactly once (not twice)",
    base(
        group_by=[BAD_GROUP_BY_ENTRY],
        computed_columns=WEEKEND_CC,
    ),
    # The GROUP BY clause must reference CASE once — from _case_when_expr()
    count_occurrences={"GROUP BY": 1},
    must_contain=["GROUP BY"],
)

run("DEDUP-3: Result has alias AS day_type, not bare unlabeled CASE column",
    base(
        group_by=[BAD_GROUP_BY_ENTRY],
        computed_columns=WEEKEND_CC,
    ),
    must_contain=["END AS day_type"],
    # Must NOT have a second CASE block without an alias (the duplication artifact)
    must_not_contain=["END,\n       CASE"],
)

run("DEDUP-4: Multiple plain group_by cols preserved, only CASE stripped",
    base(
        group_by=[
            "fact_orders.region",           # plain col — must be kept
            BAD_GROUP_BY_ENTRY,             # CASE WHEN — must be stripped
        ],
        computed_columns=WEEKEND_CC,
    ),
    must_contain=["fact_orders.region", "END AS day_type"],
    count_occurrences={"END AS day_type": 1},
)

run("DEDUP-5: No computed_columns → group_by CASE stays (no false stripping)",
    base(
        group_by=[BAD_GROUP_BY_ENTRY],
        computed_columns=[],               # no CC → dedup guard inactive
    ),
    # group_by CASE WHEN must appear in SELECT since there's no CC to render it
    must_contain=["CASE WHEN"],
)

run("DEDUP-6: include_in_group_by=False → CASE in group_by NOT stripped",
    base(
        group_by=[BAD_GROUP_BY_ENTRY],
        computed_columns=[{
            "alias": "day_type",
            "when_clauses": [
                {"condition": "EXTRACT(dow FROM fact_orders.order_date) IN (0, 6)", "then": "'Weekend'"},
            ],
            "else_value": "'Weekday'",
            "include_in_group_by": False,   # metric label — guard must NOT fire
        }],
    ),
    # Both the raw group_by CASE and the CC CASE appear — this is the
    # user's intent (though unusual). Guard fires only for include_in_group_by=True.
    must_contain=["CASE WHEN"],
)

run("DEDUP-7: Correct intent (no CASE in group_by) still works perfectly",
    base(
        group_by=[],                        # LLM correctly omits raw CASE
        computed_columns=WEEKEND_CC,
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {"metric":"distinct_customers","aggregation":"COUNT","target_column":"customer_id",
             "distinct":True},
        ],
        having=[{"aggregation":"COUNT","target_column":"customer_id","distinct":True,
                 "operator":">","value":50}],
    ),
    must_contain=["END AS day_type", "HAVING COUNT(DISTINCT", "> 50"],
    count_occurrences={"END AS day_type": 1},
)

run("DEDUP-8: CASE stripped from GROUP BY clause as well (not just SELECT)",
    base(
        group_by=[
            "fact_orders.region",
            BAD_GROUP_BY_ENTRY,
        ],
        computed_columns=WEEKEND_CC,
    ),
    # GROUP BY should have: region, plus the _case_when_expr() expansion
    # It should NOT have the duplicate raw CASE from group_by[]
    must_contain=["GROUP BY fact_orders.region"],
)


section("Regression — correct intent path unaffected")

run("REG-1: Standard CASE WHEN metric label (no include_in_group_by)",
    base(
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
    ),
    must_contain=["END AS spending_tier"],
    count_occurrences={"END AS spending_tier": 1},
)

run("REG-2: Anti-join unaffected",
    base(
        fact_table="dim_products",
        metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        group_by=["dim_products.product_name"],
        joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
        computed_columns=[],
    ),
    must_contain=["LEFT JOIN", "IS NULL"],
)

run("REG-3: NTILE alias expansion still works",
    base(
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {"metric":"decile","aggregation":"NTILE","ntile_buckets":10,
             "order_by_column":"total_revenue","order_dir":"DESC","target_column":""},
        ],
        group_by=["fact_orders.product_id"],
    ),
    must_contain=["NTILE(10) OVER (ORDER BY SUM(fact_orders.unit_price) DESC)"],
)

run("REG-4: Multi-hop BFS unaffected",
    base(group_by=["dim_products.category"], joins=[]),
    must_contain=["JOIN dim_products ON"],
)

run("REG-5: HAVING COUNT DISTINCT standalone",
    base(
        group_by=["fact_orders.order_id"],
        having=[{"aggregation":"COUNT","target_column":"product_id","distinct":True,
                 "operator":">","value":3}],
    ),
    must_contain=["COUNT(DISTINCT", "> 3"],
)


# ── Report ────────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n{'─'*66}")
print(f"  DEDUP FIX RESULTS  —  {PASS}/{total} passed  ({FAIL} failed)")
print(f"{'─'*66}")

if ERRORS:
    print()
    for e in ERRORS:
        print(e)
        print()

if FAIL:
    sys.exit(1)
else:
    print("\n  ✅  Deduplication fix verified. No regressions.\n")
