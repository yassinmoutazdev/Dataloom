#!/usr/bin/env python3
# =============================================================================
# eval_harness.py  —  Dataloom v3.0
#
# Regression harness: runs canonical questions against the full pipeline
# and checks that generated SQL contains expected patterns.
# Zero database connection required — validates intent + SQL only.
#
# Usage:
#   python eval_harness.py                  # run all suites
#   python eval_harness.py --suite 4b       # run one suite
#   python eval_harness.py --verbose        # print SQL for every case
#   python eval_harness.py --fail-fast      # stop on first failure
#
# Exit code 0 = all pass, 1 = any failure.
# =============================================================================

import os, sys, json, time, argparse, types

# ── Bootstrap: mock LLM providers so we can import intent_parser ──────────────
for mod in ("ollama", "openai"):
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validator   import validate_intent, set_join_paths
from sql_builder import build_sql

# ── ANSI ──────────────────────────────────────────────────────────────────────
if sys.platform == "win32":
    os.system("color")
GREEN  = "\033[92m"; RED    = "\033[91m"; YELLOW = "\033[93m"
CYAN   = "\033[96m"; DIM    = "\033[2m";  BOLD   = "\033[1m"
RESET  = "\033[0m"
def c(t, *codes): return "".join(codes) + str(t) + RESET

# ── Shared schema fixture ─────────────────────────────────────────────────────
# Uses generic names that mirror the training file.
# Swap for your real Olist schema when running validate_olist.py instead.
SCHEMA = {
    "fact_orders":    ["order_id","customer_id","product_id","employee_id",
                       "unit_price","quantity","order_date","ship_date",
                       "status","region","freight","trip_ts"],
    "dim_customers":  ["customer_id","name","city","country","email","age"],
    "dim_products":   ["product_id","product_name","category","unit_price","category_id"],
    "dim_employees":  ["employee_id","name","region","department"],
    "dim_categories": ["category_id","category_name"],
    "spend_summary":  ["customer_id","total_spend","last_order_date"],
}

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

# ── Test case helpers ─────────────────────────────────────────────────────────

def case(id, description, intent, must_contain, must_not_contain=None,
         expect_valid=True, db="postgresql"):
    return {
        "id": id, "description": description, "intent": intent,
        "must_contain": must_contain or [],
        "must_not_contain": must_not_contain or [],
        "expect_valid": expect_valid, "db": db,
    }


# ═════════════════════════════════════════════════════════════════════════════
# TEST SUITES
# ═════════════════════════════════════════════════════════════════════════════

SUITE_BASELINE = [
    case("BL-01", "Basic SUM + GROUP BY",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":["dim_products.category"],
          "joins":["fact_orders.product_id = dim_products.product_id"]},
         ["SUM(fact_orders.unit_price)","GROUP BY","dim_products.category"]),

    case("BL-02", "COUNT with filter",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
          "fact_table":"fact_orders","group_by":[],
          "filters":[{"column":"status","operator":"=","value":"shipped"}]},
         ["COUNT(","WHERE","fact_orders.status"]),  # value goes into params, not SQL text

    case("BL-03", "ORDER BY DESC LIMIT",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":["fact_orders.region"],
          "order_by":"rev","order_dir":"DESC","limit":5},
         ["ORDER BY","DESC","LIMIT 5"]),

    case("BL-04", "LEFT JOIN anti-join IS NULL",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
          "fact_table":"fact_orders","group_by":[],
          "joins":[{"condition":"fact_orders.customer_id = dim_customers.customer_id","type":"LEFT"}],
          "filters":[{"column":"dim_customers.customer_id","operator":"IS NULL"}]},
         ["LEFT JOIN","IS NULL"]),

    case("BL-05", "HAVING COUNT DISTINCT",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":["fact_orders.customer_id"],
          "having":[{"aggregation":"COUNT","target_column":"order_id",
                     "distinct":True,"operator":">","value":3}]},
         ["HAVING","COUNT(DISTINCT"]),
]

SUITE_4B = [
    case("4B-01", "RANK() OVER with PARTITION BY",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":["dim_products.category","fact_orders.customer_id"],
          "joins":["fact_orders.product_id = dim_products.product_id"],
          "window_functions":[{"alias":"rk","function":"RANK",
                               "partition_by":["dim_products.category"],
                               "order_by":"rev","order_dir":"DESC"}]},
         ["RANK() OVER","PARTITION BY dim_products.category"]),

    case("4B-02", "ROW_NUMBER no partition",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":["fact_orders.customer_id"],
          "window_functions":[{"alias":"rn","function":"ROW_NUMBER",
                               "partition_by":[],"order_by":"rev","order_dir":"DESC"}]},
         ["ROW_NUMBER() OVER"],
         must_not_contain=["PARTITION BY"]),

    case("4B-03", "LAG 3-arg form",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "window_functions":[{"alias":"prev","function":"LAG","target_column":"rev",
                               "offset":1,"default":0,"partition_by":[],
                               "order_by":"order_date","order_dir":"ASC"}]},
         ["LAG(","1, 0)"]),

    case("4B-04", "LEAD",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "window_functions":[{"alias":"nxt","function":"LEAD","target_column":"rev",
                               "offset":1,"default":"NULL","partition_by":[],
                               "order_by":"order_date","order_dir":"ASC"}]},
         ["LEAD("]),

    case("4B-05", "AVG OVER rolling 3-row frame",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "window_functions":[{"alias":"roll","function":"AVG","target_column":"unit_price",
                               "partition_by":[],"order_by":"order_date","order_dir":"ASC",
                               "frame_spec":"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"}]},
         ["AVG(","ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"]),

    case("4B-06", "SUM OVER running total UNBOUNDED",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "window_functions":[{"alias":"run","function":"SUM","target_column":"unit_price",
                               "partition_by":[],"order_by":"order_date","order_dir":"ASC",
                               "frame_spec":"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"}]},
         ["ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"]),

    case("4B-07", "Scalar subquery % of total",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":["fact_orders.region"],
          "scalar_subquery":{"alias":"pct","numerator_metric":"rev","multiply_by":100,
                             "denominator":{"aggregation":"SUM","target_column":"unit_price",
                                            "fact_table":"fact_orders"}}},
         ["NULLIF(","100.0","pct"]),

    case("4B-08", "INTERSECT set operation",
         {"metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "set_operation":{"operator":"INTERSECT",
              "left": {"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                       "fact_table":"fact_orders","group_by":[],
                       "filters":[{"column":"status","operator":"=","value":"shipped"}]},
              "right":{"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                       "fact_table":"fact_orders","group_by":[],
                       "filters":[{"column":"region","operator":"=","value":"US"}]}}},
         ["INTERSECT"]),

    case("4B-09", "EXCEPT set operation",
         {"metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "set_operation":{"operator":"EXCEPT",
              "left": {"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                       "fact_table":"fact_orders","group_by":[]},
              "right":{"metrics":[{"metric":"c","aggregation":"COUNT","target_column":"customer_id"}],
                       "fact_table":"fact_orders","group_by":[],
                       "filters":[{"column":"status","operator":"=","value":"cancelled"}]}}},
         ["EXCEPT"]),

    case("4B-10", "Bad frame_spec → validation rejects",
         {"metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "window_functions":[{"alias":"x","function":"SUM","target_column":"unit_price",
                               "partition_by":[],"order_by":"order_date","order_dir":"ASC",
                               "frame_spec":"INVALID FRAME"}]},
         [], expect_valid=False),
]

SUITE_4C = [
    case("4C-01", "CTE WITH block",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
          "fact_table":"spend_summary","group_by":[],
          "ctes":[{"name":"spend_summary","intent":{
              "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
              "fact_table":"fact_orders","group_by":["fact_orders.customer_id"]
          }}]},
         ["WITH spend_summary AS","SUM(fact_orders.unit_price)"]),

    case("4C-02", "CTE LIMIT stripped from sub-intent",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"customer_id"}],
          "fact_table":"spend_summary","group_by":[],
          "ctes":[{"name":"spend_summary","intent":{
              "metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
              "fact_table":"fact_orders","group_by":["fact_orders.customer_id"],"limit":10,
          }}]},
         ["WITH spend_summary AS"]),

    case("4C-03", "Duplicate CTE name → rejected",
         {"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],
          "fact_table":"fact_orders","group_by":[],
          "ctes":[
              {"name":"dup","intent":{"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],
                                      "fact_table":"fact_orders","group_by":[]}},
              {"name":"dup","intent":{"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"order_id"}],
                                      "fact_table":"fact_orders","group_by":[]}},
          ]},
         [], expect_valid=False),

    case("4C-04", "Correlated subquery price > category avg",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id"}],
          "fact_table":"dim_products","group_by":[],
          "correlated_filter":{"column":"dim_products.unit_price","operator":">",
              "subquery":{"aggregation":"AVG","target_column":"unit_price",
                          "fact_table":"dim_products","where_col":"category",
                          "outer_ref":"dim_products.category"}}},
         ["(SELECT AVG(","WHERE","dim_products.category"]),

    case("4C-05", "Correlated: missing outer_ref → rejected",
         {"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"product_id"}],
          "fact_table":"dim_products","group_by":[],
          "correlated_filter":{"column":"dim_products.unit_price","operator":">",
              "subquery":{"aggregation":"AVG","target_column":"unit_price","where_col":"category"}}},
         [], expect_valid=False),

    case("4C-06", "HAVING COUNT DISTINCT EXTRACT (X5)",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
          "fact_table":"fact_orders","group_by":["fact_orders.customer_id"],
          "having":[{"aggregation":"COUNT",
                     "target_column":"EXTRACT(month FROM fact_orders.order_date)",
                     "distinct":True,"operator":"=","value":12}]},
         ["COUNT(DISTINCT EXTRACT","HAVING"]),

    case("4C-07", "HAVING EXTRACT injection blocked",
         {"metrics":[{"metric":"cnt","aggregation":"COUNT","target_column":"order_id"}],
          "fact_table":"fact_orders","group_by":["fact_orders.customer_id"],
          "having":[{"aggregation":"COUNT",
                     "target_column":"EXTRACT(month FROM order_date); DROP TABLE users",
                     "distinct":True,"operator":"=","value":12}]},
         [], expect_valid=False),

    case("4C-08", "Scalar subquery alias injection blocked",
         {"metrics":[{"metric":"r","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "scalar_subquery":{"alias":"pct; DROP TABLE users","numerator_metric":"r",
                             "multiply_by":100,
                             "denominator":{"aggregation":"SUM","target_column":"unit_price"}}},
         [], expect_valid=False),

    case("4C-09", "Correlated: unqualified outer_ref → rejected",
         {"metrics":[{"metric":"r","aggregation":"COUNT","target_column":"product_id"}],
          "fact_table":"dim_products","group_by":[],
          "correlated_filter":{"column":"dim_products.unit_price","operator":">",
              "subquery":{"aggregation":"AVG","target_column":"unit_price",
                          "fact_table":"dim_products","where_col":"category",
                          "outer_ref":"category"}}},   # ← not qualified
         [], expect_valid=False),

    case("4C-10", "Scalar subquery multiply_by float OK",
         {"metrics":[{"metric":"rev","aggregation":"SUM","target_column":"unit_price"}],
          "fact_table":"fact_orders","group_by":[],
          "scalar_subquery":{"alias":"pct","numerator_metric":"rev","multiply_by":100.0,
                             "denominator":{"aggregation":"SUM","target_column":"unit_price",
                                            "fact_table":"fact_orders"}}},
         ["100.0","NULLIF("]),
]

ALL_SUITES = {
    "baseline": ("Baseline (BL)",      SUITE_BASELINE),
    "4b":       ("Sprint 4B",          SUITE_4B),
    "4c":       ("Sprint 4C + Security", SUITE_4C),
}


# ── Runner ────────────────────────────────────────────────────────────────────

def run_case(tc: dict, verbose: bool) -> dict:
    intent       = tc["intent"]
    expect_valid = tc["expect_valid"]
    db           = tc["db"]

    ok, errors = validate_intent(dict(intent), SCHEMA)

    if not expect_valid:
        passed = not ok
        return {
            "id": tc["id"], "description": tc["description"],
            "passed": passed, "expect_valid": False,
            "detail": "correctly rejected" if passed else f"should have been invalid but passed",
            "sql": None,
        }

    if not ok:
        return {
            "id": tc["id"], "description": tc["description"],
            "passed": False, "expect_valid": True,
            "detail": f"validation failed: {'; '.join(errors)}",
            "sql": None,
        }

    try:
        sql, params = build_sql(dict(intent), db)
    except Exception as e:
        return {
            "id": tc["id"], "description": tc["description"],
            "passed": False, "expect_valid": True,
            "detail": f"build_sql raised: {e}",
            "sql": None,
        }

    sql_upper = sql.upper()
    missing     = [p for p in tc["must_contain"]     if p.upper() not in sql_upper]
    forbidden   = [p for p in tc["must_not_contain"]  if p.upper() in sql_upper]

    if missing or forbidden:
        detail = ""
        if missing:   detail += f"missing: {missing}  "
        if forbidden: detail += f"forbidden present: {forbidden}"
        return {"id":tc["id"],"description":tc["description"],
                "passed":False,"expect_valid":True,"detail":detail.strip(),"sql":sql}

    return {"id":tc["id"],"description":tc["description"],
            "passed":True,"expect_valid":True,"detail":"","sql":sql}


def run_suite(name: str, cases: list, verbose: bool, fail_fast: bool) -> tuple[int, int]:
    passes = fails = 0
    for tc in cases:
        r = run_case(tc, verbose)
        sym  = c("✓", GREEN, BOLD) if r["passed"] else c("✗", RED, BOLD)
        desc = f"{c(tc['id'], CYAN)}  {tc['description']}"
        print(f"  {sym}  {desc}")
        if not r["passed"]:
            print(f"       {c(r['detail'], RED)}")
            fails += 1
            if fail_fast:
                print(c("\n  Stopped on first failure (--fail-fast).", YELLOW))
                return passes, fails
        else:
            passes += 1
        if verbose and r.get("sql"):
            for line in r["sql"].strip().split("\n"):
                print(f"       {c(line, DIM)}")
    return passes, fails


def main():
    parser = argparse.ArgumentParser(description="Dataloom eval harness")
    parser.add_argument("--suite",     default="all", choices=["all","baseline","4b","4c"])
    parser.add_argument("--verbose",   action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args()

    suites = ALL_SUITES if args.suite == "all" else {args.suite: ALL_SUITES[args.suite]}

    total_pass = total_fail = 0
    t0 = time.time()

    print(f"\n{c('DATALOOM  —  Eval Harness', BOLD, CYAN)}")
    print(c(f"  {'all suites' if args.suite=='all' else args.suite}  ·  intent→sql only  ·  no DB required", DIM))
    print(c("  " + "─" * 58, DIM))

    for key, (label, cases) in suites.items():
        print(f"\n  {c(label, BOLD)}  {c(f'({len(cases)} cases)', DIM)}")
        p, f = run_suite(label, cases, args.verbose, args.fail_fast)
        total_pass += p
        total_fail += f
        status = c(f"{p}/{len(cases)} passed", GREEN if f == 0 else YELLOW)
        print(f"\n  {c('Suite result:', DIM)} {status}")
        if args.fail_fast and f > 0:
            break

    elapsed = round(time.time() - t0, 2)
    total   = total_pass + total_fail
    print(f"\n{c('  ' + '═'*58, DIM)}")
    print(f"  {c('TOTAL', BOLD)}  {c(total_pass, GREEN, BOLD)}/{total}  ·  {elapsed}s")
    if total_fail == 0:
        print(c("  All cases passed.\n", GREEN))
    else:
        print(c(f"  {total_fail} case(s) failed.\n", RED))

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
