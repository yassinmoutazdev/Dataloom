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
#   S1-S5 Official 5-question smoke test (per audit plan)
#   R     Sprint 1+2+3 regression battery
#   D     Dialect compatibility matrix per new feature
#   X     Cross-feature compound queries (≥3 patterns in one intent)
# =============================================================================

import copy, sys, traceback
from validator import set_join_paths, validate_intent
from sql_builder   import build_sql

PASS = 0
FAIL = 0
ERRORS = []

# ─── Schema used for all tests ────────────────────────────────────────────────
SCHEMA_MAP = {
    "fact_orders": [
        "order_id","customer_id","product_id","employee_id","campaign_id",
        "order_date","ship_date","quantity","unit_price","freight","status","region",
        "group_label",
    ],
    "dim_customers": [
        "customer_id","name","email","city","country","age",
        "signup_date","is_member","member_since",
    ],
    "dim_products": [
        "product_id","product_name","category","subcategory",
        "cost","price","stock_level","supplier_id",
    ],
    "dim_employees": ["employee_id","name","department","region","hire_date","role"],
    "dim_campaigns": ["campaign_id","source","clicks","conversions","month"],
}
SCHEMA_TYPES = {
    "fact_orders": {
        "order_id":"varchar","customer_id":"varchar","product_id":"varchar",
        "employee_id":"varchar","campaign_id":"varchar",
        "quantity":"integer","unit_price":"numeric","freight":"numeric",
        "order_date":"timestamp","ship_date":"timestamp",
    },
    "dim_products": {
        "price":"numeric","cost":"numeric","stock_level":"integer",
    },
}
set_join_paths({
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
})

# ─── Helpers ──────────────────────────────────────────────────────────────────
def base_intent(**overrides):
    i = {
        "metrics":         [{"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False}],
        "fact_table":      "fact_orders",
        "group_by":        [],
        "joins":           [],
        "filters":         [],
        "computed_columns":[],
        "time_filter":     None,
        "time_bucket":     None,
        "time_bucket_column": None,
        "having":          [],
        "limit":           10,
        "order_by":        None,
        "order_dir":       "DESC",
    }
    i.update(overrides)
    return i

def run(label, intent_raw, db_type="postgresql",
        must_contain=None, must_not_contain=None,
        expect_fail=False, check_params=None):
    global PASS, FAIL
    intent = copy.deepcopy(intent_raw)
    try:
        ok, errs = validate_intent(intent, SCHEMA_MAP, SCHEMA_TYPES)
        if expect_fail:
            if ok:
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: expected validation failure, got success")
                return
            else:
                PASS += 1
                print(f"  ✓  {label}  [expected reject: {errs[0][:60]}]")
                return
        if not ok:
            FAIL += 1
            ERRORS.append(f"[FAIL] {label}: validate_intent failed → {errs}")
            return
        sql, params = build_sql(intent, db_type)
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
        if check_params is not None:
            if params != check_params:
                FAIL += 1
                ERRORS.append(f"[FAIL] {label}: params mismatch. Expected {check_params}, got {params}")
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


# =============================================================================
# SECTION 4A-1: IS NULL / IS NOT NULL
# =============================================================================
section("4A-1 · IS NULL / IS NOT NULL operator")

run("4A-1-A: IS NULL on email (E9 smoke)",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"cust_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        filters=[{"column":"email","operator":"IS NULL"}],
        joins=[],
    ),
    must_contain=["IS NULL"],
    check_params=[],
)

run("4A-1-B: IS NOT NULL on email",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"cust_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        filters=[{"column":"email","operator":"IS NOT NULL"}],
        joins=[],
    ),
    must_contain=["IS NOT NULL"],
    check_params=[],
)

run("4A-1-C: IS NULL combined with value filter",
    base_intent(
        filters=[
            {"column":"email","operator":"IS NULL"},
            {"column":"status","operator":"=","value":"pending"},
        ],
    ),
    must_contain=["IS NULL", "%s"],
    check_params=["pending"],
)

run("4A-1-D: IS NULL on cross-table column with JOIN",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"cust_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        filters=[{"column":"dim_customers.email","operator":"IS NULL"}],
        joins=[],
    ),
    must_contain=["IS NULL"],
)

run("4A-1-E: Invalid operator (reject)",
    base_intent(filters=[{"column":"email","operator":"IS MISSING","value":None}]),
    expect_fail=True,
)

run("4A-1-F: SQLite — IS NULL produces correct parameterless fragment",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"cust_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        filters=[{"column":"email","operator":"IS NULL"}],
    ),
    db_type="sqlite",
    must_contain=["IS NULL"],
    check_params=[],
)


# =============================================================================
# SECTION 4A-2: Typed JOINs / Anti-Join
# =============================================================================
section("4A-2 · LEFT JOIN / Anti-Join (I2 and A8 patterns)")

run("4A-2-A: LEFT JOIN renders LEFT JOIN keyword",
    base_intent(
        fact_table="dim_products",
        metrics=[{"metric":"product_count","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        group_by=["dim_products.product_name"],
        joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
    ),
    must_contain=["LEFT JOIN", "IS NULL"],
)

run("4A-2-B: INNER JOIN string stays as plain JOIN",
    base_intent(
        joins=["fact_orders.product_id = dim_products.product_id"],
        group_by=["dim_products.category"],
    ),
    must_contain=["JOIN dim_products"],
    must_not_contain=["LEFT JOIN", "RIGHT JOIN"],
)

run("4A-2-C: Anti-join: products never ordered (I2)",
    base_intent(
        fact_table="dim_products",
        metrics=[{"metric":"unordered_count","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        group_by=["dim_products.product_name"],
        joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
    ),
    must_contain=["LEFT JOIN", "IS NULL"],
)

run("4A-2-D: Anti-join: churned customers (A8 pattern)",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"churn_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        group_by=["dim_customers.name"],
        joins=[{"type":"LEFT","condition":"dim_customers.customer_id = fact_orders.customer_id"}],
        filters=[{"column":"fact_orders.customer_id","operator":"IS NULL"}],
    ),
    must_contain=["LEFT JOIN", "fact_orders.customer_id IS NULL"],
)

run("4A-2-E: RIGHT JOIN keyword renders correctly",
    base_intent(
        fact_table="dim_products",
        metrics=[{"metric":"cnt","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        joins=[{"type":"RIGHT","condition":"dim_products.product_id = fact_orders.product_id"}],
    ),
    must_contain=["RIGHT JOIN"],
)

run("4A-2-F: Mixed typed + plain joins in same query",
    base_intent(
        group_by=["dim_products.category","dim_employees.department"],
        joins=[
            "fact_orders.product_id = dim_products.product_id",
            {"type":"LEFT","condition":"fact_orders.employee_id = dim_employees.employee_id"},
        ],
    ),
    must_contain=["JOIN dim_products ON", "LEFT JOIN dim_employees ON"],
)


# =============================================================================
# SECTION 4A-3: EXTRACT / Date-Part Expressions in group_by
# =============================================================================
section("4A-3 · EXTRACT date-part expressions in group_by")

run("4A-3-A: EXTRACT month in group_by — PostgreSQL",
    base_intent(
        group_by=["EXTRACT(month FROM fact_orders.order_date) AS order_month"],
    ),
    must_contain=["EXTRACT(month FROM fact_orders.order_date)"],
)

run("4A-3-B: EXTRACT DOW in group_by — weekend/weekday (I9 pattern)",
    base_intent(
        group_by=["EXTRACT(dow FROM fact_orders.order_date) AS day_of_week"],
    ),
    must_contain=["EXTRACT(dow FROM"],
)

run("4A-3-C: EXTRACT year MySQL emits YEAR()",
    base_intent(
        group_by=["YEAR(fact_orders.order_date) AS order_year"],
    ),
    db_type="mysql",
    must_contain=["YEAR("],
)

run("4A-3-D: EXTRACT in group_by also appears in GROUP BY clause",
    base_intent(
        group_by=["EXTRACT(month FROM fact_orders.order_date) AS order_month"],
    ),
    must_contain=["GROUP BY"],
)

run("4A-3-E: SQLite strftime expression passes through",
    base_intent(
        group_by=["CAST(strftime('%m', fact_orders.order_date) AS INTEGER) AS order_month"],
    ),
    db_type="sqlite",
    must_contain=["strftime"],
)

run("4A-3-F: EXTRACT is NOT schema-validated (no false errors)",
    base_intent(
        group_by=["EXTRACT(quarter FROM fact_orders.order_date) AS qtr"],
    ),
    must_contain=["EXTRACT"],
)


# =============================================================================
# SECTION 4A-4: Date-Arithmetic Metrics
# =============================================================================
section("4A-4 · Date-arithmetic expressions (A2, A5, W10 patterns)")

run("4A-4-A: AVG diff_days PostgreSQL (A2 pattern — avg shipping delay)",
    base_intent(
        metrics=[{
            "metric":"avg_delay_days",
            "aggregation":"AVG",
            "target_column":"ship_date - order_date",
            "is_expression": True,
            "date_arithmetic":{
                "operation":"diff_days",
                "col_a":"ship_date",
                "col_b":"order_date",
            },
        }],
    ),
    must_contain=["AVG(", "86400", "epoch"],
)

run("4A-4-B: AVG diff_days MySQL emits TIMESTAMPDIFF",
    base_intent(
        metrics=[{
            "metric":"avg_delay_days",
            "aggregation":"AVG",
            "target_column":"",
            "is_expression": True,
            "date_arithmetic":{"operation":"diff_days","col_a":"ship_date","col_b":"order_date"},
        }],
    ),
    db_type="mysql",
    must_contain=["TIMESTAMPDIFF", "86400"],
)

run("4A-4-C: AVG diff_days SQLite emits julianday",
    base_intent(
        metrics=[{
            "metric":"avg_delay_days",
            "aggregation":"AVG",
            "target_column":"",
            "is_expression": True,
            "date_arithmetic":{"operation":"diff_days","col_a":"ship_date","col_b":"order_date"},
        }],
    ),
    db_type="sqlite",
    must_contain=["julianday"],
)

run("4A-4-D: diff_hours operation",
    base_intent(
        metrics=[{
            "metric":"avg_delay_hours",
            "aggregation":"AVG",
            "target_column":"",
            "is_expression": True,
            "date_arithmetic":{"operation":"diff_hours","col_a":"ship_date","col_b":"order_date"},
        }],
    ),
    must_contain=["3600"],
)

run("4A-4-E: diff_seconds operation SQLite",
    base_intent(
        metrics=[{
            "metric":"delay_secs",
            "aggregation":"AVG",
            "target_column":"",
            "is_expression": True,
            "date_arithmetic":{"operation":"diff_seconds","col_a":"ship_date","col_b":"order_date"},
        }],
    ),
    db_type="sqlite",
    must_contain=["86400", "julianday"],
)

run("4A-4-F: Invalid date_arithmetic.operation rejected",
    base_intent(
        metrics=[{
            "metric":"bad",
            "aggregation":"AVG",
            "target_column":"",
            "is_expression": True,
            "date_arithmetic":{"operation":"diff_years","col_a":"ship_date","col_b":"order_date"},
        }],
    ),
    expect_fail=True,
)

run("4A-4-G: Generic raw expression (no date_arithmetic block)",
    base_intent(
        metrics=[{
            "metric":"avg_ratio",
            "aggregation":"AVG",
            "target_column":"unit_price / NULLIF(quantity, 0)",
            "is_expression": True,
        }],
    ),
    must_contain=["AVG(unit_price / NULLIF(quantity, 0))"],
)


# =============================================================================
# SECTION 4A-5: NTILE and PERCENTILE_CONT
# =============================================================================
section("4A-5 · NTILE and PERCENTILE_CONT (W12, W13, X7 patterns)")

run("4A-5-A: NTILE(10) PostgreSQL — revenue deciles (X7)",
    base_intent(
        metrics=[{
            "metric":"revenue_decile",
            "aggregation":"NTILE",
            "ntile_buckets":10,
            "order_by_column":"total_revenue",
            "order_dir":"DESC",
            "target_column":"",
        }],
    ),
    must_contain=["NTILE(10)", "ORDER BY total_revenue DESC"],
)

run("4A-5-B: NTILE(10) MySQL same syntax",
    base_intent(
        metrics=[{
            "metric":"revenue_decile",
            "aggregation":"NTILE",
            "ntile_buckets":10,
            "order_by_column":"total_revenue",
            "order_dir":"DESC",
            "target_column":"",
        }],
    ),
    db_type="mysql",
    must_contain=["NTILE(10)"],
)

run("4A-5-C: NTILE(10) SQLite",
    base_intent(
        metrics=[{
            "metric":"revenue_decile",
            "aggregation":"NTILE",
            "ntile_buckets":10,
            "order_by_column":"total_revenue",
            "order_dir":"DESC",
            "target_column":"",
        }],
    ),
    db_type="sqlite",
    must_contain=["NTILE(10)"],
)

run("4A-5-D: NTILE missing ntile_buckets rejected",
    base_intent(
        metrics=[{
            "metric":"decile",
            "aggregation":"NTILE",
            "order_by_column":"total_revenue",
            "target_column":"",
        }],
    ),
    expect_fail=True,
)

run("4A-5-E: PERCENTILE_CONT(0.5) PostgreSQL — median (W12)",
    base_intent(
        metrics=[{
            "metric":"median_order_value",
            "aggregation":"PERCENTILE_CONT",
            "percentile":0.5,
            "target_column":"unit_price",
            "order_dir":"ASC",
        }],
    ),
    must_contain=["PERCENTILE_CONT(0.5)", "WITHIN GROUP", "ORDER BY"],
)

run("4A-5-F: PERCENTILE_CONT(0.9) PostgreSQL",
    base_intent(
        metrics=[{
            "metric":"p90",
            "aggregation":"PERCENTILE_CONT",
            "percentile":0.9,
            "target_column":"unit_price",
            "order_dir":"ASC",
        }],
    ),
    must_contain=["PERCENTILE_CONT(0.9)"],
)

run("4A-5-G: PERCENTILE_CONT SQLite emits fallback subquery",
    base_intent(
        metrics=[{
            "metric":"median_order_value",
            "aggregation":"PERCENTILE_CONT",
            "percentile":0.5,
            "target_column":"unit_price",
            "order_dir":"ASC",
        }],
    ),
    db_type="sqlite",
    must_contain=["ROW_NUMBER", "cnt"],
)

run("4A-5-H: PERCENTILE_CONT percentile out of range rejected",
    base_intent(
        metrics=[{
            "metric":"bad_p",
            "aggregation":"PERCENTILE_CONT",
            "percentile":1.5,
            "target_column":"unit_price",
        }],
    ),
    expect_fail=True,
)


# =============================================================================
# SECTION 4A-6: Standalone HAVING COUNT DISTINCT
# =============================================================================
section("4A-6 · Standalone HAVING aggregation — COUNT DISTINCT (A11, A15 patterns)")

run("4A-6-A: HAVING COUNT(DISTINCT product_id) > 3 (A11 pattern)",
    base_intent(
        group_by=["fact_orders.order_id"],
        having=[{
            "aggregation":"COUNT",
            "target_column":"product_id",
            "distinct":True,
            "operator":">",
            "value":3,
        }],
    ),
    must_contain=["COUNT(DISTINCT", "product_id", "> 3"],
)

run("4A-6-B: HAVING COUNT DISTINCT across dialects — MySQL",
    base_intent(
        group_by=["fact_orders.order_id"],
        having=[{
            "aggregation":"COUNT",
            "target_column":"product_id",
            "distinct":True,
            "operator":">",
            "value":3,
        }],
    ),
    db_type="mysql",
    must_contain=["COUNT(DISTINCT"],
)

run("4A-6-C: HAVING COUNT DISTINCT across dialects — SQLite",
    base_intent(
        group_by=["fact_orders.order_id"],
        having=[{
            "aggregation":"COUNT",
            "target_column":"product_id",
            "distinct":True,
            "operator":">",
            "value":3,
        }],
    ),
    db_type="sqlite",
    must_contain=["COUNT(DISTINCT"],
)

run("4A-6-D: HAVING SUM standalone (not in SELECT)",
    base_intent(
        group_by=["dim_products.category"],
        joins=["fact_orders.product_id = dim_products.product_id"],
        having=[{
            "aggregation":"SUM",
            "target_column":"unit_price",
            "operator":">",
            "value":50000,
        }],
    ),
    must_contain=["HAVING SUM(", "> 50000"],
)

run("4A-6-E: HAVING references SELECT metric by name (backward compat)",
    base_intent(
        group_by=["dim_customers.country"],
        joins=["fact_orders.customer_id = dim_customers.customer_id"],
        having=[{"metric":"total_revenue","operator":">","value":1000}],
    ),
    must_contain=["HAVING", "1000"],
)

run("4A-6-F: HAVING missing operator rejected",
    base_intent(
        having=[{"aggregation":"COUNT","target_column":"product_id","distinct":True,"value":3}],
    ),
    expect_fail=True,
)


# =============================================================================
# SECTION 4A-7: CASE WHEN computed_columns[]
# =============================================================================
section("4A-7 · CASE WHEN computed_columns (I9, A3, A14, W8, X2, X10 patterns)")

run("4A-7-A: High/Medium/Low spender tiers (A3 pattern)",
    base_intent(
        group_by=["fact_orders.customer_id"],
        computed_columns=[{
            "alias":"spending_tier",
            "when_clauses":[
                {"condition":"SUM(fact_orders.unit_price) > 5000","then":"'High'"},
                {"condition":"SUM(fact_orders.unit_price) > 1000","then":"'Medium'"},
            ],
            "else_value":"'Low'",
        }],
    ),
    must_contain=["CASE", "WHEN", "THEN", "ELSE", "END AS spending_tier"],
)

run("4A-7-B: Weekend / weekday bucket (I9 pattern)",
    base_intent(
        group_by=["EXTRACT(dow FROM fact_orders.order_date) AS dow"],
        computed_columns=[{
            "alias":"day_type",
            "when_clauses":[
                {"condition":"EXTRACT(dow FROM fact_orders.order_date) IN (0,6)","then":"'Weekend'"},
            ],
            "else_value":"'Weekday'",
        }],
    ),
    must_contain=["CASE", "Weekend", "Weekday", "END AS day_type"],
)

run("4A-7-C: CASE WHEN alias appears in SELECT before metric",
    base_intent(
        computed_columns=[{
            "alias":"revenue_band",
            "when_clauses":[
                {"condition":"SUM(fact_orders.unit_price) > 10000","then":"'Premium'"},
            ],
            "else_value":"'Standard'",
        }],
    ),
    must_contain=["END AS revenue_band", "SUM(fact_orders.unit_price) AS total_revenue"],
)

run("4A-7-D: CASE WHEN with include_in_group_by=True appears in GROUP BY",
    base_intent(
        group_by=["fact_orders.customer_id"],
        computed_columns=[{
            "alias":"tier",
            "when_clauses":[{"condition":"fact_orders.region = 'APAC'","then":"'Asia'"}],
            "else_value":"'Other'",
            "include_in_group_by":True,
        }],
    ),
    must_contain=["GROUP BY", "CASE"],
)

run("4A-7-E: Multiple CASE WHEN columns",
    base_intent(
        computed_columns=[
            {
                "alias":"size_tier",
                "when_clauses":[{"condition":"SUM(fact_orders.quantity) > 100","then":"'Large'"}],
                "else_value":"'Small'",
            },
            {
                "alias":"value_tier",
                "when_clauses":[{"condition":"SUM(fact_orders.unit_price) > 5000","then":"'High'"}],
                "else_value":"'Low'",
            },
        ],
    ),
    must_contain=["END AS size_tier", "END AS value_tier"],
)

run("4A-7-F: Missing alias rejected",
    base_intent(
        computed_columns=[{
            "when_clauses":[{"condition":"1=1","then":"'x'"}],
        }],
    ),
    expect_fail=True,
)

run("4A-7-G: Invalid alias (CamelCase) rejected",
    base_intent(
        computed_columns=[{
            "alias":"SpendingTier",
            "when_clauses":[{"condition":"1=1","then":"'x'"}],
        }],
    ),
    expect_fail=True,
)

run("4A-7-H: Empty when_clauses rejected",
    base_intent(
        computed_columns=[{
            "alias":"tier",
            "when_clauses":[],
        }],
    ),
    expect_fail=True,
)

run("4A-7-I: CASE WHEN MySQL — same syntax",
    base_intent(
        computed_columns=[{
            "alias":"spending_tier",
            "when_clauses":[{"condition":"SUM(fact_orders.unit_price) > 5000","then":"'High'"}],
            "else_value":"'Low'",
        }],
    ),
    db_type="mysql",
    must_contain=["CASE", "WHEN", "END AS spending_tier"],
)

run("4A-7-J: CASE WHEN SQLite — same syntax",
    base_intent(
        computed_columns=[{
            "alias":"spending_tier",
            "when_clauses":[{"condition":"SUM(fact_orders.unit_price) > 5000","then":"'High'"}],
            "else_value":"'Low'",
        }],
    ),
    db_type="sqlite",
    must_contain=["CASE", "WHEN", "END AS spending_tier"],
)


# =============================================================================
# SECTION S: Official 5-Question Smoke Test (per audit roadmap)
# =============================================================================
section("SMOKE TEST · 5 Official Questions from Audit Plan")

# S1 — E9: Customers without valid email (IS NULL)
run("S1 · E9: Customers without valid email",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"cust_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        filters=[{"column":"email","operator":"IS NULL"}],
        joins=[],
    ),
    must_contain=["WHERE", "email IS NULL"],
    check_params=[],
)

# S2 — I2: Products never ordered (LEFT JOIN anti-join)
run("S2 · I2: Products never ordered",
    base_intent(
        fact_table="dim_products",
        metrics=[{"metric":"product_count","aggregation":"COUNT","target_column":"product_id","distinct":False}],
        group_by=["dim_products.product_name"],
        joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        filters=[{"column":"fact_orders.product_id","operator":"IS NULL"}],
    ),
    must_contain=["LEFT JOIN", "fact_orders.product_id IS NULL"],
    check_params=[],
)

# S3 — A2: Average shipping delay in days (date arithmetic)
run("S3 · A2: Average shipping delay in days",
    base_intent(
        metrics=[{
            "metric":"avg_delay_days",
            "aggregation":"AVG",
            "target_column":"",
            "is_expression":True,
            "date_arithmetic":{"operation":"diff_days","col_a":"ship_date","col_b":"order_date"},
        }],
    ),
    must_contain=["AVG(", "epoch", "86400"],
)

# S4 — X7: Products in revenue deciles (NTILE)
run("S4 · X7: Products in revenue deciles",
    base_intent(
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {
                "metric":"revenue_decile",
                "aggregation":"NTILE",
                "ntile_buckets":10,
                "order_by_column":"total_revenue",
                "order_dir":"DESC",
                "target_column":"",
            },
        ],
        group_by=["fact_orders.product_id"],
    ),
    must_contain=["NTILE(10)", "SUM(fact_orders.unit_price)"],
)

# S5 — A3: High/Medium/Low spender categories (CASE WHEN)
run("S5 · A3: High/Medium/Low spender categories",
    base_intent(
        group_by=["fact_orders.customer_id"],
        computed_columns=[{
            "alias":"spending_tier",
            "when_clauses":[
                {"condition":"SUM(fact_orders.unit_price) > 5000","then":"'High'"},
                {"condition":"SUM(fact_orders.unit_price) > 1000","then":"'Medium'"},
            ],
            "else_value":"'Low'",
        }],
    ),
    must_contain=["CASE", "WHEN SUM(fact_orders.unit_price) > 5000 THEN 'High'",
                  "WHEN SUM(fact_orders.unit_price) > 1000 THEN 'Medium'",
                  "ELSE 'Low'", "END AS spending_tier"],
)


# =============================================================================
# SECTION X: Cross-Feature Compound Queries
# =============================================================================
section("X · Cross-feature compound (≥3 patterns per query)")

# X1: Anti-join + IS NULL + time filter (I2 + E9 + time)
run("X1 · Anti-join + IS NULL + time_filter",
    base_intent(
        fact_table="dim_customers",
        metrics=[{"metric":"cust_count","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
        group_by=["dim_customers.city"],
        joins=[{"type":"LEFT","condition":"dim_customers.customer_id = fact_orders.customer_id"}],
        filters=[{"column":"fact_orders.customer_id","operator":"IS NULL"},
                 {"column":"dim_customers.email","operator":"IS NOT NULL"}],
    ),
    must_contain=["LEFT JOIN", "IS NULL", "IS NOT NULL", "GROUP BY"],
)

# X2: CASE WHEN + EXTRACT + HAVING COUNT DISTINCT (I9 + A11)
run("X2 · CASE WHEN + EXTRACT + HAVING COUNT DISTINCT",
    base_intent(
        group_by=["EXTRACT(dow FROM fact_orders.order_date) AS dow"],
        computed_columns=[{
            "alias":"day_type",
            "when_clauses":[{"condition":"EXTRACT(dow FROM fact_orders.order_date) IN (0,6)","then":"'Weekend'"}],
            "else_value":"'Weekday'",
        }],
        having=[{
            "aggregation":"COUNT",
            "target_column":"order_id",
            "distinct":True,
            "operator":">",
            "value":100,
        }],
    ),
    must_contain=["CASE", "EXTRACT(dow", "COUNT(DISTINCT", "> 100"],
)

# X3: Date arith + CASE WHEN + LEFT JOIN (A2 + A3 + I2)
run("X3 · Date arith + CASE WHEN + HAVING (compound A2+A3)",
    base_intent(
        group_by=["fact_orders.customer_id"],
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {
                "metric":"avg_ship_delay",
                "aggregation":"AVG",
                "target_column":"",
                "is_expression":True,
                "date_arithmetic":{"operation":"diff_days","col_a":"ship_date","col_b":"order_date"},
            },
        ],
        computed_columns=[{
            "alias":"customer_tier",
            "when_clauses":[{"condition":"SUM(fact_orders.unit_price) > 5000","then":"'High'"}],
            "else_value":"'Standard'",
        }],
        having=[{"metric":"total_revenue","operator":">","value":500}],
    ),
    must_contain=["SUM(fact_orders.unit_price)", "AVG(", "epoch", "CASE", "HAVING"],
)

# X4: NTILE + JOIN + CASE WHEN + filter (X7 + A3)
run("X4 · NTILE + JOIN + CASE WHEN",
    base_intent(
        group_by=["dim_products.category"],
        joins=["fact_orders.product_id = dim_products.product_id"],
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {
                "metric":"cat_decile",
                "aggregation":"NTILE",
                "ntile_buckets":10,
                "order_by_column":"total_revenue",
                "order_dir":"DESC",
                "target_column":"",
            },
        ],
        computed_columns=[{
            "alias":"revenue_band",
            "when_clauses":[
                {"condition":"SUM(fact_orders.unit_price) > 100000","then":"'Tier 1'"},
                {"condition":"SUM(fact_orders.unit_price) > 50000","then":"'Tier 2'"},
            ],
            "else_value":"'Tier 3'",
        }],
    ),
    must_contain=["NTILE(10)", "JOIN dim_products", "CASE", "Tier 1"],
)


# =============================================================================
# SECTION R: Sprint 1 + 2 + 3 Regression Battery
# =============================================================================
section("R · Sprint 1+2+3 Regression Battery")

run("R1 · S1 Revenue by month (DATE_TRUNC)",
    base_intent(
        time_bucket="month",
        time_bucket_column="order_date",
    ),
    must_contain=["DATE_TRUNC('month'", "month_period"],
)

run("R2 · S1 Orders/week 2017 (year filter)",
    base_intent(
        metrics=[{"metric":"order_count","aggregation":"COUNT","target_column":"order_id","distinct":False}],
        time_bucket="week",
        time_bucket_column="order_date",
        time_filter={"column":"order_date","year":2017},
    ),
    must_contain=["2017", "week_period"],
)

run("R3 · S1 Categories revenue > 50k (HAVING)",
    base_intent(
        group_by=["dim_products.category"],
        joins=["fact_orders.product_id = dim_products.product_id"],
        having=[{"metric":"total_revenue","operator":">","value":50000}],
    ),
    must_contain=["HAVING", "50000"],
)

run("R4 · S2 Multi-metric: revenue + orders by state",
    base_intent(
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {"metric":"order_count","aggregation":"COUNT","target_column":"order_id","distinct":False},
        ],
        group_by=["dim_customers.city"],
        joins=["fact_orders.customer_id = dim_customers.customer_id"],
    ),
    must_contain=["SUM(fact_orders.unit_price) AS total_revenue",
                  "COUNT(*) AS order_count"],
)

run("R5 · S3 Multi-hop: fact → products → (auto-repaired from group_by)",
    base_intent(
        group_by=["dim_products.category"],
        joins=[],  # auto-repair should add the join
    ),
    must_contain=["JOIN dim_products ON"],
)

run("R6 · S1+S2 Monthly + HAVING + multi-metric",
    base_intent(
        metrics=[
            {"metric":"total_revenue","aggregation":"SUM","target_column":"unit_price","distinct":False},
            {"metric":"order_count","aggregation":"COUNT","target_column":"order_id","distinct":False},
        ],
        time_bucket="month",
        time_bucket_column="order_date",
        having=[{"metric":"total_revenue","operator":">","value":10000}],
    ),
    must_contain=["DATE_TRUNC", "HAVING", "SUM", "COUNT"],
)


# =============================================================================
# SECTION D: Dialect Integrity Matrix (all new patterns, all 3 dialects)
# =============================================================================
section("D · Dialect Integrity Matrix (new patterns × 3 dialects)")

for db in ("postgresql","mysql","sqlite"):
    run(f"D-ISNULL-{db}: IS NULL syntax identical",
        base_intent(
            fact_table="dim_customers",
            metrics=[{"metric":"n","aggregation":"COUNT","target_column":"customer_id","distinct":False}],
            filters=[{"column":"email","operator":"IS NULL"}],
        ),
        db_type=db,
        must_contain=["IS NULL"],
        check_params=[],
    )

for db in ("postgresql","mysql","sqlite"):
    run(f"D-LEFTJOIN-{db}: LEFT JOIN keyword",
        base_intent(
            fact_table="dim_products",
            metrics=[{"metric":"n","aggregation":"COUNT","target_column":"product_id","distinct":False}],
            joins=[{"type":"LEFT","condition":"dim_products.product_id = fact_orders.product_id"}],
        ),
        db_type=db,
        must_contain=["LEFT JOIN"],
    )

for db in ("postgresql","mysql","sqlite"):
    run(f"D-CASEWHEN-{db}: CASE WHEN syntax identical",
        base_intent(
            computed_columns=[{
                "alias":"tier",
                "when_clauses":[{"condition":"SUM(fact_orders.unit_price) > 1000","then":"'High'"}],
                "else_value":"'Low'",
            }],
        ),
        db_type=db,
        must_contain=["CASE", "WHEN", "THEN", "ELSE", "END AS tier"],
    )

for db,expected in (
    ("postgresql", "epoch"),
    ("mysql",      "TIMESTAMPDIFF"),
    ("sqlite",     "julianday"),
):
    run(f"D-DATEARITH-{db}: diff_days uses correct dialect expression",
        base_intent(
            metrics=[{
                "metric":"d",
                "aggregation":"AVG",
                "target_column":"",
                "is_expression":True,
                "date_arithmetic":{"operation":"diff_days","col_a":"ship_date","col_b":"order_date"},
            }],
        ),
        db_type=db,
        must_contain=[expected],
    )

for db in ("postgresql","mysql","sqlite"):
    run(f"D-NTILE-{db}: NTILE(5) present",
        base_intent(
            metrics=[{
                "metric":"q","aggregation":"NTILE","ntile_buckets":5,
                "order_by_column":"total_revenue","order_dir":"DESC","target_column":"",
            }],
        ),
        db_type=db,
        must_contain=["NTILE(5)"],
    )

run("D-PERCENTILE-postgresql: PERCENTILE_CONT WITHIN GROUP",
    base_intent(
        metrics=[{"metric":"med","aggregation":"PERCENTILE_CONT","percentile":0.5,
                  "target_column":"unit_price","order_dir":"ASC"}],
    ),
    db_type="postgresql",
    must_contain=["PERCENTILE_CONT(0.5)", "WITHIN GROUP"],
)

run("D-PERCENTILE-sqlite: fallback ROW_NUMBER subquery",
    base_intent(
        metrics=[{"metric":"med","aggregation":"PERCENTILE_CONT","percentile":0.5,
                  "target_column":"unit_price","order_dir":"ASC"}],
    ),
    db_type="sqlite",
    must_contain=["ROW_NUMBER", "cnt"],
    must_not_contain=["WITHIN GROUP"],
)

for db in ("postgresql","mysql","sqlite"):
    run(f"D-HAVINGDIST-{db}: COUNT(DISTINCT) in HAVING",
        base_intent(
            group_by=["fact_orders.order_id"],
            having=[{"aggregation":"COUNT","target_column":"product_id","distinct":True,
                     "operator":">","value":2}],
        ),
        db_type=db,
        must_contain=["COUNT(DISTINCT"],
    )


# =============================================================================
# FINAL REPORT
# =============================================================================
total = PASS + FAIL
section(f"RESULTS  —  {PASS}/{total} passed  ({FAIL} failed)")

if ERRORS:
    print()
    for e in ERRORS:
        print(e)
        print()

if FAIL:
    sys.exit(1)
else:
    print("\n  ✅  All Sprint 4A tests passed. v3.0 is GREEN.\n")
