"""
Natural-language intent parsing for the Dataloom query pipeline (v3.0, Sprint 4A).

Owns the LLM interaction layer: converts a user question into a structured intent
dict that validator.py and sql_builder.py can consume. Supports Ollama, OpenAI,
Gemini, Anthropic, xAI, OpenRouter, and Azure OpenAI as interchangeable backends.

Public API:
    is_vague_question(question)                                  → bool
    parse_intent(question, schema, history, model_config)        → dict
    parse_retry(failed_sql, error_msg, previous_intent, ...)     → dict
    parse_validation_retry(question, failed_intent, errors, ...) → dict
"""

# Sprint 4A prompt additions (marked # 4A-N):
#   4A-1  IS NULL / IS NOT NULL operators in filters[]
#   4A-2  Typed JOINs (join_type: "LEFT") for anti-join patterns
#   4A-3  EXTRACT expressions in group_by
#   4A-4  Date-arithmetic metrics (is_expression + date_arithmetic)
#   4A-5  NTILE and PERCENTILE_CONT aggregations
#   4A-6  Standalone HAVING aggregation (COUNT DISTINCT not in SELECT)
#   4A-7  computed_columns[] for CASE WHEN expressions

import json
import ollama

try:
    import openai
except ImportError:
    openai = None

# Optional dependency for Anthropic Claude support
try:
    import anthropic  # type: ignore
except ImportError:
    anthropic = None

# Regex patterns used by is_vague_question to detect open-ended, non-queryable prompts.
VAGUE_PATTERNS = [
    r"^how are we doing",
    r"^how.{0,20}doing",
    r"^what.{0,20}happening",
    r"^give me an update",
    r"^how.{0,20}going",
    r"^what.{0,20}status",
    r"^how.{0,20}performing",
    r"^tell me about",
    r"^what.{0,20}looking like",
    r"^any updates",
    r"^how.{0,20}business",
]


def is_vague_question(question: str) -> bool:
    """Return True if the question is too open-ended to map to a SQL query.

    Matches against VAGUE_PATTERNS to catch greetings and status questions
    that the pipeline should route to a clarification prompt instead of
    attempting intent parsing.

    Args:
        question: Raw user input string.

    Returns:
        True if the question matches a vague pattern, False otherwise.
    """
    import re
    q = question.lower().strip()
    return any(re.search(p, q) for p in VAGUE_PATTERNS)


# ═════════════════════════════════════════════════════════════════════════════
# INTENT_SYSTEM_PROMPT  —  v3.0 (Sprint 4A)
# ═════════════════════════════════════════════════════════════════════════════
INTENT_SYSTEM_PROMPT = r"""You are a strict intent parser for a database assistant.

Your ONLY job is to convert a natural language question into a JSON object.
You must NEVER write SQL. You must NEVER write prose. JSON only.
━━━━━━━━━━━━━━━━━━━━
FULL INTENT SCHEMA  (v3.0 + Sprint 4B/4C)
━━━━━━━━━━━━━━━━━━━━
{
  "metrics": [ ... ],
  "fact_table": "main table to query",
  "group_by": [],
  "joins": [],
  "computed_columns": [],
  "window_functions": [],
  "scalar_subquery": null,
  "set_operation": null,
  "ctes": [],
  "correlated_filter": null,
  "time_filter": null,
  "time_bucket": null,
  "time_bucket_column": null,
  "having": [],
  "filters": [],
  "limit": 10,
  "order_by": "metric name from metrics[] or null",
  "order_dir": "DESC | ASC",
  "clarification_needed": null,
  "confidence": "high"
}
━━━━━━━━━━━━━━━━━━━━
CONFIDENCE & CLARIFICATION RULES
━━━━━━━━━━━━━━━━━━━━
Every response MUST include a "confidence" field. Choose one:

  "high"   — You are certain about the table, metric, and all column
             references. The schema clearly supports the question.

  "medium" — You made at least one assumption. Common triggers:
             • Metric name is ambiguous (e.g. "revenue" could be
               unit_price OR unit_price * quantity)
             • Time period implied but not stated
             • Column match required a best-guess (e.g. "sales" → "price")
             • Multiple candidate tables for the same entity

  "low"    — You cannot map the question reliably to the schema.
             Either the entity is not in the schema, or two equally
             valid interpretations exist with different SQL outcomes.
Rules:
- If confidence is "low", you MUST also set clarification_needed
  with a specific question that would resolve the ambiguity.
- If confidence is "medium", set clarification_needed ONLY if the
  assumption you made could materially change the result.
- If confidence is "high", clarification_needed must be null.
- Never omit confidence. Default "high" only when genuinely certain.
━━━━━━━━━━━━━━━━━━━━
SECTION 1 — metrics[]
━━━━━━━━━━━━━━━━━━━━
Standard metric (SUM / COUNT / AVG / MAX / MIN):
{
  "metric": "snake_case name, e.g. total_revenue",
  "aggregation": "SUM | COUNT | AVG | MAX | MIN",
  "target_column": "plain column name — never an expression or SQL function",
  "distinct": false
}
Rules:
- Always use metrics[]. One item for single-metric questions, two+ for multi-metric.
- target_column must be a raw column name (e.g. "price", "order_id"). Never write
  "SUM(price)" or any expression here.
- For COUNT, set target_column to the most relevant id column (e.g. "order_id").
- distinct: true only when the question says "unique", "distinct", or "different".
- Metric names must be unique within the array — suffix _2, _3 on duplicates.

4A-5  NTILE  (segment/decile/percentile-rank questions):
{
  "metric": "revenue_decile",
  "aggregation": "NTILE",
  "ntile_buckets": 10,
  "order_by_column": "SUM(fact_orders.unit_price)",
  "order_dir": "DESC"
}
Use when: "segment into deciles", "rank into N groups", "top 10%".
Rules:
- order_by_column MUST be the full SQL aggregation expression
  (e.g., "SUM(fact_orders.unit_price)"), NEVER an alias name.

4A-5  PERCENTILE_CONT  (median questions):
{
  "metric": "median_order_value",
  "aggregation": "PERCENTILE_CONT",
  "target_column": "unit_price",
  "percentile": 0.5,
  "order_dir": "ASC"
}
Use when: "median", "50th percentile".

4A-4  Date arithmetic  (interval / duration questions):
{
  "metric": "avg_shipping_delay",
  "aggregation": "AVG",
  "is_expression": true,
  "outer_aggregation": "AVG",
  "date_arithmetic": {
    "operation": "diff_days | diff_hours | diff_seconds",
    "col_a": "ship_date",
    "col_b": "order_date"
  }
}
Use when: "average delay", "time between", "hours from X to Y".
Rules:
- You MUST explicitly set "outer_aggregation" (e.g., "AVG") whenever
  using date_arithmetic. Do not rely on defaults.
operation values:
  diff_days    — result in fractional days
  diff_hours   — result in fractional hours
  diff_seconds — result in whole seconds
━━━━━━━━━━━━━━━━━━━━
SECTION 2 — joins[]
━━━━━━━━━━━━━━━━━━━━
Standard INNER join (plain string, backward-compatible):
  "fact_orders.customer_id = dim_customers.customer_id"

4A-2  LEFT join (anti-join pattern — "never ordered", "no orders", "no activity"):
{
  "type": "LEFT",
  "condition": "dim_products.product_id = fact_orders.product_id"
}
When using a LEFT join to find unmatched rows, always pair it with an
IS NULL filter on the FK column:
  { "column": "fact_orders.product_id", "operator": "IS NULL" }
Distinguish carefully:
- "never ordered" / "no orders ever"          → LEFT join + IS NULL filter
- "has not ordered recently" / "no orders in last N days"
  → INNER join + appropriate time_filter / HAVING on last order date.
━━━━━━━━━━━━━━━━━━━━
SECTION 3 — computed_columns[]  (CASE WHEN)
━━━━━━━━━━━━━━━━━━━━
4A-7  Use for: "categorize as High/Medium/Low", "label", "bucket into groups",
      "classify", "weekend vs weekday".
{
  "alias": "spending_tier",
  "when_clauses": [
    { "condition": "SUM(fact_orders.unit_price) > 5000", "then": "'High'"   },
    { "condition": "SUM(fact_orders.unit_price) > 1000", "then": "'Medium'" }
  ],
  "else_value": "'Low'",
  "include_in_group_by": false
}
Rules:
- alias must be snake_case.
- condition must be a valid SQL boolean expression. Use fully-qualified
  table.column references where possible.
- then and else_value must be SQL values: quoted strings ('High'),
  numbers (1), or NULL.
- include_in_group_by: set true ONLY when this CASE WHEN is a grouping
  dimension (e.g. day-of-week bucket), not a metric label.
- String literals in then/else_value must use single quotes: 'High', not "High".
- NEVER add the CASE WHEN expression text to group_by[]. When
  include_in_group_by is true the builder handles GROUP BY placement
  automatically. Adding it manually to group_by[] causes the expression
  to appear twice in SELECT and twice in GROUP BY.
━━━━━━━━━━━━━━━━━━━━
SECTION 4B-1 — window_functions[]  (RANK / ROW_NUMBER / DENSE_RANK)
━━━━━━━━━━━━━━━━━━━━
Use for: 'rank within group', 'first per customer', 'top N per category'
{
  "alias": "revenue_rank",
  "function": "RANK",            # RANK | ROW_NUMBER | DENSE_RANK
  "partition_by": ["region"],        # table.col or bare col list
  "order_by": "total_revenue",   # metric alias from metrics[] or column
  "order_dir": "DESC"
}
Rules:
- partition_by may be empty [] for unpartitioned ranking
- order_by should reference a metric alias from metrics[] when ranking
  by an aggregated value (e.g. SUM(revenue))
- DENSE_RANK: no gaps in rank sequence
  RANK: gaps after ties (1,1,3)
  ROW_NUMBER: unique integer per row regardless of ties
━━━━━━━━━━━━━━━━━━━━
SECTION 4B-2 — window_functions[]  (LAG / LEAD)
━━━━━━━━━━━━━━━━━━━━
Use for: 'previous month revenue', 'compare to last period',
         'time between events', '3 consecutive months decline'
{
  "alias": "prev_revenue",
  "function": "LAG",          # LAG | LEAD
  "target_column": "total_revenue", # metric alias or column name
  "offset": 1,               # rows back (LAG) or forward (LEAD)
  "default": 0,               # ALWAYS specify — avoids NULL surprises
  "partition_by": [],
  "order_by": "month",
  "order_dir": "ASC"
}
Rules:
- ALWAYS set 'default' explicitly. Omitting it returns NULL when no prior row.
- LAG looks back (previous row). LEAD looks forward (next row).
- target_column may reference a metric alias from metrics[].
- Dialect note: LAG/LEAD syntax is identical on PostgreSQL, MySQL 8+, SQLite 3.25+.
━━━━━━━━━━━━━━━━━━━━
SECTION 4B-3 — window_functions[]  (Aggregate OVER + frame specs)
━━━━━━━━━━━━━━━━━━━━
Use for: 'running total', 'rolling N-month average', 'cumulative revenue per year'
{
  "alias": "rolling_3m_avg",
  "function": "AVG",             # SUM | AVG | COUNT | MAX | MIN
  "target_column": "total_revenue",   # metric alias or column
  "partition_by": ["dim_products.category"],
  "order_by": "month",
  "order_dir": "ASC",
  "frame_spec": "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
}
Standard frame_spec values:
  Running total  : "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
  Rolling 3-month: "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
  Rolling 7-day  : "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW"
  Rolling 6-month: "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW"
Rules:
- Always use ROWS BETWEEN (not RANGE BETWEEN) for consistent results across dialects.
- target_column may be a metric alias from metrics[].
- partition_by may be empty [] for a global running total.
━━━━━━━━━━━━━━━━━━━━
SECTION 4B-4 — scalar_subquery  (percentage of total)
━━━━━━━━━━━━━━━━━━━━
Use for: '% of total revenue', 'share of category', 'contribution per product line'
{
  "scalar_subquery": {
    "alias": "pct_of_total",
    "numerator_metric": "total_revenue",  # metric alias from metrics[]
    "multiply_by": 100,              # 100 for percentage, omit for raw ratio
    "denominator": {
      "aggregation": "SUM",
      "target_column": "unit_price",
      "fact_table": "fact_orders"
    }
  }
}
Rules:
- numerator_metric must match an alias in metrics[].
- denominator.fact_table defaults to the main fact_table if omitted.
- multiply_by: use 100 for %, omit or set 1 for raw ratio.
- The result uses NULLIF to prevent division-by-zero.
━━━━━━━━━━━━━━━━━━━━
SECTION 4B-5 — set_operation  (INTERSECT / EXCEPT)
━━━━━━━━━━━━━━━━━━━━
Use for: 'customers who bought BOTH X and Y', 'products in both lists',
         'users who did A but NOT B'
{
  "set_operation": {
    "operator": "INTERSECT",   # INTERSECT | EXCEPT | UNION
    "left": {
      "metrics": [{"metric":"customer_id","aggregation":"COUNT","target_column":"customer_id"}],
      "fact_table": "fact_orders",
      "filters": [{"column":"category","operator":"=","value":"Mountain Bikes"}],
      "group_by": ["fact_orders.customer_id"]
    },
    "right": {
      "metrics": [{"metric":"customer_id","aggregation":"COUNT","target_column":"customer_id"}],
      "fact_table": "fact_orders",
      "filters": [{"column":"category","operator":"=","value":"Road Bikes"}],
      "group_by": ["fact_orders.customer_id"]
    }
  }
}
Rules:
- Both sub-intents must select the same columns in the same order.
- INTERSECT: rows present in both. EXCEPT: rows in left but not right.
- Do NOT set limit on sub-intents inside set_operation.
- Use UNION only if duplicates are intentionally kept.
━━━━━━━━━━━━━━━━━━━━
SECTION 4C-1 — ctes[]  (WITH … AS sub-intents)
━━━━━━━━━━━━━━━━━━━━
Use for: '360-degree customer view', 'top N per group needing a rank filter',
         'multi-step aggregation where step 1 feeds step 2'
"ctes": [
  {
    "name": "spend_summary",
    "intent": {
      "metrics": [
        {"metric":"total_spend",     "aggregation":"SUM", "target_column":"unit_price"},
        {"metric":"last_order_date", "aggregation":"MAX", "target_column":"order_date"}
      ],
      "fact_table": "fact_orders",
      "group_by": ["fact_orders.customer_id"],
      "joins": [],
      "filters": []
    }
  }
]
Rules:
- Each sub-intent is a complete, valid intent dict.
- Do NOT set limit inside a CTE sub-intent.
- CTE name must be snake_case and unique within the ctes[] array.
- The main query references CTE names in fact_table or joins[].
- Do NOT create circular references.
━━━━━━━━━━━━━━━━━━━━
SECTION 4C-2 — correlated_filter  (correlated subquery in WHERE)
━━━━━━━━━━━━━━━━━━━━
Use for: 'more expensive than category average', 'above their own group average',
         'spent before their membership date'
{
  "correlated_filter": {
    "column": "dim_products.unit_price",
    "operator": ">",
    "subquery": {
      "aggregation": "AVG",
      "target_column": "unit_price",
      "fact_table": "dim_products",
      "where_col": "category",
      "outer_ref": "dim_products.category"
    }
  }
}
Rules:
- outer_ref must be a fully-qualified table.column from the main query.
- where_col is the column inside the subquery table that must match outer_ref.
- Use this only when a WHERE condition depends on an aggregate of the same table.
- Valid operators: =  >  <  >=  <=
━━━━━━━━━━━━━━━━━━━━
SECTION 4C-3 — Advanced Patterns
━━━━━━━━━━━━━━━━━━━━
Pattern X5 — HAVING COUNT(DISTINCT EXTRACT(...))
Use for: 'active every month of the year', 'ordered in all 12 months'
"having": [{
  "aggregation": "COUNT",
  "target_column": "EXTRACT(month FROM fact_orders.order_date)",
  "distinct": true,
  "operator": "=",
  "value": 12
}]
Note: EXTRACT(...) in target_column is allowed — the validator bypasses the
schema check for expression strings.

Pattern W8 — CASE WHEN with date-distance conditions
Use for: 'tenure groups', '0-6 months / 6-12 months buckets'
Use computed_columns[] with EXTRACT or date-diff expressions in the condition.
The condition field is a SQL boolean — it is NOT subject to the literal_regex.
The then/else_value fields must still be quoted strings or numbers.
{
  "alias": "tenure_group",
  "when_clauses": [
    {"condition": "EXTRACT(month FROM AGE(NOW(), signup_date)) <= 6",  "then": "'0-6m'"},
    {"condition": "EXTRACT(month FROM AGE(NOW(), signup_date)) <= 12", "then": "'6-12m'"}
  ],
  "else_value": "'12m+'",
  "include_in_group_by": true
}

Pattern W10 — Date interval boolean filter
Use for: 'completed within 168 hours', 'within 7 days of signup'
Use a standard filter with the interval expression as the value string.
PostgreSQL: {"column": "trip_ts", "operator": "<=", "value": "signup_ts + INTERVAL '168 hours'"}
MySQL:      {"column": "trip_ts", "operator": "<=", "value": "DATE_ADD(signup_ts, INTERVAL 168 HOUR)"}
SQLite:     {"column": "trip_ts", "operator": "<=", "value": "datetime(signup_ts, '+7 days')"}
━━━━━━━━━━━━━━━━━━━━
SECTION 4 — filters[]
━━━━━━━━━━━━━━━━━━━━
Standard filter:
  { "column": "customer_state", "operator": "=",    "value": "SP" }
  { "column": "freight",        "operator": ">",    "value": 100  }
  { "column": "status",         "operator": "IN",   "value": ["shipped","delivered"] }
  { "column": "name",           "operator": "LIKE", "value": "%Smith%" }
4A-1  NULL checks (no "value" field):
  { "column": "email",    "operator": "IS NULL"     }
  { "column": "email",    "operator": "IS NOT NULL" }
Valid operators: =  >  <  >=  <=  LIKE  IN  IS NULL  IS NOT NULL
━━━━━━━━━━━━━━━━━━━━
SECTION 5 — having[]
━━━━━━━━━━━━━━━━━━━━
Form B — references a named metric (existing behaviour):
  { "metric": "total_revenue", "operator": ">", "value": 50000 }
  metric must exactly match a name in metrics[].

4A-6  Form A — standalone aggregation NOT in SELECT:
{
  "aggregation": "COUNT",
  "target_column": "product_type",
  "distinct": true,
  "operator": ">",
  "value": 3
}
Use Form A for: "more than 3 distinct product types", "spans all categories",
                "ordered in every month" — conditions where the aggregated
                column does not appear as a SELECT metric.
Valid having operators: =  >  <  >=  <=
━━━━━━━━━━━━━━━━━━━━
SECTION 6 — group_by
━━━━━━━━━━━━━━━━━━━━
- Always prefix with table name: "dim_customers.customer_state"
- Every table in group_by must have a matching join.
4A-3  EXTRACT expressions — use when grouping by date part:
  "EXTRACT(month FROM fact_orders.order_date) AS order_month"
  "EXTRACT(dow   FROM fact_orders.order_date) AS day_of_week"
Valid EXTRACT parts: year  month  day  week  quarter  dow  hour  minute
DOW values: 0=Sunday, 1=Monday, … 6=Saturday (PostgreSQL convention).
When time_bucket is set, do NOT also add the raw date column to group_by.
━━━━━━━━━━━━━━━━━━━━
SECTION 7 — time_filter / time_bucket
━━━━━━━━━━━━━━━━━━━━
Relative: { "column": "order_date", "range": "last_7_days|last_30_days|last_month|last_year|this_year" }
Specific year: { "column": "order_date", "year": 2017 }
time_bucket: "day" | "week" | "month" | "quarter" | "year" | null
time_bucket_column: exact date/timestamp column. Required when time_bucket is set.
━━━━━━━━━━━━━━━━━━━━
SECTION 8 — order_by / order_dir / limit
━━━━━━━━━━━━━━━━━━━━
order_by: exact metric name from metrics[] or null.
  Never include ASC/DESC here — use order_dir.
  Leave null for time-bucketed queries (chronological default).
order_dir: "DESC" (default) | "ASC"
limit: integer 1–1000. Default 10.
━━━━━━━━━━━━━━━━━━━━
DECISION TREE — which features to use
━━━━━━━━━━━━━━━━━━━━
"average delay / time between"         → 4A-4 date_arithmetic metric
"never ordered / no activity"          → 4A-2 LEFT join + IS NULL filter
"missing email / no email"             → 4A-1 IS NULL filter
"categorize / High Medium Low"         → 4A-7 computed_columns CASE WHEN
"weekend vs weekday"                   → 4A-7 CASE WHEN (DOW condition) +
                                          include_in_group_by: true
"median"                               → 4A-5 PERCENTILE_CONT(0.5)
"decile / segment into N groups"       → 4A-5 NTILE
"more than N distinct product types"   → 4A-6 Form A having
"spans all categories"                 → 4A-6 Form A having COUNT(DISTINCT) =
                                          (subquery — emit in having condition)
"rank within group / top N per region" → 4B-1 window_functions RANK/ROW_NUMBER
"first per customer / most recent"     → 4B-1 window_functions ROW_NUMBER
"previous month / compare to last"     → 4B-2 window_functions LAG
"next period / look ahead"             → 4B-2 window_functions LEAD
"running total / cumulative"           → 4B-3 window_functions SUM OVER +
                                          frame_spec UNBOUNDED PRECEDING
"rolling N-month average"              → 4B-3 window_functions AVG OVER +
                                          frame_spec N-1 PRECEDING
"% of total / contribution share"      → 4B-4 scalar_subquery
"bought both X and Y"                  → 4B-5 set_operation INTERSECT
"did A but not B"                      → 4B-5 set_operation EXCEPT
"360-degree view / multi-step agg"     → 4C-1 ctes[]
"more expensive than category avg"     → 4C-2 correlated_filter
"ordered every month of the year"      → 4C-3 X5: having COUNT(DISTINCT EXTRACT)
"tenure group / 0-6 months bucket"     → 4C-3 W8: computed_columns date condition
"completed within N hours of signup"   → 4C-3 W10: filter with interval expression

Compound example:
- "Premium vs Standard revenue for US customers"
  → filters[] (country='US')
  + computed_columns[] (CASE WHEN Premium vs Standard)
  + metrics[] (SUM revenue)
All list fields must be [] (not null) when empty.
If the question is genuinely ambiguous, set clarification_needed.
Always include "confidence": "high" | "medium" | "low".
Return ONLY the JSON object. No prose, no markdown fences."""

# ─────────────────────────────────────────────────────────────────────────────
# Retry prompt — fires when generated SQL fails at execution (DB error)
RETRY_PROMPT = """\
The following SQL query failed with an error.

Failed SQL:
{sql}

Error:
{error}

Original intent JSON:
{intent}

Fix the intent JSON to resolve this specific error. Return ONLY the corrected JSON object.
Change the minimum number of fields needed to fix the problem.

Critical rules for the corrected JSON:
- "metrics" must be a list of objects with aggregation, target_column, etc.
- "limit" must be between 1 and 1000.
- "having" entries: use "metric" (matching metrics[]) OR "aggregation"+"target_column".
- "time_bucket" must be null or one of: day, week, month, quarter, year.
- "time_filter" for years: {{"column": "...", "year": 2017}} — not a range string.
- IS NULL / IS NOT NULL filters must not have a "value" field.
- computed_columns entries must have "alias" (snake_case) and "when_clauses" (non-empty list).
- Always include "confidence": "high" | "medium" | "low".
- Preserve all fields. Only change what is needed to fix the error."""

# ─────────────────────────────────────────────────────────────────────────────
# Validation retry prompt — fires when validator.py rejects the intent
VALIDATION_RETRY_PROMPT = """\
The intent JSON you produced failed schema validation before any SQL was built.

Original question: {question}

Failed intent JSON:
{intent}

Validation errors:
{errors}

Fix ONLY the fields that caused these validation errors.
Do not change anything else.

Rules:
- Every column reference must exist in the schema provided.
- "aggregation" must be one of: SUM, COUNT, AVG, MAX, MIN, NTILE, PERCENTILE_CONT.
- "group_by" entries must be "table.column" format.
- "joins" entries must reference real FK relationships.
- "alias" fields must be snake_case (letters, digits, underscores only).
- Always include "confidence": "high" | "medium" | "low".
Return ONLY the corrected JSON object."""


# ─────────────────────────────────────────────────────────────────────────────

def _strip_meta(intent: dict) -> dict:
    """
    Remove observability-only fields before the intent reaches
    validator.py or sql_builder.py. These fields are logged/displayed
    by the pipeline but must never reach schema validation or SQL rendering.
    """
    META_FIELDS = {"confidence", "reasoning"}
    return {k: v for k, v in intent.items() if k not in META_FIELDS}


def parse_intent(question: str, schema: str, history: list, model_config: dict) -> dict:
    """Convert a natural-language question into a structured intent dict.

    Injects the database schema and up to three recent query intents as
    context, then dispatches to the configured LLM backend. Returns the raw
    model response including the 'confidence' field; callers must invoke
    _strip_meta() before passing the result to validator.py or sql_builder.py.

    Args:
        question: The user's natural-language question.
        schema: Database schema string injected verbatim into the prompt.
        history: List of previous intent dicts used for follow-up resolution.
            Only the three most recent entries are sent to the model.
        model_config: Provider configuration dict with keys 'provider', 'model',
            and optionally 'api_key', 'host', 'endpoint', 'api_version'.

    Returns:
        Raw intent dict as returned by the LLM, including 'confidence' and
        optional 'reasoning' observability fields.

    Raises:
        ValueError: If the model returns invalid JSON.
        RuntimeError: If a required API key or library is missing for the
            configured provider.
    """
    history_context = ""
    if history:
        history_context = "\nRecent query intents:\n"
        for h in history[-3:]:
            metrics = h.get("metrics") or []
            metric_str = (
                ", ".join(m.get("metric", "?") for m in metrics[:2])
                if metrics else h.get("metric", "?")
            )
            history_context += (
                f"- metrics: {metric_str}, "
                f"table: {h.get('fact_table')}, "
                f"group_by: {h.get('group_by')}\n"
            )

    user_prompt = (
        f"Database Schema:\n{schema}\n"
        f"{history_context}\n"
        f"Question: {question}\n\n"
        f"Output only the JSON intent:"
    )

    raw_intent = _call_model(INTENT_SYSTEM_PROMPT, user_prompt, model_config)
    # Preserve confidence for pipeline display, but return full dict.
    # Caller must call _strip_meta() before passing to validator/builder.
    return raw_intent


def parse_retry(
    failed_sql: str,
    error_msg: str,
    previous_intent: dict,
    model_config: dict,
) -> dict:
    """Re-parse an intent after the generated SQL failed at execution.

    Sends the failed SQL, the DB error, and the original intent back to the
    LLM so it can produce a corrected intent with the minimum set of changes
    needed to fix the problem. Distinct from parse_validation_retry, which
    fires before any SQL is built.

    Args:
        failed_sql: The SQL string that caused the DB error.
        error_msg: The error message returned by the database driver.
        previous_intent: The intent dict that produced the failing SQL.
        model_config: Provider configuration dict (same shape as parse_intent).

    Returns:
        Corrected intent dict. May still require validation before use.

    Raises:
        ValueError: If the model returns invalid JSON.
        RuntimeError: If a required API key or library is missing.
    """
    user_prompt = RETRY_PROMPT.format(
        sql=failed_sql,
        error=error_msg,
        intent=json.dumps(previous_intent, indent=2),
    )
    return _call_model(INTENT_SYSTEM_PROMPT, user_prompt, model_config)


def parse_validation_retry(
    question: str,
    failed_intent: dict,
    validation_errors: list,
    model_config: dict,
) -> dict:
    """Re-parse an intent after validator.py rejects it pre-execution.

    Sends the original question, the failed intent, and the specific
    validation error messages back to the LLM for a targeted correction,
    without consuming an execution retry attempt.

    Args:
        question: The original user question.
        failed_intent: The intent dict that failed schema validation.
        validation_errors: List of error strings from validate_intent().
        model_config: Provider configuration dict (same shape as parse_intent).

    Returns:
        Corrected intent dict ready for re-validation.

    Raises:
        ValueError: If the model returns invalid JSON.
        RuntimeError: If a required API key or library is missing.
    """
    user_prompt = VALIDATION_RETRY_PROMPT.format(
        question=question,
        intent=json.dumps(failed_intent, indent=2),
        errors="\n".join(f"- {e}" for e in validation_errors),
    )
    return _call_model(INTENT_SYSTEM_PROMPT, user_prompt, model_config)


def _call_model(system_prompt: str, user_prompt: str, model_config: dict) -> dict:
    """Dispatch a prompt pair to the configured LLM backend and parse the JSON response.

    Supports Ollama (default), OpenAI, Gemini (OpenAI-compatible endpoint),
    Anthropic, xAI Grok, OpenRouter, and Azure OpenAI. Strips markdown code
    fences from the raw response before JSON parsing, since some models wrap
    JSON in ```json blocks despite being instructed not to.

    Args:
        system_prompt: The system-role prompt (e.g. INTENT_SYSTEM_PROMPT).
        user_prompt: The user-role prompt containing schema + question.
        model_config: Dict with keys:
            provider  — one of 'ollama', 'openai', 'gemini', 'anthropic',
                        'xai', 'openrouter', 'azure'
            model     — model name or Azure deployment name
            api_key   — required for all non-Ollama providers
            host      — Ollama host URL (default: http://localhost:11434)
            endpoint  — Azure OpenAI endpoint URL
            api_version — Azure API version string

    Returns:
        Parsed intent dict from the model's JSON response.

    Raises:
        RuntimeError: If a required library (openai, anthropic) is not installed,
            or if a required config value (api_key, endpoint) is missing.
        ValueError: If the model returns text that cannot be parsed as JSON.
    """
    provider = model_config.get("provider", "ollama")

    if provider == "openai":
        if openai is None:
            raise RuntimeError(
                "MODEL_PROVIDER is 'openai' but the 'openai' package is not installed. "
                "Run: pip install openai"
            )
        if not model_config.get("api_key"):
            raise RuntimeError(
                "MODEL_PROVIDER is 'openai' but OPENAI_API_KEY is missing from .env"
            )
        client   = openai.OpenAI(api_key=model_config["api_key"], timeout=60.0)
        response = client.chat.completions.create(
            model=model_config["model"],
            messages=[
                {"role": "system",  "content": system_prompt},
                {"role": "user",    "content": user_prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content.strip() if response.choices[0].message.content else "{}"

    elif provider == "gemini":
        # Gemini exposes an OpenAI-compatible endpoint — no new library needed.
        if openai is None:
            raise RuntimeError(
                "MODEL_PROVIDER is 'gemini' but the 'openai' package is not installed. "
                "Run: pip install openai"
            )
        if not model_config.get("api_key"):
            raise RuntimeError(
                "MODEL_PROVIDER is 'gemini' but GEMINI_API_KEY is missing from .env"
            )
        client = openai.OpenAI(
            api_key=model_config["api_key"],
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            timeout=60.0,
        )
        response = client.chat.completions.create(
            model=model_config["model"],          # e.g. gemini-2.5-flash-lite-preview-06-17
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content.strip() if response.choices[0].message.content else "{}"

    elif provider == "anthropic":
        if anthropic is None:
            raise RuntimeError(
                "MODEL_PROVIDER is 'anthropic' but the 'anthropic' package is not installed. "
                "Run: pip install anthropic"
            )
        if not model_config.get("api_key"):
            raise RuntimeError(
                "MODEL_PROVIDER is 'anthropic' but ANTHROPIC_API_KEY is missing from .env"
            )
        client  = anthropic.Anthropic(api_key=model_config["api_key"])
        message = client.messages.create(
            model=model_config["model"],
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = message.content[0].text.strip() if message.content and message.content[0].text else "{}"

    elif provider == "xai":
        # xAI Grok — OpenAI-compatible endpoint, no extra library needed.
        if openai is None:
            raise RuntimeError(
                "MODEL_PROVIDER is 'xai' but the 'openai' package is not installed. "
                "Run: pip install openai"
            )
        if not model_config.get("api_key"):
            raise RuntimeError("MODEL_PROVIDER is 'xai' but XAI_API_KEY is missing from .env")
        client = openai.OpenAI(
            api_key=model_config["api_key"],
            base_url="https://api.x.ai/v1",
            timeout=60.0,
        )
        response = client.chat.completions.create(
            model=model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content.strip() if response.choices[0].message.content else "{}"

    elif provider == "openrouter":
        # OpenRouter — OpenAI-compatible, requires HTTP-Referer + X-Title headers.
        if openai is None:
            raise RuntimeError(
                "MODEL_PROVIDER is 'openrouter' but the 'openai' package is not installed. "
                "Run: pip install openai"
            )
        if not model_config.get("api_key"):
            raise RuntimeError(
                "MODEL_PROVIDER is 'openrouter' but OPENROUTER_API_KEY is missing from .env"
            )
        client = openai.OpenAI(
            api_key=model_config["api_key"],
            base_url="https://openrouter.ai/api/v1",
            timeout=60.0,
            default_headers={
                "HTTP-Referer": "https://dataloom.app",
                "X-Title":      "Dataloom",
            },
        )
        # Not all OpenRouter models support response_format; omit it and rely
        # on the existing JSON-extraction logic below.
        response = client.chat.completions.create(
            model=model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0,
        )
        raw = response.choices[0].message.content.strip() if response.choices[0].message.content else "{}"

    elif provider == "azure":
        # Azure OpenAI — slightly different client constructor.
        # config["model"] is the *deployment name*, not the model family name.
        if openai is None:
            raise RuntimeError(
                "MODEL_PROVIDER is 'azure' but the 'openai' package is not installed. "
                "Run: pip install openai"
            )
        if not model_config.get("api_key"):
            raise RuntimeError(
                "MODEL_PROVIDER is 'azure' but AZURE_OPENAI_API_KEY is missing from .env"
            )
        if not model_config.get("endpoint"):
            raise RuntimeError(
                "MODEL_PROVIDER is 'azure' but AZURE_OPENAI_ENDPOINT is missing from .env"
            )
        client = openai.AzureOpenAI(
            api_key=model_config["api_key"],
            azure_endpoint=model_config["endpoint"],
            api_version=model_config.get("api_version", "2024-02-01"),
            timeout=60.0,
        )
        response = client.chat.completions.create(
            model=model_config["model"],   # deployment name
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content.strip() if response.choices[0].message.content else "{}"

    else:
        # Ollama — uses Client(host=...) so a non-default host is respected.
        host = model_config.get("host", "http://localhost:11434")
        client = ollama.Client(host=host)
        response = client.chat(
            model=model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            options={"temperature": 0},
        )
        raw = response.message.content.strip() if response.message.content else "{}"

    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Model returned invalid JSON: {e}\nRaw: {raw[:300]}")
