"""
SQL generation layer for the Dataloom query pipeline (v3.0, Sprint 4A).

Owns the translation from a validated intent dict to a parameterised SQL string.
Handles dialect differences for PostgreSQL, MySQL, and SQLite. Depends on
normalize_joins() and normalize_metrics() from validator.py.

Public API:
    build_sql(intent, db_type) → (sql_string, params_list)
"""

# Sprint 4A additions (marked with # 4A-N):
#   4A-1  IS NULL / IS NOT NULL — no placeholder emitted
#   4A-2  Typed JOINs: LEFT / RIGHT / FULL keyword rendering
#   4A-3  EXTRACT dialect matrix + expression-column GROUP BY
#   4A-4  Date-arithmetic expressions (diff_days / diff_hours / diff_seconds)
#   4A-5  NTILE and PERCENTILE_CONT rendering (with SQLite fallback)
#   4A-6  Standalone HAVING aggregation (COUNT DISTINCT not in SELECT)
#   4A-7  computed_columns[] → CASE WHEN … END AS alias in SELECT

import re
from validator import normalize_joins, normalize_metrics, _join_condition, _join_type

# Words that signal the user wants results ranked or sorted by the primary metric.
RANKING_KEYWORDS = [
    "top", "most", "highest", "lowest", "best", "worst",
    "ranked", "leading", "bottom", "least",
]

MAX_GROUP_LIMIT = 5000   # safety cap for grouped queries

# ── Dialect maps ──────────────────────────────────────────────────────────────

# Dialect-specific SQL expressions for relative time range boundaries in WHERE clauses.
TIME_RANGE_SQL = {
    "postgresql": {
        "last_7_days":  "CURRENT_DATE - INTERVAL '7 days'",
        "last_30_days": "CURRENT_DATE - INTERVAL '30 days'",
        "last_month":   "DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'",
        "last_year":    "DATE_TRUNC('year',  CURRENT_DATE) - INTERVAL '1 year'",
        "this_year":    "DATE_TRUNC('year',  CURRENT_DATE)",
    },
    "mysql": {
        "last_7_days":  "DATE_SUB(CURDATE(), INTERVAL 7 DAY)",
        "last_30_days": "DATE_SUB(CURDATE(), INTERVAL 30 DAY)",
        "last_month":   "DATE_SUB(DATE_FORMAT(NOW(),'%Y-%m-01'), INTERVAL 1 MONTH)",
        "last_year":    "DATE_SUB(DATE_FORMAT(NOW(),'%Y-01-01'), INTERVAL 1 YEAR)",
        "this_year":    "DATE_FORMAT(NOW(),'%Y-01-01')",
    },
    "sqlite": {
        "last_7_days":  "date('now','-7 days')",
        "last_30_days": "date('now','-30 days')",
        "last_month":   "date('now','start of month','-1 month')",
        "last_year":    "date('now','start of year','-1 year')",
        "this_year":    "date('now','start of year')",
    },
}

# Dialect-specific date truncation expressions used for time_bucket GROUP BY columns.
DATE_BUCKET_SQL = {
    "postgresql": {
        "day":     "DATE_TRUNC('day',     {col})",
        "week":    "DATE_TRUNC('week',    {col})",
        "month":   "DATE_TRUNC('month',   {col})",
        "quarter": "DATE_TRUNC('quarter', {col})",
        "year":    "DATE_TRUNC('year',    {col})",
    },
    "mysql": {
        "day":     "DATE({col})",
        "week":    "DATE_FORMAT({col},'%Y-%u')",
        "month":   "DATE_FORMAT({col},'%Y-%m')",
        "quarter": "CONCAT(YEAR({col}),'-Q',QUARTER({col}))",
        "year":    "YEAR({col})",
    },
    "sqlite": {
        "day":     "strftime('%Y-%m-%d',{col})",
        "week":    "strftime('%Y-%W',   {col})",
        "month":   "strftime('%Y-%m',   {col})",
        "quarter": "strftime('%Y',{col})||'-Q'||((CAST(strftime('%m',{col}) AS INTEGER)-1)/3+1)",
        "year":    "strftime('%Y',{col})",
    },
}

# 4A-3: EXTRACT dialect matrix
# Format: EXTRACT_SQL[db_type][part] → format-string with {col}
EXTRACT_SQL = {
    "postgresql": {
        "year":    "EXTRACT(year    FROM {col})",
        "month":   "EXTRACT(month   FROM {col})",
        "day":     "EXTRACT(day     FROM {col})",
        "week":    "EXTRACT(week    FROM {col})",
        "quarter": "EXTRACT(quarter FROM {col})",
        "dow":     "EXTRACT(dow     FROM {col})",   # 0=Sun … 6=Sat
        "hour":    "EXTRACT(hour    FROM {col})",
        "minute":  "EXTRACT(minute  FROM {col})",
    },
    "mysql": {
        "year":    "YEAR({col})",
        "month":   "MONTH({col})",
        "day":     "DAY({col})",
        "week":    "WEEK({col})",
        "quarter": "QUARTER({col})",
        "dow":     "DAYOFWEEK({col})",              # 1=Sun … 7=Sat
        "hour":    "HOUR({col})",
        "minute":  "MINUTE({col})",
    },
    "sqlite": {
        "year":    "CAST(strftime('%Y',{col}) AS INTEGER)",
        "month":   "CAST(strftime('%m',{col}) AS INTEGER)",
        "day":     "CAST(strftime('%d',{col}) AS INTEGER)",
        "week":    "CAST(strftime('%W',{col}) AS INTEGER)",
        "quarter": "((CAST(strftime('%m',{col}) AS INTEGER)-1)/3+1)",
        "dow":     "CAST(strftime('%w',{col}) AS INTEGER)",  # 0=Sun … 6=Sat
        "hour":    "CAST(strftime('%H',{col}) AS INTEGER)",
        "minute":  "CAST(strftime('%M',{col}) AS INTEGER)",
    },
}

# 4A-4: Date-arithmetic dialect matrix
# diff_days(a, b) = a - b in fractional days
DATE_ARITH_SQL = {
    "diff_days": {
        "postgresql": "EXTRACT(epoch FROM ({col_a}::timestamptz - {col_b}::timestamptz)) / 86400.0",
        "mysql":      "TIMESTAMPDIFF(SECOND, {col_b}, {col_a}) / 86400.0",
        "sqlite":     "(julianday({col_a}) - julianday({col_b}))",
    },
    "diff_hours": {
        "postgresql": "EXTRACT(epoch FROM ({col_a}::timestamptz - {col_b}::timestamptz)) / 3600.0",
        "mysql":      "TIMESTAMPDIFF(SECOND, {col_b}, {col_a}) / 3600.0",
        "sqlite":     "(julianday({col_a}) - julianday({col_b})) * 24.0",
    },
    "diff_seconds": {
        "postgresql": "EXTRACT(epoch FROM ({col_a}::timestamptz - {col_b}::timestamptz))",
        "mysql":      "TIMESTAMPDIFF(SECOND, {col_b}, {col_a})",
        "sqlite":     "(julianday({col_a}) - julianday({col_b})) * 86400.0",
    },
}

# Readable alias prefixes used when the metric name is a bare aggregation keyword.
AGG_PREFIX = {
    "SUM": "total", "AVG": "avg", "COUNT": "total",
    "MAX": "max",   "MIN": "min",
}


# ── Public API ────────────────────────────────────────────────────────────────

def build_sql(intent: dict, db_type: str = "postgresql") -> tuple[str, list]:
    """
    Build a parameterised SQL query from a validated intent dict.
    Returns (sql_string, params_list).

    Sprint 4A expands the intent surface to include:
      computed_columns[]  — CASE WHEN … END blocks (4A-7)
      LEFT/RIGHT/FULL joins in joins[]  (4A-2)
      IS NULL / IS NOT NULL filters     (4A-1)
      NTILE / PERCENTILE_CONT metrics   (4A-5)
      is_expression metrics             (4A-4)
    """
    fact_table      = intent["fact_table"]
    group_by        = list(intent.get("group_by") or [])
    joins           = normalize_joins(intent.get("joins") or [])
    time_filter     = intent.get("time_filter")
    time_bucket     = (intent.get("time_bucket")        or "").lower().strip() or None
    time_bucket_col = (intent.get("time_bucket_column") or "").strip()         or None
    having          = intent.get("having")    or []
    filters         = intent.get("filters")   or []
    computed_cols   = intent.get("computed_columns") or []   # 4A-7
    limit           = int(intent.get("limit") or 10)
    order_by        = intent.get("order_by")
    order_dir       = (intent.get("order_dir") or "DESC").upper()

    # ── 4B-5: set_operation (INTERSECT / EXCEPT / UNION) ─────────
    sop = intent.get("set_operation")
    if sop:
        operator       = (sop.get("operator") or "INTERSECT").upper()
        left_sql,  lp  = build_sql(sop["left"],  db_type)
        right_sql, rp  = build_sql(sop["right"], db_type)
        left_sql       = re.sub(r"\nLIMIT \d+$", "", left_sql.strip())
        right_sql      = re.sub(r"\nLIMIT \d+$", "", right_sql.strip())
        return f"{left_sql}\n{operator}\n{right_sql}", lp + rp

    # ── 4C-1: collect CTE sub-intents (prepended to final SQL) ───
    _cte_clauses: list = []
    _cte_params:  list = []
    for cte in (intent.get("ctes") or []):
        cte_name   = cte["name"]
        sub_intent = cte["intent"]
        sub_sql, sub_p = build_sql(sub_intent, db_type)
        sub_sql = re.sub(r"\nLIMIT \d+$", "", sub_sql.strip())
        _cte_clauses.append(f"{cte_name} AS (\n{sub_sql}\n)")
        _cte_params.extend(sub_p)

    if isinstance(order_by, str) and order_by.lower() in ("null", "none", ""):
        order_by = None

    # ── Metrics ───────────────────────────────────────────────────
    metrics  = normalize_metrics(intent)
    compiled = _compile_metrics(metrics, fact_table, db_type)
    primary  = compiled[0]

    # ── DATE_TRUNC time bucket ────────────────────────────────────
    bucket_expr  = None
    bucket_alias = "period"
    if time_bucket and time_bucket_col:
        qualified = (
            time_bucket_col if "." in time_bucket_col
            else f"{fact_table}.{time_bucket_col}"
        )
        tmpl = DATE_BUCKET_SQL.get(db_type, DATE_BUCKET_SQL["postgresql"]).get(time_bucket)
        if tmpl:
            bucket_expr  = tmpl.format(col=qualified)
            bucket_alias = f"{time_bucket}_period"

    # ── 4A-7: CASE WHEN computed columns ─────────────────────────
    case_when_parts = _render_computed_columns(computed_cols)
    window_parts = _compile_window_functions(
        intent.get("window_functions") or [], compiled, fact_table, db_type
    )

    # Deduplication guard: if the LLM put a raw CASE WHEN expression into
    # group_by[] AND also declared it in computed_columns with
    # include_in_group_by:true, the builder would render the CASE block twice
    # in SELECT and twice in GROUP BY.  Strip any group_by entry whose
    # upper-stripped content starts with "CASE" when at least one
    # computed_column with include_in_group_by=True exists — the
    # computed_columns path owns both SELECT rendering and GROUP BY placement.
    has_groupby_cc = any(cc.get("include_in_group_by") for cc in computed_cols)
    if has_groupby_cc:
        group_by = [
            g for g in group_by
            if not g.upper().lstrip().startswith("CASE")
        ]

    # ── SELECT clause ─────────────────────────────────────────────
    select_parts = list(group_by)
    if bucket_expr:
        select_parts.append(f"{bucket_expr} AS {bucket_alias}")
    select_parts.extend(case_when_parts)                 # 4A-7: CASE WHENs after dims
    select_parts.extend(window_parts)                    # 4B-1: window functions
    # ── 4B-4: scalar subquery (% of total) ───────────────────────
    ssq_part = _build_scalar_subquery(
        intent.get("scalar_subquery"), compiled, fact_table
    )
    if ssq_part:
        select_parts.append(ssq_part)
    for c in compiled:
        select_parts.append(f"{c['agg_expr']} AS {c['alias']}")
    select_clause = "SELECT " + ",\n       ".join(select_parts)

    # ── FROM + JOINs ──────────────────────────────────────────────
    from_clause  = f"FROM {fact_table}"
    join_clauses = _build_joins(joins, fact_table)       # 4A-2: typed JOIN keywords

    # ── WHERE clause ──────────────────────────────────────────────
    where_parts: list = []
    params:      list = []
    placeholder = "?" if db_type == "sqlite" else "%s"

    if time_filter and isinstance(time_filter, dict):
        tf_col   = time_filter.get("column", "")
        tf_year  = time_filter.get("year")
        tf_range = time_filter.get("range", "")
        q_tf     = tf_col if "." in tf_col else f"{fact_table}.{tf_col}"

        if tf_year is not None:
            yr = int(tf_year)
            if db_type == "postgresql":
                where_parts.append(
                    f"{q_tf} >= '{yr}-01-01' AND {q_tf} < '{yr+1}-01-01'"
                )
            elif db_type == "mysql":
                where_parts.append(f"YEAR({q_tf}) = {yr}")
            else:
                where_parts.append(f"strftime('%Y',{q_tf}) = '{yr}'")
        elif tf_range:
            cutoff = TIME_RANGE_SQL.get(db_type, TIME_RANGE_SQL["postgresql"]).get(tf_range)
            if cutoff:
                where_parts.append(f"{q_tf} >= {cutoff}")

    for f in filters:
        col = f.get("column", "")
        op  = f.get("operator", "")
        val = f.get("value")
        if not col or not op:
            continue
        q_col = col if "." in col else f"{fact_table}.{col}"

        # 4A-1: NULL operators — no placeholder, no params
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{q_col} {op}")
            continue

        if val is None:
            continue

        if op == "IN":
            vals = val if isinstance(val, list) else [val]
            phs  = ", ".join([placeholder] * len(vals))
            where_parts.append(f"{q_col} IN ({phs})")
            params.extend(vals)
        elif op == "LIKE":
            where_parts.append(f"{q_col} LIKE {placeholder}")
            params.append(val)
        else:
            where_parts.append(f"{q_col} {op} {placeholder}")
            params.append(val)

    # ── 4C-2: correlated_filter ───────────────────────────────────
    cf_clause = _build_correlated_filter(
        intent.get("correlated_filter"), fact_table
    )
    if cf_clause:
        where_parts.append(cf_clause)

    # ── GROUP BY clause ───────────────────────────────────────────
    group_by_parts = list(group_by)
    if bucket_expr:
        group_by_parts.append(bucket_expr)

    # 4A-7: dimension-type CASE WHEN columns may also appear in GROUP BY
    for cc in computed_cols:
        if cc.get("include_in_group_by"):
            group_by_parts.append(_case_when_expr(cc))

    group_clause = f"GROUP BY {', '.join(group_by_parts)}" if group_by_parts else ""

    # ── HAVING clause ─────────────────────────────────────────────
    alias_to_agg = {c["alias"]: c["agg_expr"] for c in compiled}
    name_to_agg  = {m.get("metric", ""): c["agg_expr"] for m, c in zip(metrics, compiled)}

    having_parts: list = []
    for h in having:
        if not isinstance(h, dict):
            continue
        h_op  = h.get("operator", "")
        h_val = h.get("value")
        if not h_op or h_val is None:
            continue

        # 4A-6: Form A — standalone aggregation defined in having entry itself
        if h.get("aggregation"):
            h_agg      = h["aggregation"].upper()
            h_col      = h.get("target_column", "*")
            h_distinct = h.get("distinct", False)
            q_col = (
                h_col if h_col == "*" or "." in h_col
                else f"{fact_table}.{h_col}"
            )
            if h_agg == "COUNT" and h_distinct and h_col != "*":
                raw_expr = f"COUNT(DISTINCT {q_col})"
            elif h_agg == "COUNT":
                raw_expr = f"COUNT({q_col})"
            else:
                raw_expr = f"{h_agg}({q_col})"

        # Form B — references a metric from metrics[]
        else:
            h_metric = h.get("metric", "")
            raw_expr = (
                name_to_agg.get(h_metric)
                or alias_to_agg.get(h_metric)
                or primary["agg_expr"]
            )

        having_parts.append(f"{raw_expr} {h_op} {h_val}")

    having_clause = ("HAVING " + "\n  AND ".join(having_parts)) if having_parts else ""

    # ── ORDER BY clause ───────────────────────────────────────────
    effective_group = bool(group_by_parts)
    question_lower  = (intent.get("_question") or "").lower()
    has_ranking     = any(kw in question_lower for kw in RANKING_KEYWORDS)

    if has_ranking and effective_group:
        order_clause = f"ORDER BY {primary['alias']} DESC"
    elif order_by:
        order_alias = _resolve_order_alias(order_by, compiled, primary["alias"])
        clean       = _clean_order_by(order_alias, primary["alias"])
        # Expand the resolved alias to its full aggregation expression so ORDER BY
        # is always in canonical form (e.g. SUM(fact_orders.unit_price) not total_revenue).
        order_expr  = _alias_to_agg_expr(clean, compiled, clean)
        order_clause = (
            "" if not effective_group and clean != primary["alias"]
            else f"ORDER BY {order_expr} {order_dir}"
        )
    elif effective_group:
        order_clause = (
            f"ORDER BY {bucket_alias} ASC" if bucket_expr
            else f"ORDER BY {primary['alias']} DESC"
        )
    else:
        order_clause = ""

    # ── LIMIT ─────────────────────────────────────────────────────
    limit_clause = (
        f"LIMIT {MAX_GROUP_LIMIT}"        if effective_group
        else f"LIMIT {min(limit or 10, 100)}"
    )

    # ── Assemble ──────────────────────────────────────────────────
    parts = [select_clause, from_clause]
    parts.extend(join_clauses)
    if where_parts:
        parts.append("WHERE " + "\n  AND ".join(where_parts))
    if group_clause:
        parts.append(group_clause)
    if having_clause:
        parts.append(having_clause)
    if order_clause:
        parts.append(order_clause)
    parts.append(limit_clause)

    final_sql = "\n".join(parts)

    # ── 4C-1: prepend WITH block if CTEs present ──────────────────
    if _cte_clauses:
        with_block = "WITH " + ",\n     ".join(_cte_clauses)
        final_sql  = with_block + "\n" + final_sql
        params     = _cte_params + params

    return final_sql, params


# ── Private helpers ───────────────────────────────────────────────────────────

def _compile_metrics(metrics: list, fact_table: str, db_type: str = "postgresql") -> list:
    """
    Convert metrics[] into compiled dicts:
      alias    — unique readable SQL alias
      agg_expr — the full aggregation expression (may include OVER clause for 4A-5)

    Sprint 4A extensions:
      4A-4: is_expression + date_arithmetic → raw expression string
      4A-5: NTILE / PERCENTILE_CONT → window / ordered-set expressions
    """
    compiled:     list = []
    seen_aliases: dict = {}

    for m in metrics:
        # Map previously compiled metric aliases to their full aggregation
        # expressions so NTILE can order by the expression even if the LLM
        # emitted an alias name.
        alias_to_expr = {c["alias"]: c["agg_expr"] for c in compiled}
        agg      = (m.get("aggregation") or "COUNT").upper()
        col      = m.get("target_column") or "*"
        mname    = (m.get("metric") or "").strip()
        distinct = m.get("distinct", False)

        # ── 4A-5: NTILE ──────────────────────────────────────────
        if agg == "NTILE":
            n          = m.get("ntile_buckets", 10)
            order_col  = m.get("order_by_column", "")
            order_dir  = (m.get("order_dir") or "DESC").upper()
            # Prefer the full aggregation expression when order_by_column
            # matches a previously defined metric alias; otherwise use the
            # value verbatim (typically a full SQL expression).
            order_expr = alias_to_expr.get(order_col, order_col)
            agg_expr   = f"NTILE({n}) OVER (ORDER BY {order_expr} {order_dir})"

        # ── 4A-5: PERCENTILE_CONT ────────────────────────────────
        elif agg == "PERCENTILE_CONT":
            p         = m.get("percentile", 0.5)
            order_dir = (m.get("order_dir") or "ASC").upper()
            q         = col if "." in col else f"{fact_table}.{col}"

            if db_type == "sqlite":
                # SQLite lacks PERCENTILE_CONT — approximate via ROW_NUMBER subquery.
                # Caller should note this is an approximation.
                agg_expr = _sqlite_percentile_approx(q, p, fact_table)
            elif db_type == "mysql":
                # MySQL does not support PERCENTILE_CONT ... WITHIN GROUP syntax.
                # Fail fast with a clear error instead of emitting invalid SQL.
                raise ValueError(
                    "PERCENTILE_CONT is not supported for MySQL. "
                    "Use PostgreSQL for PERCENTILE_CONT or switch to a different metric."
                )
            else:
                # PostgreSQL and other supported dialects that accept the
                # ordered-set aggregate syntax.
                agg_expr = (
                    f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {q} {order_dir})"
                )

        # ── 4A-4: Date arithmetic ─────────────────────────────────
        elif m.get("is_expression") and m.get("date_arithmetic"):
            da        = m["date_arithmetic"]
            operation = da.get("operation", "diff_days")
            col_a_raw = da.get("col_a", "")
            col_b_raw = da.get("col_b", "")
            col_a     = col_a_raw if "." in col_a_raw else f"{fact_table}.{col_a_raw}"
            col_b     = col_b_raw if "." in col_b_raw else f"{fact_table}.{col_b_raw}"
            tmpl      = DATE_ARITH_SQL.get(operation, {}).get(
                db_type, DATE_ARITH_SQL["diff_days"]["postgresql"]
            )
            inner_expr = tmpl.format(col_a=col_a, col_b=col_b)
            outer_agg  = (m.get("outer_aggregation") or agg or "AVG").upper()
            agg_expr   = f"{outer_agg}({inner_expr})"

        # ── 4A-4: Generic raw expression (no date_arithmetic block) ──
        elif m.get("is_expression"):
            # col is a raw SQL expression — pass through verbatim, no table prefix
            outer_agg = (m.get("outer_aggregation") or agg or "AVG").upper()
            agg_expr  = f"{outer_agg}({col})"

        # ── Standard aggregation ─────────────────────────────────
        else:
            if agg == "COUNT":
                if distinct and col and col != "*":
                    q = col if "." in col else f"{fact_table}.{col}"
                    agg_expr = f"COUNT(DISTINCT {q})"
                else:
                    # COUNT(*) is the canonical form for row counting.
                    # COUNT(col) is only emitted for COUNT DISTINCT — a plain
                    # COUNT(col) silently skips NULLs and diverges from
                    # COUNT(*) semantics, so we never emit it implicitly.
                    agg_expr = "COUNT(*)"
            else:
                q = col if "." in col else f"{fact_table}.{col}"
                agg_expr = f"{agg}({q})"

        # Build unique alias
        base_alias = _build_alias(mname, agg, col)
        alias      = base_alias
        if alias in seen_aliases:
            seen_aliases[alias] += 1
            alias = f"{base_alias}_{seen_aliases[base_alias]}"
        else:
            seen_aliases[alias] = 1

        compiled.append({"alias": alias, "agg_expr": agg_expr, "raw_agg": agg})

    return compiled


def _sqlite_percentile_approx(col: str, p: float, fact_table: str) -> str:
    """
    SQLite approximation for PERCENTILE_CONT(p).
    Uses a correlated subquery pattern:
      (SELECT col FROM (SELECT col, ROW_NUMBER() OVER (ORDER BY col) rn,
                               COUNT(*) OVER () cnt FROM t)
       WHERE rn = CAST(ROUND(cnt * p) AS INTEGER))
    """
    return (
        f"(SELECT {col} FROM "
        f"(SELECT {col}, ROW_NUMBER() OVER (ORDER BY {col}) rn, "
        f"COUNT(*) OVER () cnt FROM {fact_table}) sub "
        f"WHERE rn = MAX(1, CAST(ROUND(cnt * {p}) AS INTEGER)))"
    )


def _compile_window_functions(
    window_fns: list,
    compiled_metrics: list,
    fact_table: str,
    db_type: str = "postgresql",
) -> list:
    """
    Render window_functions[] entries into SELECT expressions.
    Returns list of 'expr AS alias' strings.
    """
    alias_to_expr = {c["alias"]: c["agg_expr"] for c in compiled_metrics}
    parts: list = []
    for wf in window_fns:
        fn        = wf.get("function", "ROW_NUMBER").upper()
        alias     = wf.get("alias", "window_col")
        partition = wf.get("partition_by") or []
        order_col = wf.get("order_by", "")
        order_dir = (wf.get("order_dir") or "DESC").upper()

        # Resolve order_by: may reference a metric alias
        order_expr = alias_to_expr.get(order_col, order_col)

        # Build OVER clause
        over_parts = []
        if partition:
            over_parts.append(f"PARTITION BY {', '.join(partition)}")
        if order_expr:
            over_parts.append(f"ORDER BY {order_expr} {order_dir}")

        # Frame spec (for aggregate windows — handled in 4B-3)
        frame = wf.get("frame_spec")
        if frame:
            over_parts.append(frame)

        over_clause = f"OVER ({' '.join(over_parts)})"

        if fn in ("RANK", "ROW_NUMBER", "DENSE_RANK"):
            parts.append(f"{fn}() {over_clause} AS {alias}")
        elif fn in ("LAG", "LEAD"):
            target  = wf.get("target_column", "")
            offset  = wf.get("offset", 1)
            default = wf.get("default", "NULL")
            # Resolve target: may be a metric alias
            target_expr = alias_to_expr.get(target, target) or target
            # Always use three-arg form: LAG(expr, offset, default)
            parts.append(
                f"{fn}({target_expr}, {offset}, {default}) {over_clause} AS {alias}"
            )
        else:
            # Aggregate window: SUM(col) OVER (...)
            target = wf.get("target_column", "*")
            q = target if "." in target else f"{fact_table}.{target}"
            parts.append(f"{fn}({q}) {over_clause} AS {alias}")
    return parts



def _build_scalar_subquery(
    ssq: dict,
    compiled_metrics: list,
    fact_table: str,
) -> str:
    """
    4B-4: Render a scalar subquery for percentage-of-total calculations.
    e.g.: 100.0 * total_revenue / NULLIF((SELECT SUM(unit_price) FROM fact_orders), 0)
    """
    if not ssq:
        return None
    alias      = ssq.get("alias", "pct_total")
    num_metric = ssq.get("numerator_metric", "")
    multiply   = ssq.get("multiply_by", 1)
    denom      = ssq.get("denominator", {})
    agg        = (denom.get("aggregation") or "SUM").upper()
    col        = denom.get("target_column", "*")
    tbl        = denom.get("fact_table") or fact_table
    q_col      = col if "." in col else f"{tbl}.{col}"
    subq       = f"(SELECT {agg}({q_col}) FROM {tbl})"
    alias_map  = {c["alias"]: c["alias"] for c in compiled_metrics}
    num_expr   = alias_map.get(num_metric, num_metric)
    # Safe numeric conversion — validator already checked this but builder must not crash
    try:
        multiply_int = int(float(multiply)) if multiply is not None else 1
    except (TypeError, ValueError):
        multiply_int = 1
    if multiply_int != 1:
        expr = f"{multiply_int}.0 * {num_expr} / NULLIF({subq}, 0)"
    else:
        expr = f"{num_expr} / NULLIF({subq}, 0)"
    return f"{expr} AS {alias}"


def _build_correlated_filter(cf: dict, fact_table: str) -> str:
    """
    4C-2: Render a correlated WHERE clause:
    outer_col op (SELECT agg(col) FROM tbl WHERE tbl.key = outer_ref)
    """
    if not cf:
        return None
    outer_col = cf.get("column", "")
    op        = cf.get("operator", "=")
    sq        = cf.get("subquery", {})
    agg       = (sq.get("aggregation") or "AVG").upper()
    inner_col = sq.get("target_column", "")
    inner_tbl = sq.get("fact_table") or fact_table
    where_col = sq.get("where_col", "")
    outer_ref = sq.get("outer_ref", "")
    q_inner   = inner_col if "." in inner_col else f"{inner_tbl}.{inner_col}"
    q_where   = where_col if "." in where_col else f"{inner_tbl}.{where_col}"
    subq      = f"(SELECT {agg}({q_inner}) FROM {inner_tbl} WHERE {q_where} = {outer_ref})"
    return f"{outer_col} {op} {subq}"


def _build_joins(joins: list, fact_table: str) -> list:
    """
    Render SQL JOIN clauses in topological order.

    4A-2: Emits 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN' for typed-dict entries;
          falls back to plain 'JOIN' (INNER) for string entries.
    Sprint 3 fix retained: adds whichever side of = is not yet in seen_tables,
    so multi-hop conditions where neither side is the fact table are handled.
    """
    clauses:     list = []
    seen_tables: set  = {fact_table}

    for join in joins:
        condition  = _join_condition(join)
        join_kw    = f"{_join_type(join)} JOIN"   # 4A-2

        parts = [p.strip() for p in condition.split("=")]
        if len(parts) != 2:
            continue
        left_t  = parts[0].split(".")[0].split("::")[0].strip()
        right_t = parts[1].split(".")[0].split("::")[0].strip()

        if right_t not in seen_tables:
            join_table = right_t
        elif left_t not in seen_tables:
            join_table = left_t
        else:
            continue   # both sides already joined — duplicate, skip

        clauses.append(f"{join_kw} {join_table} ON {condition}")
        seen_tables.add(join_table)

    return clauses


def _render_computed_columns(computed_cols: list) -> list:
    """
    4A-7: Convert each computed_columns entry into a CASE WHEN … END AS alias
    string, ready to be inserted into the SELECT clause.

    Each entry:
      alias        — output SQL alias
      when_clauses — [{condition, then}, ...]
      else_value   — fallback (default: NULL)
    """
    parts = []
    for cc in computed_cols:
        lines = ["CASE"]
        for wc in cc.get("when_clauses", []):
            lines.append(f"  WHEN {wc['condition']} THEN {wc['then']}")
        else_val = cc.get("else_value", "NULL")
        lines.append(f"  ELSE {else_val}")
        lines.append(f"END AS {cc['alias']}")
        parts.append("\n  ".join(lines))   # compact multi-line inside SELECT
    return parts


def _case_when_expr(cc: dict) -> str:
    """Return the raw CASE WHEN … END expression (without alias) for GROUP BY use."""
    lines = ["CASE"]
    for wc in cc.get("when_clauses", []):
        lines.append(f"  WHEN {wc['condition']} THEN {wc['then']}")
    lines.append(f"  ELSE {cc.get('else_value', 'NULL')}")
    lines.append("END")
    return "\n  ".join(lines)


def _build_alias(metric_name: str, aggregation: str, target_col: str) -> str:
    """Build a readable, non-generic SQL alias."""
    agg_upper = aggregation.upper()
    if metric_name.upper() in ("SUM", "COUNT", "AVG", "MAX", "MIN", ""):
        col_base = target_col.split(".")[-1].replace("*", "rows") if target_col else "value"
        prefix   = AGG_PREFIX.get(agg_upper, agg_upper.lower())
        return f"{prefix}_{col_base}"
    return re.sub(r"[^a-z0-9_]", "_", metric_name.lower().strip())


def _resolve_order_alias(order_by: str, compiled: list, fallback: str) -> str:
    ob_norm = order_by.lower().strip()
    for c in compiled:
        if c["alias"].lower() == ob_norm:
            return c["alias"]
    for c in compiled:
        if ob_norm in c["alias"].lower() or c["alias"].lower() in ob_norm:
            return c["alias"]
    return fallback


def _alias_to_agg_expr(alias: str, compiled: list, fallback: str) -> str:
    """Resolve a metric alias to its full aggregation expression.

    Used to produce canonical ORDER BY clauses (e.g. ORDER BY SUM(fact_orders.unit_price))
    instead of alias-based ones (ORDER BY total_revenue), ensuring consistent SQL
    semantics across all clauses regardless of how the LLM named the metric.
    Falls back to ``fallback`` when the alias does not match any compiled metric.
    """
    for c in compiled:
        if c["alias"] == alias:
            return c["agg_expr"]
    return fallback


def _clean_order_by(order_by: str, fallback_alias: str) -> str:
    cleaned = re.sub(r"\b(ASC|DESC)\b", "", order_by, flags=re.IGNORECASE).strip().rstrip(",").strip()
    if re.match(r"^\w+\(.+\)$", cleaned):
        return fallback_alias
    return cleaned if cleaned else fallback_alias
