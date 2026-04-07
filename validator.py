import re

# =============================================================================
# validator.py  —  Dataloom v3.0  (Sprint 4A)
#
# Sprint 4A additions (marked with # 4A-N):
#   4A-1  IS NULL / IS NOT NULL operators
#   4A-2  Typed JOINs: LEFT / RIGHT / FULL (anti-join support)
#   4A-3  EXTRACT / computed-expression bypass in group_by
#   4A-4  Date-arithmetic metrics  (is_expression + date_arithmetic block)
#   4A-5  NTILE and PERCENTILE_CONT aggregations
#   4A-6  Standalone HAVING aggregation (COUNT DISTINCT not in SELECT)
#   4A-7  computed_columns[]  (CASE WHEN expressions)
# =============================================================================

# ── Constant sets ─────────────────────────────────────────────────────────────

VALID_AGGREGATIONS = {
    "SUM", "COUNT", "AVG", "MAX", "MIN",
    "NTILE",           # 4A-5  NTILE(n) OVER (ORDER BY …)
    "PERCENTILE_CONT", # 4A-5  PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY …)
}

# 4A-1: IS NULL / IS NOT NULL require no value — handled separately in builder
VALID_OPERATORS = {
    "=", ">", "<", ">=", "<=", "LIKE", "IN",
    "IS NULL", "IS NOT NULL",              # 4A-1
}

VALID_HAVING_OPS  = {"=", ">", "<", ">=", "<="}
VALID_ORDER_DIRS  = {"DESC", "ASC"}
VALID_TIME_RANGES = {
    "last_7_days", "last_30_days", "last_month", "last_year", "this_year",
}
VALID_TIME_BUCKETS = {"day", "week", "month", "quarter", "year"}

# 4A-2: join_type field on join objects
VALID_JOIN_TYPES = {"INNER", "LEFT", "RIGHT", "FULL"}

# 4A-4: date_arithmetic.operation values
VALID_DATE_OPS = {"diff_days", "diff_hours", "diff_seconds"}

VALID_WINDOW_FUNCTIONS = {
    "RANK", "ROW_NUMBER", "DENSE_RANK",
    "LAG", "LEAD",
    "SUM", "AVG", "COUNT", "MAX", "MIN",
}

VALID_FRAME_UNITS = {"ROWS", "RANGE"}

NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric", "decimal",
    "real", "double precision", "float", "money",
}


# ── Join graph ────────────────────────────────────────────────────────────────
# Format: { table: { neighbor_table: "join_condition" } }
# Undirected adjacency list — each FK edge stored in both directions.
_JOIN_GRAPH: dict = {}


def set_join_paths(paths: dict) -> None:
    """
    Load the FK graph from schema.py.

    Accepts two formats:
      Adjacency list (new): { table: { neighbor: "condition" } }
      Flat (old):           { dim_table: "condition_string" }
    Old format is auto-promoted for backward compatibility.
    """
    global _JOIN_GRAPH
    if not paths:
        _JOIN_GRAPH = {}
        return

    first_val = next(iter(paths.values()))
    if isinstance(first_val, str):
        graph: dict = {}
        for table, condition in paths.items():
            parts = [p.strip() for p in condition.split("=")]
            if len(parts) == 2:
                left_t   = parts[0].split(".")[0].strip()
                right_t  = parts[1].split(".")[0].strip()
                neighbor = left_t if left_t != table else right_t
                graph.setdefault(table,    {})[neighbor] = condition
                graph.setdefault(neighbor, {})[table]    = condition
        _JOIN_GRAPH = graph
    else:
        _JOIN_GRAPH = dict(paths)


def find_join_path(fact_table: str, target_table: str) -> list:
    """
    BFS through _JOIN_GRAPH → shortest FK path from fact_table to target_table.

    Returns ordered list of (condition_str, table_to_add) tuples.
    Returns [] if target == fact_table or is unreachable.
    """
    if not _JOIN_GRAPH or target_table == fact_table:
        return []

    from collections import deque
    visited: set = {fact_table}
    queue:  deque = deque([(fact_table, [])])

    while queue:
        current, path = queue.popleft()
        for neighbor, condition in _JOIN_GRAPH.get(current, {}).items():
            if neighbor in visited:
                continue
            new_path = path + [(condition, neighbor)]
            if neighbor == target_table:
                return new_path
            visited.add(neighbor)
            queue.append((neighbor, new_path))

    return []


def auto_repair_joins(intent: dict) -> dict:
    """
    For every table referenced in group_by or in computed_columns WHEN
    conditions that lacks a join, BFS-find the shortest FK path from
    fact_table and insert all intermediate hops in topo order.
    Works for both plain-string and typed-dict joins (4A-2).
    """
    import copy
    import re

    repaired   = copy.deepcopy(intent)
    fact_table = repaired.get("fact_table", "")

    joined_tables: set = {fact_table}
    for join in repaired["joins"]:
        condition = _join_condition(join)
        for part in condition.split("="):
            t = part.strip().split(".")[0].split("::")[0].strip()
            if t:
                joined_tables.add(t)

    # If any existing join is a typed LEFT join, propagate LEFT semantics to
    # BFS-inserted intermediate hops to preserve anti-join behaviour.
    has_left_join = any(_join_type(j) == "LEFT" for j in repaired["joins"])

    def _ensure_join_to(target_table: str) -> None:
        """
        Ensure there is a join path from fact_table to target_table.
        Inserts any missing intermediate hops using BFS if they are not
        already present in joined_tables.
        """
        if not target_table or target_table in joined_tables or target_table == fact_table:
            return

        path = find_join_path(fact_table, target_table)
        for condition, hop_table in path:
            if hop_table not in joined_tables:
                # Use LEFT joins for BFS insertions when the query is already
                # using LEFT joins, so anti-join patterns are preserved.
                join_entry = (
                    {"type": "LEFT", "condition": condition}
                    if has_left_join
                    else condition
                )
                repaired["joins"].append(join_entry)
                joined_tables.add(hop_table)

    # ── 1) Tables referenced directly in group_by ─────────────────────────────
    for col in repaired["group_by"]:
        if "." not in col or _is_expr(col):
            continue
        target = col.split(".")[0]
        _ensure_join_to(target)

    # ── 2) Tables referenced in computed_columns WHEN conditions ──────────────
    table_col_pattern = re.compile(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b")

    for cc in repaired.get("computed_columns") or []:
        for wc in cc.get("when_clauses") or []:
            condition = wc.get("condition") or ""
            # Extract all table.column patterns from the condition expression.
            for match in table_col_pattern.finditer(condition):
                table_name = match.group(1)
                _ensure_join_to(table_name)

    return repaired


# ── Normalizers ───────────────────────────────────────────────────────────────

def normalize_joins(joins: list) -> list:
    """
    Normalize every join entry to one of two canonical forms:

      "fact.col = dim.col"                          ← plain string  (INNER JOIN)
      {"type": "LEFT", "condition": "fact…=dim…"}   ← typed dict    (4A-2)

    INNER joins are kept as plain strings for full backward compatibility.
    LEFT / RIGHT / FULL joins are preserved as typed dicts so _build_joins
    can emit the correct keyword.
    """
    normalized = []
    for join in joins:
        if isinstance(join, str):
            normalized.append(join.strip())
            continue
        if not isinstance(join, dict):
            continue
        condition = (
            join.get("condition") or join.get("on") or
            join.get("on_condition") or join.get("join_condition") or ""
        ).strip()
        if not condition:
            continue
        raw_type  = (join.get("type") or join.get("join_type") or "INNER").upper().strip()
        join_type = raw_type if raw_type in VALID_JOIN_TYPES else "INNER"
        if join_type == "INNER":
            normalized.append(condition)        # stay backward-compatible
        else:
            normalized.append({"type": join_type, "condition": condition})
    return normalized


def normalize_filters(filters: list) -> list:
    """Accept 'comparison' / 'comparator' / 'op' as aliases for 'operator'."""
    normalized = []
    for f in filters:
        if not isinstance(f, dict):
            continue
        fixed = dict(f)
        if "operator" not in fixed:
            for alias in ("comparison", "comparator", "op", "condition"):
                if alias in fixed:
                    fixed["operator"] = fixed.pop(alias)
                    break
        normalized.append(fixed)
    return normalized


def normalize_metrics(intent: dict) -> list:
    """
    Return a canonical list of metric dicts.
    Promotes the old single-metric format (metric/aggregation/target_column)
    to metrics[0] for backward compatibility.
    Mutates intent["metrics"] in place.
    """
    raw = intent.get("metrics")
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return raw

    old_metric   = intent.get("metric")        or ""
    old_agg      = intent.get("aggregation")   or ""
    old_col      = intent.get("target_column") or "*"
    old_distinct = intent.get("distinct", False)

    if old_metric or old_agg:
        promoted = [{
            "metric":        old_metric,
            "aggregation":   old_agg,
            "target_column": old_col,
            "distinct":      old_distinct,
        }]
        intent["metrics"] = promoted
        return promoted

    intent["metrics"] = []
    return []


# ── Main validation entry point ───────────────────────────────────────────────

def humanize_errors(errors: list[str]) -> str:
    """
    Convert a list of raw validator error strings into a single,
    friendly sentence a non-technical user can understand and act on.
    Maps the most common error patterns to plain English.
    Falls back to a generic message if no pattern matches.
    """
    if not errors:
        return "Something went wrong. Try rephrasing your question."

    first = errors[0].lower()

    # ── Column / field not found ──────────────────────────────────
    if "not found in schema" in first or "column" in first and "not found" in first:
        # Extract the column name if present e.g. "group_by column 'xyz' not found"
        import re
        match = re.search(r"""['"]([^'"]+)['"]""", errors[0])
        col = (" \"" + match.group(1) + "\"") if match else ""
        return (
            f"I couldn’t find a column{col} in your database. "
            "Try using the exact column name shown in the sidebar, "
            "or rephrase using terms like ‘orders’, ‘revenue’, or ‘customers’."
        )

    # ── Missing metrics ───────────────────────────────────────────
    if "missing metrics" in first or "aggregation" in first and "invalid" in first:
        return (
            "I couldn’t work out what to measure from your question. "
            "Try being more specific — for example: "
            "‘total revenue by category’ or ‘count of orders per month’."
        )

    # ── Invalid aggregation (e.g. LAG in metrics[]) ───────────────
    if "invalid aggregation" in first:
        import re
        match = re.search(r"""invalid aggregation ['"]([^'"]+)['"]""", errors[0], re.I)
        agg = f" ‘{match.group(1)}’" if match else ""
        return (
            f"I tried to use{agg} in an unsupported way. "
            "Try rephrasing — for example: ‘show revenue compared to the previous month’ "
            "instead of referencing specific SQL functions."
        )

    # ── Filter / operator errors ──────────────────────────────────
    if "filter operator" in first or "not valid" in first and "operator" in first:
        return (
            "I used an invalid filter in your query. "
            "Try rephrasing your condition using words like "
            "‘equal to’, ‘greater than’, ‘less than’, or ‘contains’."
        )

    # ── Missing fact table ────────────────────────────────────────
    if "missing fact_table" in first or "fact_table" in first:
        return (
            "I couldn’t identify which table your question is about. "
            "Try mentioning the subject more clearly — for example: "
            "‘orders’, ‘products’, or ‘customers’."
        )

    # ── Join / relationship errors ────────────────────────────────
    if "join" in first or "unknown table" in first:
        return (
            "I tried to combine data from tables that aren’t directly connected. "
            "Try simplifying your question to focus on one subject at a time."
        )

    # ── Time filter / date errors ─────────────────────────────────
    if "time_filter" in first or "time_bucket" in first or "date" in first:
        return (
            "I had trouble interpreting the time period in your question. "
            "Try being more specific — for example: "
            "‘in 2017’, ‘last 30 days’, or ‘by month’."
        )

    # ── Having / group-by errors ──────────────────────────────────
    if "having" in first or "group_by" in first:
        return (
            "I couldn’t map your question to a valid grouping or filter. "
            "Try rephrasing using exact column names visible in the sidebar."
        )

    # ── Window function errors ────────────────────────────────────
    if "window_function" in first or "partition" in first or "frame" in first:
        return (
            "I had trouble building the time comparison or ranking in your question. "
            "Try rephrasing — for example: "
            "‘show revenue by month with the previous month side by side’."
        )

    # ── CTE / subquery errors ─────────────────────────────────────
    if "cte" in first or "set_operation" in first or "scalar_subquery" in first:
        return (
            "I had trouble building a multi-step query for your question. "
            "Try breaking it into a simpler request first."
        )

    # ── Generic fallback ──────────────────────────────────────────
    return (
        "I couldn’t map your question to the database schema. "
        "Try rephrasing — for example, use the exact column names shown in the sidebar."
    )


def validate_intent(
    intent:      dict,
    schema_map:  dict,
    schema_types: dict | None = None,
) -> tuple[bool, list[str]]:

    errors: list[str] = []

    # ── Validate join types BEFORE normalization ─────────────────
    for join in intent.get("joins", []):
        if isinstance(join, dict):
            raw_type = join.get("type", "").lower()
            if raw_type and raw_type not in {t.lower() for t in VALID_JOIN_TYPES}:
                errors.append(f"Invalid join type: {raw_type}")

    # ── Normalise structural fields ───────────────────────────────
    intent["group_by"]           = intent.get("group_by")           or []
    intent["joins"]              = normalize_joins(intent.get("joins", []))
    intent["filters"]            = normalize_filters(intent.get("filters", []))
    intent["having"]             = intent.get("having")             or []
    intent["computed_columns"]   = intent.get("computed_columns")   or []   # 4A-7
    intent["time_bucket"]        = (intent.get("time_bucket")       or "").lower().strip() or None
    intent["time_bucket_column"] = (intent.get("time_bucket_column") or "").strip()        or None
    intent["order_dir"]          = (intent.get("order_dir")         or "DESC").upper()

    # ── Metrics ───────────────────────────────────────────────────
    metrics = normalize_metrics(intent)

    if not metrics:
        errors.append(
            "Missing metrics. Provide at least one metric with aggregation and target_column."
        )
    else:
        seen_aliases: dict = {}
        for i, m in enumerate(metrics):
            if not isinstance(m, dict):
                errors.append(f"metrics[{i}] must be an object.")
                continue

            agg   = (m.get("aggregation") or "").upper()
            col   = m.get("target_column") or ""
            mname = (m.get("metric") or "").strip()

            # 4A-5 ── NTILE
            if agg == "NTILE":
                n = m.get("ntile_buckets")
                if not isinstance(n, int) or n < 2:
                    errors.append(
                        f"metrics[{i}]: NTILE requires 'ntile_buckets' (integer ≥ 2)."
                    )
                if not m.get("order_by_column"):
                    errors.append(
                        f"metrics[{i}]: NTILE requires 'order_by_column'."
                    )
                else:
                    order_col = m.get("order_by_column", "")
                    # If order_by_column is a simple table.column reference (no
                    # function call), verify the column exists in the schema.
                    if order_col and "." in order_col and "(" not in order_col:
                        bare = order_col.split(".")[-1]
                        if not _col_exists(bare, schema_map):
                            errors.append(
                                f"metrics[{i}]: NTILE order_by_column '{order_col}' not found in schema."
                            )

            # 4A-5 ── PERCENTILE_CONT
            elif agg == "PERCENTILE_CONT":
                p = m.get("percentile")
                if p is None:
                    errors.append(
                        f"metrics[{i}]: PERCENTILE_CONT requires 'percentile' (float 0.0–1.0)."
                    )
                elif not isinstance(p, (int, float)) or not (0.0 <= float(p) <= 1.0):
                    errors.append(
                        f"metrics[{i}]: 'percentile' must be a number in [0.0, 1.0]."
                    )
                if col and col != "*":
                    bare = col.split(".")[-1]
                    if not _col_exists(bare, schema_map):
                        errors.append(
                            f"metrics[{i}]: column '{col}' not found in schema."
                        )

            # 4A-4 ── Raw expression / date arithmetic
            elif m.get("is_expression"):
                da = m.get("date_arithmetic")
                if da:
                    op = da.get("operation", "")
                    if op not in VALID_DATE_OPS:
                        errors.append(
                            f"metrics[{i}]: date_arithmetic.operation must be one of "
                            f"{sorted(VALID_DATE_OPS)}."
                        )
                    for field in ("col_a", "col_b"):
                        fc = da.get(field, "")
                        if not fc:
                            errors.append(
                                f"metrics[{i}]: date_arithmetic.{field} is required."
                            )
                        else:
                            bare = fc.split(".")[-1]
                            if not _col_exists(bare, schema_map):
                                errors.append(
                                    f"metrics[{i}]: date_arithmetic.{field} "
                                    f"'{fc}' not found in schema."
                                )
                # Non-date raw expressions: no schema check — expression is trusted
                # as-is (validated by intent_parser prompt constraints).

            # ── Standard aggregation
            else:
                if agg not in VALID_AGGREGATIONS:
                    if any(w in mname.lower()
                           for w in ("count", "number", "how_many", "unique")):
                        agg = "COUNT"
                        metrics[i]["aggregation"] = "COUNT"
                    else:
                        errors.append(
                            f"metrics[{i}]: invalid aggregation '{agg}'. "
                            f"Must be one of {VALID_AGGREGATIONS}."
                        )

                if col and col not in ("*", "count(*)") and "(" not in col:
                    bare       = col.split(".")[-1]
                    fact_table = intent.get("fact_table", "")
                    if not _col_exists(bare, schema_map):
                        errors.append(
                            f"metrics[{i}]: column '{col}' not found in schema."
                        )
                    elif agg in ("SUM", "AVG") and schema_types:
                        col_type = _col_type(bare, fact_table, schema_map, schema_types)
                        if col_type and col_type.lower() not in NUMERIC_TYPES:
                            errors.append(
                                f"metrics[{i}]: cannot use {agg} on '{bare}' "
                                f"(type: {col_type}). Numeric only."
                            )

            # Deduplicate metric names
            if mname:
                if mname in seen_aliases:
                    seen_aliases[mname] += 1
                    metrics[i]["metric"] = f"{mname}_{seen_aliases[mname]}"
                else:
                    seen_aliases[mname] = 1

    # ── fact_table ────────────────────────────────────────────────
    fact_table = intent.get("fact_table")
    if not fact_table:
        errors.append("Missing fact_table.")
    elif fact_table not in schema_map:
        errors.append(
            f"Table '{fact_table}' does not exist. "
            f"Available: {list(schema_map.keys())}"
        )

    # ── Auto-repair joins ─────────────────────────────────────────
    repaired        = auto_repair_joins(intent)
    intent["joins"] = repaired["joins"]

    # ── time_bucket ───────────────────────────────────────────────
    tb     = intent["time_bucket"]
    tb_col = intent["time_bucket_column"]
    if tb:
        if tb not in VALID_TIME_BUCKETS:
            errors.append(
                f"time_bucket '{tb}' not valid. "
                f"Must be one of {VALID_TIME_BUCKETS}."
            )
        if not tb_col:
            tf = intent.get("time_filter")
            if tf and isinstance(tf, dict) and tf.get("column"):
                intent["time_bucket_column"] = tf["column"]
                tb_col = tf["column"]
            else:
                errors.append("time_bucket requires time_bucket_column.")
        if tb_col:
            bare = tb_col.split(".")[-1]
            if not _col_exists(bare, schema_map):
                errors.append(f"time_bucket_column '{tb_col}' not found in schema.")

    # ── group_by + join coverage ──────────────────────────────────
    join_tables = _extract_join_tables(intent["joins"], fact_table or "")
    join_tables.add(fact_table or "")

    for col in intent["group_by"]:
        if _is_expr(col):                      # 4A-3: expression bypass
            continue
        bare       = col.split(".")[-1]
        col_table  = col.split(".")[0] if "." in col else None
        if not _col_exists(bare, schema_map):
            errors.append(f"group_by column '{col}' not found in schema.")
        elif col_table and col_table != fact_table and col_table not in join_tables:
            errors.append(
                f"group_by references '{col_table}' "
                f"but no join to '{col_table}' was provided."
            )

    # ── join conditions ───────────────────────────────────────────
    for join in intent["joins"]:
        condition = _join_condition(join)
        parts = [p.strip() for p in condition.split("=")]
        if len(parts) < 2:
            errors.append(f"Invalid join condition: '{condition}'")
            continue
        for part in parts[:2]:
            table = part.split(".")[0].split("::")[0].strip()
            if table and table not in schema_map:
                errors.append(f"Join references unknown table '{table}'")

    # ── time_filter ───────────────────────────────────────────────
    time_filter = intent.get("time_filter")
    if time_filter:
        if not isinstance(time_filter, dict):
            errors.append("time_filter must be an object.")
        else:
            tf_col   = time_filter.get("column", "")
            tf_range = time_filter.get("range",  "")
            tf_year  = time_filter.get("year")
            bare     = tf_col.split(".")[-1] if tf_col else ""

            if bare and not _col_exists(bare, schema_map):
                errors.append(f"time_filter column '{tf_col}' not found in schema.")
            elif tf_col and "." in tf_col:
                intent["time_filter"]["column"] = bare

            if tf_year is not None:
                try:
                    yr = int(tf_year)
                    if not (2000 <= yr <= 2100):
                        errors.append(
                            f"time_filter year '{tf_year}' must be 2000–2100."
                        )
                    else:
                        intent["time_filter"]["year"] = yr
                        intent["time_filter"].pop("range", None)
                except (TypeError, ValueError):
                    errors.append(
                        f"time_filter year '{tf_year}' is not a valid integer."
                    )
            elif tf_range:
                if tf_range not in VALID_TIME_RANGES:
                    try:
                        yr = int(tf_range)
                        if 2000 <= yr <= 2100:
                            intent["time_filter"]["year"] = yr
                            intent["time_filter"].pop("range", None)
                        else:
                            errors.append(
                                f"time_filter range '{tf_range}' not recognized."
                            )
                    except (TypeError, ValueError):
                        errors.append(
                            f"time_filter range '{tf_range}' not recognized. "
                            f"Valid: {VALID_TIME_RANGES}"
                        )

    # ── filters ───────────────────────────────────────────────────
    for f in intent["filters"]:
        col  = f.get("column", "")
        op   = f.get("operator", "")
        val  = f.get("value")
        bare = col.split(".")[-1] if col else ""

        # 4A-1: IS NULL / IS NOT NULL — column must exist, no value required
        if op in ("IS NULL", "IS NOT NULL"):
            if bare and not _col_exists(bare, schema_map):
                errors.append(f"Filter column '{col}' not found in schema.")
            continue

        if bare and not _col_exists(bare, schema_map):
            errors.append(f"Filter column '{col}' not found in schema.")
        if op and op not in VALID_OPERATORS:
            errors.append(f"Filter operator '{op}' not valid.")

    # ── having ────────────────────────────────────────────────────
    intent["having"] = [h for h in intent["having"] if isinstance(h, dict)]
    valid_metric_names = {m.get("metric", "") for m in metrics if isinstance(m, dict)}

    for h in intent["having"]:
        op  = h.get("operator", "")
        val = h.get("value")

        if op not in VALID_HAVING_OPS:
            errors.append(
                f"having operator '{op}' not valid. "
                f"Must be one of {VALID_HAVING_OPS}."
            )
        if val is None:
            errors.append("having entry missing 'value'.")

        # 4A-6: Form A — standalone aggregation (COUNT DISTINCT etc., not in SELECT)
        if h.get("aggregation"):
            h_agg = (h.get("aggregation") or "").upper()
            h_col = h.get("target_column", "")
            if h_agg not in VALID_AGGREGATIONS:
                errors.append(
                    f"having aggregation '{h_agg}' not valid. "
                    f"Must be one of {VALID_AGGREGATIONS}."
                )
            if h_col and h_col != "*" and not _is_expr(h_col):
                bare = h_col.split(".")[-1]
                if not _col_exists(bare, schema_map):
                    errors.append(f"having column '{h_col}' not found in schema.")
            elif h_col and _is_expr(h_col):
                # Expression bypass — block injection keywords
                _check_expr_injection(h_col, f"having target_column", errors)

        # Form B — references a named metric from metrics[]
        elif h.get("metric"):
            hm = h.get("metric", "")
            if hm not in valid_metric_names:
                errors.append(
                    f"having metric '{hm}' not found in metrics[]. "
                    f"Valid: {sorted(valid_metric_names)}"
                )

        else:
            errors.append(
                "having entry must have 'metric' (a name from metrics[]) "
                "or 'aggregation' (standalone COUNT/SUM etc.)."
            )

    # ── 4A-7: computed_columns (CASE WHEN) ───────────────────────
    _validate_computed_columns(intent, schema_map, errors)

    # ── window_functions ──────────────────────────────────────────
    intent["window_functions"] = intent.get("window_functions") or []
    for i, wf in enumerate(intent["window_functions"]):
        fn    = (wf.get("function") or "").upper()
        alias = (wf.get("alias") or "").strip()
        if not alias:
            errors.append(f"window_functions[{i}]: missing alias.")
        if fn not in VALID_WINDOW_FUNCTIONS:
            errors.append(f"window_functions[{i}]: unknown function {fn!r}.")
        for col in (wf.get("partition_by") or []):
            bare = col.split(".")[-1]
            if not _col_exists(bare, schema_map):
                errors.append(
                    f"window_functions[{i}]: partition_by column {col!r} not in schema."
                )
        # 4B-3: frame_spec format check
        frame = (wf.get("frame_spec") or "").strip().upper()
        if frame:
            if not (frame.startswith("ROWS BETWEEN") or frame.startswith("RANGE BETWEEN")):
                errors.append(
                    f"window_functions[{i}]: frame_spec must start with "
                    f"ROWS BETWEEN or RANGE BETWEEN."
                )

    # ── 4B-4: scalar_subquery ─────────────────────────────────────
    ssq = intent.get("scalar_subquery")
    if ssq:
        if not isinstance(ssq, dict):
            errors.append("scalar_subquery must be a dict.")
        else:
            alias_raw = ssq.get("alias") or ""
            if not alias_raw:
                errors.append("scalar_subquery: missing alias.")
            elif not re.match(r"^[a-z_][a-z0-9_]*$", alias_raw):
                errors.append(
                    f"scalar_subquery: alias {alias_raw!r} must be snake_case."
                )
            # Validate multiply_by is numeric
            mb = ssq.get("multiply_by", 1)
            if mb is not None:
                try:
                    float(mb)
                except (TypeError, ValueError):
                    errors.append(
                        f"scalar_subquery: multiply_by must be a number, got {mb!r}."
                    )
            denom = ssq.get("denominator", {})
            if not denom.get("aggregation"):
                errors.append("scalar_subquery.denominator: missing aggregation.")
            elif (denom.get("aggregation") or "").upper() not in VALID_AGGREGATIONS:
                errors.append(
                    f"scalar_subquery.denominator: aggregation "
                    f"{denom.get('aggregation')!r} not valid."
                )
            if not denom.get("target_column"):
                errors.append("scalar_subquery.denominator: missing target_column.")
            else:
                d_col = denom["target_column"]
                if d_col != "*" and not _is_expr(d_col):
                    bare = d_col.split(".")[-1]
                    if not _col_exists(bare, schema_map):
                        errors.append(
                            f"scalar_subquery.denominator: column {d_col!r} "
                            f"not found in schema."
                        )
            # Validate denominator fact_table if explicitly provided
            d_tbl = denom.get("fact_table")
            if d_tbl and d_tbl not in schema_map:
                errors.append(
                    f"scalar_subquery.denominator: fact_table {d_tbl!r} "
                    f"not in schema."
                )

    # ── 4B-5: set_operation ───────────────────────────────────────
    VALID_SET_OPS = {"INTERSECT", "EXCEPT", "UNION"}
    sop = intent.get("set_operation")
    if sop:
        op = (sop.get("operator") or "").upper()
        if op not in VALID_SET_OPS:
            errors.append(
                f"set_operation.operator {op!r} must be INTERSECT, EXCEPT, or UNION."
            )
        for side in ("left", "right"):
            sub = sop.get(side)
            if not sub or not isinstance(sub, dict):
                errors.append(
                    f"set_operation.{side}: missing or invalid sub-intent."
                )
            else:
                sub_ok, sub_errs = validate_intent(sub, schema_map, schema_types)
                for err in sub_errs:
                    errors.append(f"set_operation.{side}: {err}")

    # ── 4C-1: ctes[] ─────────────────────────────────────────────
    intent["ctes"] = intent.get("ctes") or []
    cte_names: set = set()
    for i, cte in enumerate(intent["ctes"]):
        name = (cte.get("name") or "").strip()
        if not name:
            errors.append(f"ctes[{i}]: missing name.")
            continue
        if name in cte_names:
            errors.append(f"ctes[{i}]: duplicate CTE name {name!r}.")
        cte_names.add(name)
        sub = cte.get("intent")
        if not sub or not isinstance(sub, dict):
            errors.append(f"ctes[{i}] ({name!r}): missing or invalid sub-intent.")
            continue
        # Cycle detection: sub-intent fact_table cannot be another CTE name
        sub_ft = sub.get("fact_table", "")
        if sub_ft in cte_names and sub_ft != name:
            errors.append(
                f"ctes[{i}] ({name!r}): circular reference to CTE {sub_ft!r}."
            )
            continue
        sub_ok, sub_errs = validate_intent(sub, schema_map, schema_types)
        for err in sub_errs:
            errors.append(f"ctes[{i}] ({name!r}): {err}")

    # ── 4C-2: correlated_filter ───────────────────────────────────
    cf = intent.get("correlated_filter")
    if cf:
        if not cf.get("column"):
            errors.append("correlated_filter: missing column.")
        else:
            cf_bare = cf["column"].split(".")[-1]
            if not _col_exists(cf_bare, schema_map):
                errors.append(
                    f"correlated_filter: column {cf['column']!r} not found in schema."
                )
        if cf.get("operator") not in VALID_HAVING_OPS:
            errors.append(
                f"correlated_filter: invalid operator {cf.get('operator')!r}."
            )
        sq = cf.get("subquery", {})
        if not sq.get("aggregation"):
            errors.append("correlated_filter.subquery: missing aggregation.")
        elif sq["aggregation"].upper() not in VALID_AGGREGATIONS:
            errors.append(
                f"correlated_filter.subquery: aggregation "
                f"{sq['aggregation']!r} not valid."
            )
        if not sq.get("target_column"):
            errors.append("correlated_filter.subquery: missing target_column.")
        else:
            sq_bare = sq["target_column"].split(".")[-1]
            if not _col_exists(sq_bare, schema_map):
                errors.append(
                    f"correlated_filter.subquery: target_column "
                    f"{sq['target_column']!r} not found in schema."
                )
        if not sq.get("outer_ref"):
            errors.append("correlated_filter.subquery: missing outer_ref.")
        else:
            # outer_ref must be a fully-qualified table.column — validate both parts
            or_val = sq["outer_ref"]
            if "." not in or_val:
                errors.append(
                    f"correlated_filter.subquery: outer_ref {or_val!r} "
                    f"must be fully qualified (table.column)."
                )
            else:
                or_bare = or_val.split(".")[-1]
                if not _col_exists(or_bare, schema_map):
                    errors.append(
                        f"correlated_filter.subquery: outer_ref {or_val!r} "
                        f"not found in schema."
                    )
        if sq.get("where_col"):
            wc_bare = sq["where_col"].split(".")[-1]
            if not _col_exists(wc_bare, schema_map):
                errors.append(
                    f"correlated_filter.subquery: where_col "
                    f"{sq['where_col']!r} not found in schema."
                )
        # Validate inner fact_table if explicitly provided
        sq_tbl = sq.get("fact_table")
        if sq_tbl and sq_tbl not in schema_map:
            errors.append(
                f"correlated_filter.subquery: fact_table {sq_tbl!r} not in schema."
            )

    # ── order_dir ─────────────────────────────────────────────────
    if intent["order_dir"] not in VALID_ORDER_DIRS:
        errors.append("order_dir must be DESC or ASC.")

    # ── limit (clamp, never reject) ───────────────────────────────
    raw_limit = intent.get("limit")
    try:
        lim = int(raw_limit) if raw_limit is not None else 10
    except (TypeError, ValueError):
        lim = 10
    intent["limit"] = max(1, min(lim, 5000))

    return (len(errors) == 0), errors


# ── 4A-7: computed_columns validator ─────────────────────────────────────────

def _validate_computed_columns(intent: dict, schema_map: dict, errors: list) -> None:
    """
    Validate the computed_columns[] field used for CASE WHEN expressions.

    Each entry schema:
    {
        "alias":              "snake_case_name",          # required
        "when_clauses": [                                  # required, non-empty
            { "condition": "<SQL expression>", "then": "<value or quoted string>" },
            ...
        ],
        "else_value":         "'Low'" | "NULL" | "0",    # optional, default NULL
        "include_in_group_by": false                      # optional, default false
    }
    """
    import re as _re

    computed = intent.get("computed_columns")
    if not computed:
        intent["computed_columns"] = []
        return
    if not isinstance(computed, list):
        errors.append("computed_columns must be a list.")
        intent["computed_columns"] = []
        return

    # Allow only simple SQL literals in THEN / ELSE: quoted strings, numbers, or NULL.
    literal_regex = _re.compile(
        r"""^\s*(
                NULL
              | -?\d+(?:\.\d+)?          # numeric literal
              | '(?:[^']|'')*'           # single-quoted string, '' for escaped '
            )\s*$""",
        _re.IGNORECASE | _re.VERBOSE,
    )

    valid = []
    for i, cc in enumerate(computed):
        px = f"computed_columns[{i}]"
        if not isinstance(cc, dict):
            errors.append(f"{px} must be an object.")
            continue

        alias = (cc.get("alias") or "").strip()
        if not alias:
            errors.append(f"{px}: missing 'alias'.")
            continue
        if not _re.match(r"^[a-z_][a-z0-9_]*$", alias):
            errors.append(
                f"{px}: alias '{alias}' must be snake_case "
                f"(lowercase letters, digits, underscores only)."
            )
            continue

        when_clauses = cc.get("when_clauses")
        if not when_clauses or not isinstance(when_clauses, list):
            errors.append(f"{px}: 'when_clauses' must be a non-empty list.")
            continue

        clause_ok = True
        for j, wc in enumerate(when_clauses):
            wp = f"{px}.when_clauses[{j}]"
            if not isinstance(wc, dict):
                errors.append(f"{wp}: must be an object with 'condition' and 'then'.")
                clause_ok = False
                continue
            condition = wc.get("condition") or ""
            if not condition:
                errors.append(f"{wp}: missing 'condition'.")
                clause_ok = False
            if "then" not in wc:
                errors.append(f"{wp}: missing 'then'.")
                clause_ok = False

            # Block obvious subqueries / DDL/DML in CASE WHEN conditions.
            cond_upper = condition.upper()
            if any(kw in cond_upper for kw in ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "DROP ", ";", "--")):
                errors.append(f"{wp}: 'condition' contains forbidden SQL keywords.")
                clause_ok = False

            # Light structural check: any table.col references must exist in schema
            for match in _re.finditer(
                r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b",
                condition,
            ):
                t, c = match.group(1), match.group(2)
                if t in schema_map and c not in schema_map.get(t, []):
                    errors.append(
                        f"{wp}: condition references unknown column '{t}.{c}'."
                    )

            # THEN must be a simple SQL literal: quoted string, number, or NULL.
            then_val = str(wc.get("then", "")).strip()
            if then_val and not literal_regex.match(then_val):
                errors.append(
                    f"{wp}: 'then' must be a quoted string literal, numeric literal, or NULL."
                )
                clause_ok = False

        if not clause_ok:
            continue

        # Validate else_value content with same literal rules.
        else_val = str(cc.get("else_value", "NULL")).strip()
        if not literal_regex.match(else_val):
            errors.append(
                f"{px}: 'else_value' must be a quoted string literal, numeric literal, or NULL."
            )
            continue

        # Normalise optional fields with safe defaults
        cc.setdefault("else_value", else_val or "NULL")
        cc.setdefault("include_in_group_by", False)
        valid.append(cc)

    intent["computed_columns"] = valid


# ── Private helpers ───────────────────────────────────────────────────────────

def _join_condition(join) -> str:
    """Return the raw ON-condition string from a plain string or typed-join dict."""
    return join["condition"] if isinstance(join, dict) else join


def _join_type(join) -> str:
    """Return the join type keyword ('INNER', 'LEFT', etc.)."""
    return join.get("type", "INNER").upper() if isinstance(join, dict) else "INNER"


# 4A-3: expression-column detection prefix table
_EXPR_PREFIXES = (
    "EXTRACT(", "DATE_PART(", "DATE_TRUNC(", "STRFTIME(",
    "CASE ",    "CAST(",      "COALESCE(",   "NULLIF(",
    "YEAR(",    "MONTH(",     "DAY(",        "WEEK(",
    "TO_CHAR(", "DATEDIFF(",  "JULIANDAY(",
)


def _is_expr(col: str) -> bool:
    """True if col is a SQL expression rather than a raw schema column reference."""
    upper = col.upper().strip()
    return any(upper.startswith(p) for p in _EXPR_PREFIXES)


# Keywords that must never appear in LLM-supplied expression strings.
# Checked wherever _is_expr() grants a schema-validation bypass.
_INJECTION_BLOCKLIST = (
    ";", "DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "TRUNCATE ",
    "EXEC ", "EXECUTE ", "GRANT ", "REVOKE ", "CREATE ", "--", "/*",
)


def _check_expr_injection(expr: str, field_name: str, errors: list) -> None:
    """
    Block SQL injection patterns in LLM-supplied expression strings.
    Called when _is_expr() bypasses the schema column check.
    """
    upper = expr.upper()
    for kw in _INJECTION_BLOCKLIST:
        if kw in upper:
            errors.append(
                f"{field_name}: expression contains forbidden pattern {kw.strip()!r}."
            )
            break  # one error per field is enough


def _col_exists(col: str, schema_map: dict) -> bool:
    col = col.split("::")[0].strip()
    if "." in col:
        table, _, c = col.partition(".")
        return c in schema_map.get(table, [])
    return any(col in cols for cols in schema_map.values())


def _extract_join_tables(joins: list, fact_table: str) -> set:
    """Return the set of non-fact tables reachable via the join list."""
    tables: set = set()
    for join in joins:
        condition = _join_condition(join)
        for part in condition.split("="):
            t = part.strip().split(".")[0].split("::")[0].strip()
            if t != fact_table:
                tables.add(t)
    return tables


def _col_type(
    col: str,
    fact_table: str,
    schema_map: dict,
    schema_types: dict,
) -> str | None:
    for table in [fact_table] + list(schema_map.keys()):
        if table and col in schema_types.get(table, {}):
            return schema_types[table][col]
    return None
