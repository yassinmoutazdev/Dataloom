"""Shared query pipeline for Dataloom.

This module coordinates the transformation of natural language questions into
executable SQL queries. It manages intent parsing, validation, SQL building,
query execution, and result delivery. It serves as the primary interface
for both CLI and Web UI components.
"""
from intent_parser import parse_intent, parse_retry, parse_validation_retry, is_vague_question, _strip_meta
from validator import validate_intent, set_join_paths, humanize_errors
from sql_builder import build_sql
from memory import IntentMemory

# Fields consumed by the pipeline but never by sql_builder.py.
# _strip_meta() (in intent_parser.py) already removes {"confidence", "reasoning"}.
# _strip_builder_fields() removes the remaining pipeline-only fields so the
# builder receives the smallest possible dict — no dead weight on every call.
_BUILDER_ONLY_STRIP = frozenset({"clarification_needed", "_question"})


def _strip_builder_fields(intent: dict) -> dict:
    """Remove all pipeline-only fields before the intent reaches sql_builder.py.

    Covers the fields _strip_meta() leaves behind:
      - clarification_needed: consumed at Stage 3, irrelevant to SQL generation.
      - _question: injected at Stage 5 for ranking detection; not a schema field.

    Call this on the already-stripped clean_intent immediately before build_sql().
    Safe to call multiple times (idempotent).
    """
    return {k: v for k, v in intent.items() if k not in _BUILDER_ONLY_STRIP}


def init_join_paths(join_paths: dict):
    """Register foreign key paths for SQL join generation.

    Args:
        join_paths: A dictionary mapping table pairs to their join keys.
    """
    set_join_paths(join_paths)


def _looks_like_connection_error(exc: Exception) -> bool:
    """Identify if an exception indicates a lost or dropped database connection.

    Uses a heuristic approach to distinguish between SQL syntax/logic errors
    and infrastructure-level failures across multiple database drivers.

    Args:
        exc: The exception caught during query execution.

    Returns:
        True if the error suggests a connection loss, False otherwise.
    """
    try:
        import psycopg2
        if isinstance(exc, (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
        )):
            return True
    except ImportError:
        pass
    try:
        import mysql.connector
        if isinstance(exc, mysql.connector.errors.OperationalError):
            return True
    except ImportError:
        pass
    # sqlite3 connection errors surface as ProgrammingError or OperationalError
    import sqlite3
    if isinstance(exc, (sqlite3.OperationalError, sqlite3.ProgrammingError)):
        msg = str(exc).lower()
        if any(k in msg for k in ("closed", "no such table", "unable to open")):
            return True
    # Fallback: string match for common messages across drivers
    msg = str(exc).lower()
    return any(k in msg for k in (
        "connection", "closed", "broken pipe",
        "server closed", "lost connection", "gone away",
        "ssl connection", "timeout",
    ))


def run_pipeline(
    question: str,
    schema_text: str,
    schema_map: dict,
    schema_types: dict,
    memory: IntentMemory,
    model_config: dict,
    conn,
    db_type: str,
    credentials: dict | None = None,
) -> dict:
    """Execute the full query pipeline for a single natural language question.

    Coordinates intent extraction, validation, SQL building, execution,
    automatic error correction, and result delivery.

    Args:
        question: The user's input question.
        schema_text: Plain-text schema representation for the LLM.
        schema_map: Dictionary mapping tables to their columns and FKs.
        schema_types: Dictionary mapping columns to their data types.
        memory: IntentMemory instance for context-aware queries and history.
        model_config: Configuration for the LLM (model name, temperature, etc.).
        conn: The active database connection object.
        db_type: The database engine type (e.g., 'sqlite', 'postgres', 'mysql').
        credentials: Optional connection details used for automatic reconnection.

    Returns:
        A dictionary containing the query result:
            - success: True if rows were successfully fetched.
            - intent: The final parsed and merged intent object.
            - sql: The final SQL query that was executed.
            - headers: Column names for the result set.
            - rows: The actual data returned by the database.
            - error: Human-readable error message on failure.
            - corrected: True if the query required model self-correction.
            - clarification: Prompt for more info if the question was vague.
            - confidence: The model's reported confidence level.
    """
    from db_connector import run_query, rollback, reconnect
    from schema import invalidate_schema_cache

    result = {
        "success":       False,
        "intent":        None,
        "sql":           None,
        "headers":       [],
        "rows":          [],
        "error":         None,
        "corrected":     False,
        "clarification": None,
        "confidence":    "high",   # populated after intent parse
    }

    # Pre-screen for questions that lack sufficient detail to be answered
    if is_vague_question(question):
        result["clarification"] = "Could you be more specific? For example: 'What is the total revenue this month?' or 'How many orders were placed last week?'"
        return result

    # Extract structured intent from the natural language question
    try:
        recent_intents = [e["intent"] for e in memory.get_recent(3)]
        intent = parse_intent(question, schema_text, recent_intents, model_config)
    except TimeoutError:
        result["error"] = "Model timed out. Try a simpler question or check your model is running."
        return result
    except Exception as e:
        result["error"] = f"Intent parsing failed: {e}"
        return result

    # Always capture confidence to ensure observability even on early returns
    result["confidence"] = (intent.get("confidence") or "high").lower()

    # Model might proactively request more information
    if intent.get("clarification_needed"):
        result["clarification"] = intent["clarification_needed"]
        return result

    # Prevent very short, ambiguous metrics (e.g., "count") from producing meaningless SQL
    metric = (intent.get("metric") or "").lower()
    generic_metrics = {"count", "total", "number", "sum", "avg", "value", "amount"}
    if metric in generic_metrics and len(question.split()) <= 5:
        result["clarification"] = "Could you be more specific? What would you like to measure?"
        return result

    # Merge context from previous turns for conversational continuity
    if memory.is_followup(intent):
        intent = memory.merge_with_previous(intent)

    # Inject question for ranking detection downstream
    intent["_question"] = question
    result["intent"] = intent

    # Validate the intent against the actual database schema structure
    clean_intent = _strip_meta(intent)
    is_valid, errors = validate_intent(clean_intent, schema_map, schema_types)
    if not is_valid:
        result["error"] = humanize_errors(errors)
        result["error_detail"] = errors   # preserved for logging/observability
        return result

    # Construct the SQL query and its parameterized arguments.
    # Strip pipeline-only fields that sql_builder.py does not consume.
    try:
        sql, params = build_sql(_strip_builder_fields(clean_intent), db_type)
        result["sql"] = sql
    except Exception as e:
        result["error"] = (
            "I understood your question but couldn’t construct a valid query. "
            "Try simplifying it or rephrasing."
        )
        result["error_detail"] = [str(e)]
        return result

    # Attempt query execution with infrastructure recovery and model self-correction logic
    try:
        headers, rows = run_query(conn, sql, db_type, params)
    except Exception as first_error:
        rollback(conn, db_type)

        # Infrastructure recovery: reconnect then retry the same SQL
        _is_conn_error = _looks_like_connection_error(first_error)
        if _is_conn_error:
            # Evict the cached schema for this connection — the reconnect may
            # land on a different host (e.g. Railway failover) whose schema
            # should be read fresh rather than served from the stale cache.
            invalidate_schema_cache(conn, db_type)
            try:
                fresh_conn = reconnect(credentials or {})
                # Replace the live connection in the caller's context
                conn = fresh_conn
                result["_reconnected"] = True
                headers, rows = run_query(conn, sql, db_type, params)
                result["_fresh_conn"] = fresh_conn   # surface to app.py for ctx update
            except Exception as reconnect_error:
                result["error"] = (
                    "Lost connection to the database. "
                    "Please refresh the page and reconnect."
                )
                result["error_detail"] = [str(first_error), str(reconnect_error)]
                return result
        else:
            # SQL logic error: ask the model to rewrite the query based on the database error message
            try:
                fixed_intent = parse_retry(sql, str(first_error), intent, model_config)
                is_valid2, errors2 = validate_intent(fixed_intent, schema_map, schema_types)
                if not is_valid2:
                    result["error"] = (
                        "I tried to correct the query automatically but couldn’t find a valid version. "
                        "Try rephrasing your question."
                    )
                    result["error_detail"] = errors2
                    return result
                fixed_intent["_question"] = question
                fixed_sql, fixed_params = build_sql(_strip_builder_fields(fixed_intent), db_type)
                headers, rows = run_query(conn, fixed_sql, db_type, fixed_params)
                result["sql"]       = fixed_sql
                result["intent"]    = fixed_intent
                result["corrected"] = True
                intent = fixed_intent
            except Exception as second_error:
                rollback(conn, db_type)
                result["error"] = (
                    "The query ran into a database error and my automatic correction didn’t fix it. "
                    "Try rephrasing or simplifying your question."
                )
                result["error_detail"] = [str(first_error), str(second_error)]
                return result

    result["headers"] = headers
    result["rows"]    = [list(r) for r in rows]

    # Persist successfully resolved intents to the history store
    memory.add(intent, question)
    result["success"] = True
    return result
