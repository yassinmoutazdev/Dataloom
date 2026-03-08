"""
core.py — shared query pipeline used by both CLI (main.py) and web UI (app.py)
"""
from intent_parser import parse_intent, parse_retry, parse_validation_retry, is_vague_question, _strip_meta
from validator import validate_intent, set_join_paths, humanize_errors
from sql_builder import build_sql
from summarizer import summarize
from memory import IntentMemory


def init_join_paths(join_paths: dict):
    """Call at startup with auto-discovered FK paths from schema.py."""
    set_join_paths(join_paths)


def _looks_like_connection_error(exc: Exception) -> bool:
    """
    Heuristic: returns True if the exception looks like a dropped/lost
    DB connection rather than a SQL logic error.
    Covers psycopg2, mysql-connector, and sqlite3.
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
    credentials: dict = None,
) -> dict:
    """
    Run the full query pipeline for a single question.
    Returns a result dict with keys:
      success, intent, sql, headers, rows, summary, error, corrected, clarification
    """
    from db_connector import run_query, rollback, reconnect

    result = {
        "success":       False,
        "intent":        None,
        "sql":           None,
        "headers":       [],
        "rows":          [],
        "summary":       "",
        "error":         None,
        "corrected":     False,
        "clarification": None,
        "confidence":    "high",   # populated after intent parse
    }

    # 1. Pre-screen vague questions
    if is_vague_question(question):
        result["clarification"] = "Could you be more specific? For example: 'What is the total revenue this month?' or 'How many orders were placed last week?'"
        return result

    # 2. Parse intent (with timeout)
    try:
        recent_intents = [e["intent"] for e in memory.get_recent(3)]
        intent = parse_intent(question, schema_text, recent_intents, model_config)
    except TimeoutError:
        result["error"] = "Model timed out. Try a simpler question or check your model is running."
        return result
    except Exception as e:
        result["error"] = f"Intent parsing failed: {e}"
        return result

    # 3. Capture confidence (always, before any early return)
    result["confidence"] = (intent.get("confidence") or "high").lower()

    # Clarification check
    if intent.get("clarification_needed"):
        result["clarification"] = intent["clarification_needed"]
        return result

    # 4. Post-validation vagueness check
    metric = (intent.get("metric") or "").lower()
    generic_metrics = {"count", "total", "number", "sum", "avg", "value", "amount"}
    if metric in generic_metrics and len(question.split()) <= 5:
        result["clarification"] = "Could you be more specific? What would you like to measure?"
        return result

    # 5. Follow-up detection
    if memory.is_followup(intent):
        intent = memory.merge_with_previous(intent)

    # 6. Inject question for ranking detection
    intent["_question"] = question
    result["intent"] = intent

    # 7. Validate (strip confidence/reasoning before schema checks)
    clean_intent = _strip_meta(intent)
    is_valid, errors = validate_intent(clean_intent, schema_map, schema_types)
    if not is_valid:
        result["error"] = humanize_errors(errors)
        result["error_detail"] = errors   # preserved for logging/observability
        return result

    # 8. Build SQL (returns sql string + params list)
    try:
        sql, params = build_sql(clean_intent, db_type)
        result["sql"] = sql
    except Exception as e:
        result["error"] = (
            "I understood your question but couldn’t construct a valid query. "
            "Try simplifying it or rephrasing."
        )
        result["error_detail"] = [str(e)]
        return result

    # 9. Execute — with connection-recovery + one SQL auto-retry on failure
    try:
        headers, rows = run_query(conn, sql, db_type, params)
    except Exception as first_error:
        rollback(conn, db_type)

        # ── Connection recovery: reconnect then retry the same SQL ──
        _is_conn_error = _looks_like_connection_error(first_error)
        if _is_conn_error:
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
            # ── SQL error: ask model to self-correct ──────────────────
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
                fixed_sql, fixed_params = build_sql(fixed_intent, db_type)
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

    # 10. Summarize (local, with timeout guard)
    try:
        result["summary"] = summarize(
            question, rows[:20], headers, model_config,
            total_rows=len(rows)
        )
    except Exception:
        result["summary"] = ""

    # 11. Save to memory
    memory.add(intent, question)
    result["success"] = True
    return result
