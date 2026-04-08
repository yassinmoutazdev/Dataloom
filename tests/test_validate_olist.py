"""
test_validate_olist.py  —  Dataloom v3.0
Live-database integration tests: runs 10 canonical questions against the
real Olist database and verifies that generated SQL contains expected
patterns and executes without error.

Requires a live database configured via .env.
All tests are skipped gracefully when no DB connection is available, so
the suite can live alongside unit tests without failing CI.

Usage:
    pytest test_validate_olist.py -v
    pytest test_validate_olist.py -v --db=mysql
    pytest test_validate_olist.py -v -k "Q01 or Q07"

To print generated SQL, pass -s to pytest.
"""

import os
import sys
import json
import pytest
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Deferred imports (fail gracefully when codebase is absent) ────────────────
try:
    from intent_parser import parse_intent, _strip_meta
    from validator     import validate_intent, set_join_paths
    from sql_builder   import build_sql
    from schema        import get_schema
    _CODEBASE_AVAILABLE = True
except ImportError as _e:
    _CODEBASE_AVAILABLE = False
    _IMPORT_ERROR = str(_e)


# ── DB type from command-line option ──────────────────────────────────────────

def pytest_addoption(parser):
    parser.addoption("--db", action="store", default="postgresql",
                     choices=["postgresql", "mysql", "sqlite"],
                     help="Database dialect to test against")


@pytest.fixture(scope="session")
def db_type(request):
    """Return the --db dialect flag, defaulting to postgresql."""
    return request.config.getoption("--db", default="postgresql")


# ── The 10 evaluation questions ───────────────────────────────────────────────

# Each entry maps a feature tag to the question text and the SQL patterns
# that must appear in the generated output for the test to pass.
EVAL_QUESTIONS = [
    {
        "id": "Q01", "feature": "4B-2 LAG",
        "question": "For each seller, show their monthly revenue and the previous month's revenue side by side.",
        "must_contain": ["LAG(", "OVER"],
    },
    {
        "id": "Q02", "feature": "4B-3 SUM OVER",
        "question": "Show a running total of order revenue by month across all of 2017.",
        "must_contain": ["SUM(", "ROWS BETWEEN UNBOUNDED PRECEDING"],
    },
    {
        "id": "Q03", "feature": "4B-3 AVG OVER",
        "question": "Calculate the 3-month rolling average order value per product category.",
        "must_contain": ["AVG(", "ROWS BETWEEN 2 PRECEDING"],
    },
    {
        "id": "Q04", "feature": "4B-4 scalar_subquery",
        "question": "What percentage of total 2017 revenue did each product category contribute?",
        "must_contain": ["NULLIF(", "100"],
    },
    {
        "id": "Q05", "feature": "4B-5 EXCEPT",
        "question": ("Which customers placed orders in São Paulo but have never placed "
                     "an order in Rio de Janeiro?"),
        "must_contain": ["EXCEPT"],
    },
    {
        "id": "Q06", "feature": "4B-1 RANK",
        "question": "Rank sellers within each product category by total revenue, showing top 3 per category.",
        "must_contain": ["RANK()", "OVER", "PARTITION BY"],
    },
    {
        "id": "Q07", "feature": "4C-1 CTE",
        "question": ("Show the total orders, total spend, and last order date for every customer "
                     "who spent more than 500."),
        "must_contain": ["WITH ", " AS ("],
    },
    {
        "id": "Q08", "feature": "4C-2 correlated",
        "question": "Find products whose average review score is above the average review score for their category.",
        "must_contain": ["SELECT AVG(", "WHERE"],
    },
    {
        "id": "Q09", "feature": "4C-3 X5",
        "question": "Which customers placed orders in every month of 2017?",
        "must_contain": ["COUNT(DISTINCT", "EXTRACT"],
    },
    {
        "id": "Q10", "feature": "4C-3 W10",
        "question": "How many orders were delivered within 7 days of being placed? Show count per seller state.",
        "must_contain": ["INTERVAL"],
    },
]


# ── Session-scoped DB + schema fixtures ───────────────────────────────────────

def _connect_db(db_type: str):
    """Open a database connection for the given dialect.

    Args:
        db_type: One of ``"postgresql"``, ``"mysql"``, or ``"sqlite"``.

    Returns:
        An open database connection object.

    Raises:
        ValueError: If ``db_type`` is not a recognised dialect.
        ImportError: If the required driver package is not installed.
    """
    if db_type == "postgresql":
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        # Read-only mode prevents accidental writes during test runs.
        conn.set_session(readonly=True)
        return conn
    elif db_type == "mysql":
        import mysql.connector
        return mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
    elif db_type == "sqlite":
        import sqlite3
        return sqlite3.connect(os.getenv("DB_PATH", "database.db"))
    else:
        raise ValueError(f"Unknown db_type: {db_type}")


@pytest.fixture(scope="session")
def db_connection(db_type):
    """Open a session-scoped DB connection; skip the entire session if unavailable."""
    if not _CODEBASE_AVAILABLE:
        pytest.skip(f"Codebase not importable: {_IMPORT_ERROR}")
    try:
        conn = _connect_db(db_type)
    except Exception as e:
        pytest.skip(f"DB connection unavailable ({db_type}): {e}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def olist_schema(db_connection, db_type):
    """Load schema and join paths from the live DB; skip on failure.

    Returns:
        Tuple of ``(schema_text, schema_map, schema_types)`` as returned
        by ``get_schema``.
    """
    if not _CODEBASE_AVAILABLE:
        pytest.skip(f"Codebase not importable: {_IMPORT_ERROR}")

    try:
        schema_text, schema_map, schema_types, join_paths = get_schema(db_connection)  # type: ignore
    except Exception as e:
        pytest.skip(f"Schema load failed: {e}")

    try:
        set_join_paths(join_paths)  # type: ignore
    except Exception:
        pass

    return schema_text, schema_map, schema_types


@pytest.fixture(scope="session")
def model_config():
    """Return LLM provider config from environment variables.

    Supports ``MODEL_PROVIDER=openai`` (with ``OPENAI_MODEL`` and
    ``OPENAI_API_KEY``) and the default ``ollama`` provider.
    """
    provider = os.getenv("MODEL_PROVIDER", "ollama").lower()
    if provider == "openai":
        return {
            "provider": "openai",
            "model":    os.getenv("OPENAI_MODEL", "gpt-4o"),
            "api_key":  os.getenv("OPENAI_API_KEY", ""),
        }
    return {
        "provider": "ollama",
        "model":    os.getenv("OLLAMA_MODEL", "mistral"),
    }


# ── Helper ────────────────────────────────────────────────────────────────────

def _run_query(conn, sql_text: str, params: list, db_type: str):
    """Execute a parameterised query and return headers + rows.

    Rewrites ``%s`` placeholders to ``?`` for SQLite, which uses a
    different placeholder syntax from psycopg2 and mysql-connector.

    Args:
        conn: An open database connection.
        sql_text: The SQL string with ``%s`` placeholders.
        params: Positional parameter list corresponding to the placeholders.
        db_type: Dialect; used to rewrite placeholders for SQLite.

    Returns:
        Tuple of ``(headers, rows)`` where ``headers`` is a list of column
        name strings and ``rows`` is a list of row tuples.
    """
    cursor = conn.cursor()
    if db_type == "sqlite" and "%s" in sql_text:
        sql_text = sql_text.replace("%s", "?")
    cursor.execute(sql_text, params)
    rows    = cursor.fetchall()
    headers = [d[0] for d in cursor.description] if cursor.description else []
    return headers, rows


# ── Parametrized integration tests ────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.parametrize("q", EVAL_QUESTIONS, ids=[q["id"] for q in EVAL_QUESTIONS])
def test_olist_question(q, db_connection, olist_schema, model_config, db_type):
    """Full pipeline integration test for a single evaluation question.

    Steps:
        1. Parse intent via LLM.
        2. Validate the parsed intent.
        3. Build SQL from the validated intent.
        4. Assert that every expected SQL pattern is present.
        5. Execute the SQL against the live database.

    Clarification requests from the model are surfaced as failures rather
    than silent skips, so they appear in the test report.

    Args:
        q: One entry from ``EVAL_QUESTIONS``.
        db_connection: Session-scoped live DB connection.
        olist_schema: ``(schema_text, schema_map, schema_types)`` tuple.
        model_config: LLM provider config dict.
        db_type: Dialect string (postgresql / mysql / sqlite).
    """
    if not _CODEBASE_AVAILABLE:
        pytest.skip(f"Codebase not importable: {_IMPORT_ERROR}")

    schema_text, schema_map, schema_types = olist_schema

    # Step 1: Parse intent
    try:
        intent = parse_intent(q["question"], schema_text, [], model_config)  # type: ignore
    except Exception as e:
        pytest.fail(f"[{q['id']}] Intent parse failed: {e}")

    if intent.get("clarification_needed"):
        pytest.fail(
            f"[{q['id']}] Model requested clarification instead of generating intent: "
            f"{intent['clarification_needed']}"
        )

    # Step 2: Validate
    clean = _strip_meta(intent)  # type: ignore
    is_valid, errors = validate_intent(clean, schema_map, schema_types)  # type: ignore
    assert is_valid, f"[{q['id']}] Validation failed: {'; '.join(errors)}"

    # Step 3: Build SQL
    try:
        sql_text, params = build_sql(clean, db_type)  # type: ignore
    except Exception as e:
        pytest.fail(f"[{q['id']}] build_sql raised: {e}")

    # Step 4: Pattern check
    sql_upper = sql_text.upper()
    missing = [p for p in q["must_contain"] if p.upper() not in sql_upper]
    assert not missing, (
        f"[{q['id']}] SQL missing expected patterns: {missing}\nSQL:\n{sql_text}"
    )

    # Step 5: Execute
    try:
        _, rows = _run_query(db_connection, sql_text, params, db_type)
    except Exception as e:
        try:
            db_connection.rollback()
        except Exception:
            pass
        pytest.fail(f"[{q['id']}] Execution failed: {e}\nSQL:\n{sql_text}")

    assert rows is not None, f"[{q['id']}] Query returned None rows (cursor error)"
