#!/usr/bin/env python3
# =============================================================================
# validate_olist.py  —  Dataloom v3.0
#
# Runs the 10 Olist evaluation questions against your live database.
# Captures intent, SQL, confidence, row count, and any errors.
# Outputs a clean report table + saves full JSON log to validate_olist_log.json
#
# Usage:
#   python validate_olist.py
#   python validate_olist.py --db postgresql   (default)
#   python validate_olist.py --db mysql
#   python validate_olist.py --verbose          (show full SQL for each question)
# =============================================================================

import os, sys, json, time, argparse
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Bootstrap path ────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from intent_parser  import parse_intent, _strip_meta
from validator      import validate_intent, set_join_paths
from sql_builder    import build_sql
from schema         import get_schema

# ── ANSI colours (degrade gracefully on Windows) ──────────────────────────────
if sys.platform == "win32":
    os.system("color")  # enable ANSI on Windows terminal
GREEN  = "\033[92m"; RED   = "\033[91m"; YELLOW = "\033[93m"
CYAN   = "\033[96m"; DIM   = "\033[2m";  BOLD   = "\033[1m"
RESET  = "\033[0m"

def c(text, *codes): return "".join(codes) + str(text) + RESET

# ── The 10 evaluation questions ───────────────────────────────────────────────
EVAL_QUESTIONS = [
    {
        "id": "Q01",
        "feature": "4B-2 LAG",
        "question": "For each seller, show their monthly revenue and the previous month's revenue side by side.",
        "must_contain": ["LAG(", "OVER"],
    },
    {
        "id": "Q02",
        "feature": "4B-3 SUM OVER",
        "question": "Show a running total of order revenue by month across all of 2017.",
        "must_contain": ["SUM(", "ROWS BETWEEN UNBOUNDED PRECEDING"],
    },
    {
        "id": "Q03",
        "feature": "4B-3 AVG OVER",
        "question": "Calculate the 3-month rolling average order value per product category.",
        "must_contain": ["AVG(", "ROWS BETWEEN 2 PRECEDING"],
    },
    {
        "id": "Q04",
        "feature": "4B-4 scalar_subquery",
        "question": "What percentage of total 2017 revenue did each product category contribute?",
        "must_contain": ["NULLIF(", "100"],
    },
    {
        "id": "Q05",
        "feature": "4B-5 EXCEPT",
        "question": "Which customers placed orders in São Paulo but have never placed an order in Rio de Janeiro?",
        "must_contain": ["EXCEPT"],
    },
    {
        "id": "Q06",
        "feature": "4B-1 RANK",
        "question": "Rank sellers within each product category by total revenue, showing top 3 per category.",
        "must_contain": ["RANK()", "OVER", "PARTITION BY"],
    },
    {
        "id": "Q07",
        "feature": "4C-1 CTE",
        "question": "Show the total orders, total spend, and last order date for every customer who spent more than 500.",
        "must_contain": ["WITH ", " AS ("],
    },
    {
        "id": "Q08",
        "feature": "4C-2 correlated",
        "question": "Find products whose average review score is above the average review score for their category.",
        "must_contain": ["SELECT AVG(", "WHERE"],
    },
    {
        "id": "Q09",
        "feature": "4C-3 X5",
        "question": "Which customers placed orders in every month of 2017?",
        "must_contain": ["COUNT(DISTINCT", "EXTRACT"],
    },
    {
        "id": "Q10",
        "feature": "4C-3 W10",
        "question": "How many orders were delivered within 7 days of being placed? Show count per seller state.",
        "must_contain": ["INTERVAL"],
    },
]


def connect_db(db_type: str):
    """Connect to the database using .env config."""
    if db_type == "postgresql":
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
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


def run_query(conn, sql: str, params: list, db_type: str):
    cursor = conn.cursor()
    if db_type == "postgresql":
        cursor.execute(sql, params)
    else:
        # MySQL / SQLite use %s or ? placeholders
        placeholder = "?" if db_type == "sqlite" else "%s"
        if "%s" in sql and placeholder == "?":
            sql = sql.replace("%s", "?")
        cursor.execute(sql, params)
    rows = cursor.fetchall()
    headers = [d[0] for d in cursor.description] if cursor.description else []
    return headers, rows


def check_sql_patterns(sql: str, must_contain: list) -> tuple[bool, list]:
    """Return (all_present, missing_list)."""
    sql_upper = sql.upper()
    missing = [p for p in must_contain if p.upper() not in sql_upper]
    return len(missing) == 0, missing


def get_model_config() -> dict:
    provider = os.getenv("MODEL_PROVIDER", "ollama").lower()
    if provider == "openai":
        return {
            "provider": "openai",
            "model": os.getenv("OPENAI_MODEL", "gpt-4o"),
            "api_key": os.getenv("OPENAI_API_KEY", ""),
        }
    return {
        "provider": "ollama",
        "model": os.getenv("OLLAMA_MODEL", "mistral"),
    }


CONFIDENCE_COLOUR = {
    "high":   GREEN,
    "medium": YELLOW,
    "low":    RED,
}

STATUS_SYMBOL = {
    "PASS":    c("PASS", GREEN, BOLD),
    "FAIL_SQL":  c("FAIL-SQL",  RED, BOLD),
    "FAIL_VAL":  c("FAIL-VAL",  RED, BOLD),
    "FAIL_PAT":  c("FAIL-PAT",  YELLOW, BOLD),
    "FAIL_EXEC": c("FAIL-EXEC", RED, BOLD),
}


def run_evaluation(db_type: str = "postgresql", verbose: bool = False, debug: bool = False):
    print(f"\n{c('DATALOOM  —  Olist Evaluation Suite', BOLD, CYAN)}")
    print(c(f"  10 questions · feature coverage · {db_type} · {datetime.now().strftime('%Y-%m-%d %H:%M')}", DIM))
    print(c("  " + "─" * 62, DIM))

    # Connect + load schema
    try:
        conn = connect_db(db_type)
        print(c("  ✓ Database connected", GREEN))
    except Exception as e:
        print(c(f"  ✗ DB connection failed: {e}", RED))
        sys.exit(1)

    try:
        schema_text, schema_map, schema_types = get_schema(conn)
        # Wire join paths from FK graph
        from validator import set_join_paths as _sjp
        try:
            from schema import get_join_paths
            _sjp(get_join_paths(conn))
        except Exception:
            pass  # join paths may already be set from schema.py init
        print(c(f"  ✓ Schema loaded — {len(schema_map)} tables", GREEN))
        if debug:
            print(c("\n  -- schema_text sent to model (first 2000 chars) --", CYAN))
            print(schema_text[:2000])
            print(c("  -- end schema_text --\n", CYAN))
    except Exception as e:
        print(c(f"  ✗ Schema load failed: {e}", RED))
        conn.close()
        sys.exit(1)

    model_config = get_model_config()
    print(c(f"  ✓ Model: {model_config['model']} via {model_config['provider']}", GREEN))
    print(c("  " + "─" * 62 + "\n", DIM))

    results = []
    passes  = 0

    for q in EVAL_QUESTIONS:
        qid      = q["id"]
        feature  = q["feature"]
        question = q["question"]
        t_start  = time.time()

        print(f"  {c(qid, BOLD, CYAN)}  {c(feature, DIM)}  {question[:65]}{'…' if len(question)>65 else ''}")

        result = {
            "id":         qid,
            "feature":    feature,
            "question":   question,
            "status":     "FAIL_VAL",
            "confidence": "—",
            "sql":        None,
            "rows":       None,
            "error":      None,
            "elapsed_s":  None,
            "missing_patterns": [],
        }

        # ── Step 1: Parse intent ──────────────────────────────────────
        try:
            intent = parse_intent(question, schema_text, [], model_config)
        except Exception as e:
            result["error"] = f"Intent parse failed: {e}"
            print(f"       {c('✗ intent parse failed:', RED)} {e}\n")
            results.append(result)
            continue

        result["intent"] = intent
        if debug:
            print(c(f"       raw intent:", CYAN))
            import pprint
            pprint.pprint(intent, indent=8, width=100)

        confidence = (intent.get("confidence") or "—").lower()
        result["confidence"] = confidence
        conf_col = CONFIDENCE_COLOUR.get(confidence, DIM)

        # Surface clarification requests without failing hard
        if intent.get("clarification_needed"):
            result["status"] = "FAIL_VAL"
            result["error"]  = f"clarification_needed: {intent['clarification_needed']}"
            print(f"       {c('⚠ clarification requested:', YELLOW)} {intent['clarification_needed']}")
            print(f"       confidence={c(confidence, conf_col)}\n")
            results.append(result)
            continue

        # ── Step 2: Validate ──────────────────────────────────────────
        clean = _strip_meta(intent)
        is_valid, errors = validate_intent(clean, schema_map, schema_types)
        if not is_valid:
            result["status"] = "FAIL_VAL"
            result["error"]  = "; ".join(errors)
            print(f"       {c('✗ validation failed:', RED)} {errors[0]}")
            if len(errors) > 1:
                print(f"          {c(f'  (+{len(errors)-1} more)', DIM)}")
            print(f"       confidence={c(confidence, conf_col)}\n")
            results.append(result)
            continue

        # ── Step 3: Build SQL ─────────────────────────────────────────
        try:
            sql, params = build_sql(clean, db_type)
            result["sql"] = sql
        except Exception as e:
            result["status"] = "FAIL_SQL"
            result["error"]  = f"SQL build: {e}"
            print(f"       {c('✗ SQL build failed:', RED)} {e}\n")
            results.append(result)
            continue

        # ── Step 4: Pattern check ─────────────────────────────────────
        patterns_ok, missing = check_sql_patterns(sql, q["must_contain"])
        if not patterns_ok:
            result["status"]           = "FAIL_PAT"
            result["missing_patterns"] = missing
            result["error"]            = f"Missing SQL patterns: {missing}"
            print(f"       {c('⚠ pattern mismatch:', YELLOW)} expected {missing} in SQL")
            if verbose:
                for line in sql.strip().split("\n"):
                    print(f"         {c(line, DIM)}")
            print(f"       confidence={c(confidence, conf_col)}\n")
            results.append(result)
            continue

        # ── Step 5: Execute ───────────────────────────────────────────
        try:
            headers, rows = run_query(conn, sql, params, db_type)
            result["rows"] = len(rows)
            result["status"] = "PASS"
            elapsed = round(time.time() - t_start, 2)
            result["elapsed_s"] = elapsed
            passes += 1
            print(f"       {c('✓', GREEN)} {c(len(rows), BOLD)} rows · confidence={c(confidence, conf_col)} · {elapsed}s")
            if verbose:
                for line in sql.strip().split("\n"):
                    print(f"         {c(line, DIM)}")
        except Exception as e:
            result["status"] = "FAIL_EXEC"
            result["error"]  = f"Execution: {e}"
            print(f"       {c('✗ execution failed:', RED)} {e}")
            if verbose:
                for line in sql.strip().split("\n"):
                    print(f"         {c(line, DIM)}")
            try:
                conn.rollback()
            except Exception:
                pass

        print()
        results.append(result)

    # ── Summary ───────────────────────────────────────────────────────
    conn.close()
    total = len(results)
    fails = total - passes

    print(c("  " + "─" * 62, DIM))
    print(f"\n  {c('RESULTS', BOLD)}  {c(passes, GREEN, BOLD)}/{total} passed · {c(fails, RED if fails else GREEN, BOLD)} failed\n")

    # Status breakdown table
    header = f"  {'ID':<5} {'Feature':<18} {'Status':<12} {'Conf':<8} {'Rows':<6} {'Error / Notes'}"
    print(c(header, DIM))
    print(c("  " + "─" * 74, DIM))
    for r in results:
        conf_col = CONFIDENCE_COLOUR.get(r["confidence"], DIM)
        status_str = r["status"]
        if r["status"] == "PASS":
            status_disp = c(f"{'PASS':<10}", GREEN, BOLD)
        elif r["status"] == "FAIL_PAT":
            status_disp = c(f"{'FAIL-PAT':<10}", YELLOW, BOLD)
        else:
            status_disp = c(f"{r['status']:<10}", RED, BOLD)
        rows_str  = str(r["rows"]) if r["rows"] is not None else "—"
        error_str = ""
        if r["status"] == "FAIL_PAT":
            error_str = f"missing: {r['missing_patterns']}"
        elif r["error"]:
            error_str = r["error"][:55]
        conf_padded = "{:<8}".format(r["confidence"])   # avoid nested f-string
        conf_disp   = c(conf_padded, conf_col)
        print(f"  {r['id']:<5} {r['feature']:<18} {status_disp} {conf_disp} {rows_str:<6} {c(error_str, DIM)}")

    # Confidence summary
    conf_counts = {"high": 0, "medium": 0, "low": 0}
    for r in results:
        c_val = r["confidence"].lower()
        if c_val in conf_counts:
            conf_counts[c_val] += 1
    high_str   = c("high:{}".format(conf_counts["high"]),   GREEN)
    medium_str = c("medium:{}".format(conf_counts["medium"]), YELLOW)
    low_str    = c("low:{}".format(conf_counts["low"]),    RED)
    print(f"\n  Confidence  {high_str}  {medium_str}  {low_str}\n")

    # Save JSON log
    log_path = "validate_olist_log.json"
    with open(log_path, "w") as f:
        json.dump({
            "run_at":    datetime.now().isoformat(),
            "db_type":   db_type,
            "model":     model_config["model"],
            "passed":    passes,
            "total":     total,
            "results":   results,
        }, f, indent=2)
    print(c(f"  Full log saved → {log_path}", DIM))
    print()

    return results


def main():
    parser = argparse.ArgumentParser(description="Dataloom Olist Evaluation Suite")
    parser.add_argument("--db",      default="postgresql", choices=["postgresql","mysql","sqlite"])
    parser.add_argument("--verbose", action="store_true",  help="Print SQL for each question")
    parser.add_argument("--debug",   action="store_true",  help="Print schema_text and intent JSON")
    args = parser.parse_args()
    run_evaluation(db_type=args.db, verbose=args.verbose, debug=args.debug)


if __name__ == "__main__":
    main()
